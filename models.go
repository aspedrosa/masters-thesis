package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jinzhu/gorm"
	"github.com/qor/admin"
	"github.com/qor/roles"
	"github.com/segmentio/kafka-go"
	"os"
)

type Database struct {
	gorm.Model
	Hash string
}

type Pipeline struct {
	gorm.Model
	Name       string `gorm:"not null;unique"`
	Selections []PipelineSelection
	Filter     string
	Status     string `gorm:"not null;default:'STOPPED'"`
}

type PipelineSelection struct {
	gorm.Model
	PipelineID      uint   `gorm:"uniqueIndex:unique_column"`
	SelectionColumn string `gorm:"not null;uniqueIndex:unique_column"`
	SelectionOrder  int    `gorm:"default:0"`
}

type Subscription struct {
	gorm.Model
	PipelineID uint
	Name       string `gorm:"not null;unique"`
	Requests   []Request
	Status     string `gorm:"not null;default:'STOPPED'"`
	Logs       []SubscriptionLog
}

type SubscriptionLog struct {
	gorm.Model
	SubscriptionID uint
	SuccessCount   uint
	RequestsLogs   []RequestLog
}

type Request struct {
	gorm.Model
	SubscriptionID             uint
	Request_arguments_template string `gorm:"not null"`
	Success_condition_template string
	RequestOrder               int `gorm:"default:0"`
	Logs                       []RequestLog
}

type RequestLog struct {
	gorm.Model
	RequestID         uint
	SubscriptionLogID uint
	Success           bool
	Exception         string
}

func configure_admin_models(DB *gorm.DB) *admin.Admin {
	DB.AutoMigrate(
		&Database{},
		&Pipeline{}, &PipelineSelection{},
		&Subscription{}, &SubscriptionLog{},
		&Request{}, &RequestLog{},
	)

	// Initialize
	Admin := admin.New(&admin.AdminConfig{DB: DB})

	pipeline := configure_pipeline(Admin)
	configure_pipeline_selection(pipeline)

	subscription := configure_subscription(Admin)
	configure_subscription_request(subscription)

	// dont allow to create or update logs entries
	Admin.AddResource(&SubscriptionLog{}, &admin.Config{
		Permission: roles.Deny(roles.Update, roles.Anyone).Deny(roles.Create, roles.Anyone),
	})

	return Admin
}

func configure_pipeline(Admin *admin.Admin) *admin.Resource {
	// Allow to use Admin to manage Pipeline and Subscriptions
	pipeline := Admin.AddResource(&Pipeline{})
	pipeline.NewAttrs("-Status")
	pipeline.EditAttrs("-Status")

	return pipeline
}

func configure_pipeline_selection(pipeline *admin.Resource) {
	selection := pipeline.Meta(&admin.Meta{Name: "Selections"}).Resource
	selection.Meta(&admin.Meta{
		Name: "SelectionColumn",
		Config: &admin.SelectOneConfig{
			Collection: []string{
				"analysis_id",
				"stratum_1",
				"stratum_2",
				"stratum_3",
				"stratum_4",
				"stratum_5",
				"count_value",
				"min_value",
				"max_value",
				"avg_value",
				"stdev_value",
				"mean_value",
				"p10_value",
				"p25_value",
				"p75_value",
				"p90_value",
			},
		},
	})
}

func configure_subscription(Admin *admin.Admin) *admin.Resource {
	sub := Admin.AddResource(&Subscription{})

	sub.NewAttrs("-Status", "-Logs")
	sub.EditAttrs("-Status", "-Logs")
	sub.ShowAttrs("-Logs")
	sub.IndexAttrs("-Logs")

	// select between existing pipelines
	sub.Meta(&admin.Meta{
		Name: "PipelineID",
		Config: &admin.SelectOneConfig{
			Collection: func(_ interface{}, context *admin.Context) (options [][]string) {
				var pipelines []Pipeline
				context.GetDB().Find(&pipelines)

				for _, n := range pipelines {
					idStr := fmt.Sprintf("%d", n.ID)
					var option = []string{idStr, n.Name}
					options = append(options, option)
				}

				return options
			},
		},
	})

	w := &kafka.Writer{
		Addr:  kafka.TCP(os.Getenv("BOOTSTRAP_SERVERS")),
		Topic: "PIPELINES_MANAGEMENT",
	}

	// start subscription action
	sub.Action(&admin.Action{
		Name: "Start",
		Handler: func(actionArgument *admin.ActionArgument) error {
			db := actionArgument.Context.GetDB()
			for _, record := range actionArgument.FindSelectedRecords() {
				subscription := record.(*Subscription)

				if subscription.Status == "ACTIVE" {
					continue
				}

				err := db.Transaction(func(tx *gorm.DB) error {
					subscription.Status = "ACTIVE"
					tx.Save(&subscription)

					var pipeline Pipeline
					tx.Take(&pipeline, record.(*Subscription).PipelineID)

					if pipeline.Status == "STOPPED" {
						pipeline.Status = "ACTIVE"
						tx.Save(&pipeline)

						kafka_message, _ := json.Marshal(map[string]interface{}{
							"pipeline_id": pipeline.ID,
							"action":      "ACTIVE",
						})
						w.WriteMessages(
							context.Background(),
							kafka.Message{
								Value: kafka_message,
							},
						)
					}

					return nil
				})

				if err != nil {
					return err
				}
			}
			return nil
		},
		Visible: func(record interface{}, context *admin.Context) bool {
			if record == nil {
				return true
			}
			return record.(*Subscription).Status == "STOPPED"
		},
		Modes: []string{"edit", "show", "menu_item", "batch"},
	})
	sub.Action(&admin.Action{
		Name: "Stop",
		Handler: func(actionArgument *admin.ActionArgument) error {
			db := actionArgument.Context.GetDB()
			for _, record := range actionArgument.FindSelectedRecords() {
				subscription := record.(*Subscription)

				if subscription.Status == "STOPPED" {
					continue
				}

				err := db.Transaction(func(tx *gorm.DB) error {
					subscription.Status = "STOPPED"
					tx.Save(&subscription)

					var count uint
					tx2 := tx.Model(&Subscription{}).Where("pipeline_id = ? AND status = ?", subscription.PipelineID, "ACTIVE").Count(&count)
					fmt.Println(tx2.Error)

					if count == 0 {
						var pipeline Pipeline
						tx.Take(&pipeline, subscription.PipelineID)

						pipeline.Status = "STOPPED"
						tx.Save(&pipeline)

						kafka_message, _ := json.Marshal(map[string]interface{}{
							"pipeline_id": pipeline.ID,
							"action":      "STOPPED",
						})
						w.WriteMessages(
							context.Background(),
							kafka.Message{
								Value: kafka_message,
							},
						)
					}

					return nil
				})

				if err != nil {
					return err
				}
			}

			return nil
		},
		Visible: func(record interface{}, context *admin.Context) bool {
			if record == nil {
				return true
			}
			return record.(*Subscription).Status == "ACTIVE"
		},
		Modes: []string{"edit", "show", "menu_item", "batch"},
	})

	return sub
}

func configure_subscription_request(subscription *admin.Resource) {
	request := subscription.Meta(&admin.Meta{Name: "Requests"}).Resource
	// change some inputs to textarea
	request.Meta(&admin.Meta{
		Name: "Request_arguments_template",
		Type: "text",
	})
	request.Meta(&admin.Meta{
		Name: "Success_condition_template",
		Type: "text",
	})

	request.ShowAttrs("-Logs")
	request.NewAttrs("-Logs")
	request.EditAttrs("-Logs")
}
