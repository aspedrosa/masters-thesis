package main

import (
	"fmt"
	"github.com/jinzhu/gorm"
	_ "github.com/lib/pq"
	"github.com/qor/admin"
	"github.com/qor/roles"
	"log"
	"net/http"
	"os"
)

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

func main() {
	db_host := os.Getenv("DB_HOST")
	db_port := os.Getenv("DB_PORT")
	db_user := os.Getenv("DB_USER")
	db_password := os.Getenv("DB_PASSWORD")
	db_name := os.Getenv("DB_NAME")

	DB, err := gorm.Open(
		"postgres",
		fmt.Sprintf(
			"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
			db_host, db_port, db_user, db_password, db_name),
	)
	if err != nil {
		log.Fatal(err)
	}
	DB.AutoMigrate(&Pipeline{}, &PipelineSelection{}, &Subscription{}, &Request{}, &SubscriptionLog{}, &RequestLog{})

	// Initialize
	Admin := admin.New(&admin.AdminConfig{DB: DB})

	// Allow to use Admin to manage Pipeline and Subscriptions
	pipeline := Admin.AddResource(&Pipeline{})
	pipeline.NewAttrs("-Status")
	pipeline.EditAttrs("-Status")

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
						// TODO send message to topic to start pipeline workers
						pipeline.Status = "ACTIVE"
						tx.Save(&pipeline)
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

						// TODO send message to topic to stop pipeline workers
						pipeline.Status = "STOPPED"
						tx.Save(&pipeline)
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

	request := sub.Meta(&admin.Meta{Name: "Requests"}).Resource
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

	Admin.AddResource(&SubscriptionLog{}, &admin.Config{
		Permission: roles.Deny(roles.Update, roles.Anyone), //.Deny(roles.Create, roles.Anyone),
	})

	// initialize an HTTP request multiplexer
	mux := http.NewServeMux()

	// Mount admin interface to mux
	Admin.MountTo("/admin", mux)

	fmt.Println("Listening on: 9000")
	http.ListenAndServe(":9000", mux)
}
