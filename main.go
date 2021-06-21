package main

import (
  "fmt"
  "github.com/jinzhu/gorm"
  _ "github.com/mattn/go-sqlite3"
  "github.com/qor/admin"
  "net/http"
)

type Pipeline struct {
  gorm.Model
  Name string  `gorm:"not null;unique"`
  Selections []PipelineSelection
  Filter string
  Status string  `gorm:"not null;default:'STOPPED'"`
}

type PipelineSelection struct {
  gorm.Model
  PipelineID uint `gorm:"uniqueIndex:unique_column"`
  Column string `gorm:"not null;uniqueIndex:unique_column"`
  Order int `gorm:"default:0"`
}

type Subscription struct {
  gorm.Model
  PipelineID uint
  Name string  `gorm:"not null;unique"`
  Requests []Request
  Status string  `gorm:"not null;default:'STOPPED'"`
}

type Request struct {
  gorm.Model
  SubscriptionID uint
  Request_arguments_template string  `gorm:"not null"`
  Success_condition_template string  `gorm:"not null"`
  Order int  `gorm:"default:0"`
}

func main() {
  //os.Remove("demo.db")

  DB, _ := gorm.Open("sqlite3", "demo.db")
  DB.AutoMigrate(&Pipeline{}, &PipelineSelection{}, &Subscription{}, &Request{})

  // Initialize
  Admin := admin.New(&admin.AdminConfig{DB: DB})

  // Allow to use Admin to manage Pipeline and Subscriptions
  pipeline := Admin.AddResource(&Pipeline{})
  pipeline.NewAttrs("-Status")
  pipeline.EditAttrs("-Status")

  selection := pipeline.Meta(&admin.Meta{Name: "Selections"}).Resource
  selection.Meta(&admin.Meta{
    Name: "Column",
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

  sub.NewAttrs("-Status")
  sub.EditAttrs("-Status")

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

  // initialize an HTTP request multiplexer
  mux := http.NewServeMux()

  // Mount admin interface to mux
  Admin.MountTo("/admin", mux)

  fmt.Println("Listening on: 9000")
  http.ListenAndServe(":9000", mux)
}
