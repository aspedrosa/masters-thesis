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
  Status string  `gorm:"not null"`
}

type PipelineSelection struct {
  gorm.Model
  PipelineId uint
  Column string `gorm:"not null;uniqueIndex:unique_column"`
  Order int `gorm:"default:0"`
}

type Subscription struct {
  gorm.Model
  Pipeline Pipeline
  Name string  `gorm:"not null;unique"`
  Requests []Request
  Status string  `gorm:"not null"`
}

type Request struct {
  gorm.Model
  SubscriptionId uint
  Request_arguments_template string  `gorm:"not null"`
  Success_condition_template string  `gorm:"not null"`
  Order int  `gorm:"default:0"`
}

func main() {
  DB, _ := gorm.Open("sqlite3", "demo.db")
  DB.AutoMigrate(&Pipeline{}, &PipelineSelection{}, &Subscription{}, &Request{})

  // Initialize
  Admin := admin.New(&admin.AdminConfig{DB: DB})

  // Allow to use Admin to manage Pipeline and Subscriptions
  pipeline := Admin.AddResource(&Pipeline{})
  pipeline.NewAttrs("-Status")

  sub := Admin.AddResource(&Subscription{})
  sub.NewAttrs("-Status")
  sub.Meta(&admin.Meta{
    Name: "Pipeline",
    Label: "Pipeline",
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

  request := sub.Meta(&admin.Meta{Name: "Requests"}).Resource
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
