package main

import (
	"context"
	"sync"
)

type Upload struct {
	database_identifier string `json:"DATABASE_IDENTIFIER"`
	rows                uint32 `json:"ROWS"`
}

type UploadToFilter struct {
	database_identifier    string
	rows                   uint32
	belongs_to_communities bool
}

type FilterData struct {
	cancel_func               context.CancelFunc
	communities               []int
	upload_notifications_chan chan UploadToFilter
}

var mappings_mtx = sync.Mutex{}
var mappings = make(map[int]FilterData)
var filters_wait_group = sync.WaitGroup{}

func launch_filter(filter_worker_id int, filter Filter, create_streams bool) {
	// create context to stop filter worker
	ctx := context.Background()
	ctx, cancel_filter_main := context.WithCancel(ctx)

	upload_notification_chan := make(chan UploadToFilter)

	mappings_mtx.Lock()
	mappings[filter.id] = FilterData{
		cancel_func:               cancel_filter_main,
		communities:               filter.communities,
		upload_notifications_chan: upload_notification_chan,
	}
	mappings_mtx.Unlock()

	if create_streams {
		init_streams(filter_worker_id, filter)
	}

	// launch filter worker
	go filter_main(
		&filters_wait_group,
		upload_notification_chan,
		filter_worker_id,
		filter.id,
		ctx,
	)
}

func edit_filter(filter_worker_id int, new_filter_id Filter) {
	stop_filter(filter_worker_id, new_filter_id.id)

	launch_filter(filter_worker_id, new_filter_id, true)
}

func stop_filter(filter_worker_id int, filter_id int) {
	mappings[filter_id].cancel_func()
	mappings_mtx.Lock()
	delete(mappings, filter_id)
	mappings_mtx.Unlock()

	stop_streams(filter_worker_id, filter_id)
}

// function executed by the upload_watcher
func broadcast_upload_notification(upload Upload, community_id int) {
	mappings_mtx.Lock()
	filters_wait_group.Add(len(mappings))

	for _, filter := range mappings {
		belongs_to_communities := false
		for _, filter_community_id := range filter.communities {
			if filter_community_id == community_id {
				belongs_to_communities = true
				break
			}
		}

		filter.upload_notifications_chan <- UploadToFilter{
			database_identifier:    upload.database_identifier,
			rows:                   upload.rows,
			belongs_to_communities: belongs_to_communities,
		}
	}

	mappings_mtx.Unlock()

	filters_wait_group.Wait()
}
