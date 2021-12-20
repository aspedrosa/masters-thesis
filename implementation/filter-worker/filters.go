package main

import (
	"context"
	"sync"
)

type UploadToFilter struct {
	Database_identifier    string
	Rows                   uint32
	Belongs_to_communities bool
}

type FilterData struct {
	cancel_func               context.CancelFunc
	Communities               []int
	Upload_notifications_chan chan UploadToFilter
}

var Mappings_mtx = sync.Mutex{}
var Mappings = make(map[int]FilterData)
var Filters_wait_group = sync.WaitGroup{}
var Waiting_for_filters = false

func Launch_filter(filter Filter, create_streams bool) {
	// create context to stop filter worker
	ctx := context.Background()
	ctx, cancel_filter_main := context.WithCancel(ctx)

	upload_notification_chan := make(chan UploadToFilter)

	Mappings_mtx.Lock()
	Mappings[filter.Id] = FilterData{
		cancel_func:               cancel_filter_main,
		Communities:               filter.Communities,
		Upload_notifications_chan: upload_notification_chan,
	}
	Mappings_mtx.Unlock()

	if create_streams {
		Init_streams(filter)
	}

	// launch filter worker
	go filter_main(
		filter.Id,
		filter.Filter == "",
		upload_notification_chan,
		ctx,
	)
}

func Edit_filter(new_filter Filter) {
	Stop_filter(new_filter.Id)

	Launch_filter(new_filter, true)
}

func Stop_filter(filter_id int) {
	Mappings[filter_id].cancel_func()
	Mappings_mtx.Lock()
	delete(Mappings, filter_id)
	Mappings_mtx.Unlock()

	Stop_streams(filter_id)
}