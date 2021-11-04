package shared_structs

type Filter struct {
	Id          int      `json:"id"`
	Filter      string   `json:"filter"`
	Communities []int    `json:"communities"`
	Selections  []string `json:"selections"`
}

type ManagementMessage struct {
	FilterId int    `json:"filter_id"`
	Action   string `json:"action"`
}
