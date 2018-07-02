package reporting

import "time"

type JobStatusReport struct {
	Filter string        `bson:"filter" json:"filter" yaml:"filter"`
	Stats  []JobCounters `bson:"data" json:"data" yaml:"data"`
}

type JobCounters struct {
	ID    string `bson:"_id" json:"type" yaml:"type"`
	Count int    `bson:"count" json:"count" yaml:"count"`
}

type JobRuntimeReport struct {
	Filter string        `bson:"filter" json:"filter" yaml:"filter"`
	Period time.Duration `bson:"period" json:"period" yaml:"period"`
	Stats  []JobRuntimes `bson:"data" json:"data" yaml:"data"`
}

type JobRuntimes struct {
	ID       string        `bson:"_id" json:"type" yaml:"type"`
	Duration time.Duration `bson:"duration" json:"duration" yaml:"duration"`
}

type JobReportIDs struct {
	Type   string   `bson:"_id" json:"type" yaml:"type"`
	Filter string   `bson:"filter" json:"filter" yaml:"filter"`
	IDs    []string `bson:"jobs" json:"jobs" yaml:"jobs"`
}

type JobErrorsReport struct {
	Period         time.Duration      `bson:"period" json:"period" yaml:"period"`
	FilteredByType bool               `bson:"filtered" json:"filtered" yaml:"filtered"`
	Data           []JobErrorsForType `bson:"data" json:"data" yaml:"data"`
}

type JobErrorsForType struct {
	ID      string   `bson:"_id" json:"type" yaml:"type"`
	Count   int      `bson:"count" json:"count" yaml:"count"`
	Total   int      `bson:"total" json:"total" yaml:"total"`
	Average float64  `bson:"average" json:"average" yaml:"average"`
	Errors  []string `bson:"errors,omitempty" json:"errors,omitempty" yaml:"errors,omitempty"`
}
