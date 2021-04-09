package management

// JobStatusReport contains data for the numbers of jobs that exist
// for a specified type.
type JobStatusReport struct {
	Filter StatusFilter  `bson:"filter" json:"filter" yaml:"filter"`
	Stats  []JobCounters `bson:"data" json:"data" yaml:"data"`
}

// JobCounters holds data for counts of jobs by type.
type JobCounters struct {
	ID    string `bson:"_id" json:"type" yaml:"type"`
	Count int    `bson:"count" json:"count" yaml:"count"`
	Group string `bson:"group,omitempty" json:"group,omitempty" yaml:"group,omitempty"`
}

// GroupedID represents a job's ID and the group that the job belongs to, if
// it's in a queue group.
type GroupedID struct {
	ID    string `bson:"_id" bson:"_id" yaml:"_id"`
	Group string `bson:"group,omitempty" json:"group,omitempty" yaml:"group,omitempty"`
}
