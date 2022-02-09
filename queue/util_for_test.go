package queue

import (
	"github.com/evergreen-ci/utility"
	"github.com/mongodb/amboy"
)

// defaultMongoDBQueueGroupTestOptions returns default MongoDB queue group
// options for testing purposes only.
func defaultMongoDBQueueGroupTestOptions() MongoDBQueueGroupOptions {
	queueOpts := defaultMongoDBQueueTestOptions()
	return MongoDBQueueGroupOptions{
		DefaultQueue: queueOpts,
	}
}

// defaultMongoDBQueueTestOptions returns default MongoDB queue options for
// testing purposes only.
func defaultMongoDBQueueTestOptions() MongoDBQueueOptions {
	dbOpts := defaultMongoDBTestOptions()
	return MongoDBQueueOptions{
		DB:         &dbOpts,
		NumWorkers: utility.ToIntPtr(1),
	}
}

// defaultMongoDBTestOptions returns default MongoDB options for testing
// purposes only.
func defaultMongoDBTestOptions() MongoDBOptions {
	opts := DefaultMongoDBOptions()
	opts.DB = "amboy_test"
	opts.Collection = newDriverID()
	return opts
}

// bsonJobTimeInfo converts all amboy.JobTimeInfo time fields into BSON time.
func bsonJobTimeInfo(i amboy.JobTimeInfo) amboy.JobTimeInfo {
	i.Created = utility.BSONTime(i.Created)
	i.Start = utility.BSONTime(i.Start)
	i.End = utility.BSONTime(i.End)
	i.WaitUntil = utility.BSONTime(i.WaitUntil)
	i.DispatchBy = utility.BSONTime(i.DispatchBy)
	return i
}

// bsonJobStatusInfo converts all amboy.JobStatusInfo time fields into BSON
// time.
func bsonJobStatusInfo(i amboy.JobStatusInfo) amboy.JobStatusInfo {
	i.ModificationTime = utility.BSONTime(i.ModificationTime)
	return i
}

// bsonJobRetryInfo converts all amboy.JobRetryInfo time fields into BSON time.
func bsonJobRetryInfo(i amboy.JobRetryInfo) amboy.JobRetryInfo {
	i.Start = utility.BSONTime(i.Start)
	i.End = utility.BSONTime(i.End)
	return i
}
