package queue

import (
	"github.com/evergreen-ci/utility"
	"github.com/mongodb/amboy"
)

func defaultMongoDBTestOptions() MongoDBOptions {
	opts := DefaultMongoDBOptions()
	opts.DB = "amboy_test"
	return opts
}

func bsonJobTimeInfo(i amboy.JobTimeInfo) amboy.JobTimeInfo {
	i.Created = utility.BSONTime(i.Created)
	i.Start = utility.BSONTime(i.Start)
	i.End = utility.BSONTime(i.End)
	i.WaitUntil = utility.BSONTime(i.WaitUntil)
	i.DispatchBy = utility.BSONTime(i.DispatchBy)
	return i
}

func bsonJobStatusInfo(i amboy.JobStatusInfo) amboy.JobStatusInfo {
	i.ModificationTime = utility.BSONTime(i.ModificationTime)
	return i
}
