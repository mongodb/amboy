package remote

import (
	"testing"
	"time"

	"gopkg.in/mgo.v2"

	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tychoish/grip"
	"golang.org/x/net/context"
)

type MongoDBDriverSuite struct {
	driver      *MongoDBQueueDriver
	require     *require.Assertions
	collections []string
	uri         string
	dbName      string
	suite.Suite
}

func TestMongoDBDriverSuite(t *testing.T) {
	suite.Run(t, new(MongoDBDriverSuite))
}

func (s *MongoDBDriverSuite) SetupSuite() {
	s.uri = "mongodb://localhost:27017"
	s.dbName = "amboy"
	s.require = s.Require()
}

func (s *MongoDBDriverSuite) SetupTest() {
	name := uuid.NewV4().String()
	s.driver = NewMongoDBQueueDriver(name, s.uri)
	s.driver.dbName = s.dbName
	s.collections = append(s.collections, name+".jobs", name+".locks")
}

func (s *MongoDBDriverSuite) TearDownSuite() {
	session, err := mgo.Dial(s.uri)
	if !s.NoError(err) {
		defer session.Close()
	}

	db := session.DB(s.dbName)
	for _, coll := range s.collections {
		grip.CatchWarning(db.C(coll).DropCollection())
	}
}

func (s *MongoDBDriverSuite) TearDownTest() {
	s.Equal(s.dbName, s.driver.dbName)
}

func (s *MongoDBDriverSuite) TestOpenCloseAffectState() {
	ctx := context.Background()

	s.Nil(s.driver.canceler)
	s.NoError(s.driver.Open(ctx))
	s.NotNil(s.driver.canceler)

	s.driver.Close()
	s.NotNil(s.driver.canceler)
	// sleep to give it a chance to switch to close the connection
	time.Sleep(10 * time.Millisecond)
}
