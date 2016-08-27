package amboy

import (
	"encoding/json"

	"github.com/pkg/errors"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/yaml.v2"
)

type Format int

const (
	BSON Format = iota
	YAML
	JSON
)

func ConvertTo(f Format, v interface{}) ([]byte, error) {
	switch f {
	case JSON:
		return json.Marshal(v)
	case BSON:
		return bson.Marshal(v)
	case YAML:
		return yaml.Marshal(v)
	default:
		return []byte{}, errors.New("no support for specified serialization format")
	}
}

func ConvertFrom(f Format, data []byte, v interface{}) error {
	switch f {
	case JSON:
		return json.Unmarshal(data, v)
	case BSON:
		return bson.Unmarshal(data, v)
	case YAML:
		return yaml.Unmarshal(data, v)
	default:
		return errors.New("no support for specified serialization format")
	}
}
