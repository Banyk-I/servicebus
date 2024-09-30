package ServiceBus

import "encoding/json"

type JSONSerializer struct{}

func (s *JSONSerializer) Marshal(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}

func (s *JSONSerializer) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
