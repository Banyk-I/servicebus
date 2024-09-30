package servicebus

type Message interface {
	GetRoutingKey() string
}
