package ServiceBus

type Message interface {
	GetRoutingKey() string
}
