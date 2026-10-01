package join

import (
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruittop"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type clientState struct {
	items        []fruititem.FruitItem
	topsReceived int
}

type Join struct {
	aggregationAmount int
	topSize           int

	inputQueue  middleware.Middleware
	outputQueue middleware.Middleware

	clients map[string]*clientState
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}
	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		aggregationAmount: config.AggregationAmount, topSize: config.TopSize,
		inputQueue: inputQueue, outputQueue: outputQueue,
		clients: map[string]*clientState{},
	}, nil
}

func (join *Join) Run() error {
	return join.inputQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		join.handleMessage(msg, ack, nack)
	})
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	message, err := inner.Deserialize(&msg)
	if err != nil {
		slog.Error("Descartando mensaje inválido", "err", err)
		ack()
		return
	}
	if message.Kind != inner.KindTop {
		slog.Warn("Tipo de mensaje inesperado en Join", "kind", message.Kind)
		ack()
		return
	}
	if err := join.handleTop(message); err != nil {
		slog.Error("Al procesar un top parcial", "client", message.ClientID, "err", err)
		nack()
		return
	}
	ack()
}

func (join *Join) handleTop(message inner.Message) error {
	state, exists := join.clients[message.ClientID]
	if !exists {
		state = &clientState{}
		join.clients[message.ClientID] = state
	}

	state.items = append(state.items, message.Items...)
	state.topsReceived++

	if state.topsReceived < join.aggregationAmount {
		return nil
	}

	finalTop := fruittop.Top(state.items, join.topSize)

	serialized, err := inner.Serialize(inner.Message{ClientID: message.ClientID, Kind: inner.KindResult, Items: finalTop})
	if err != nil {
		return err
	}
	if err := join.outputQueue.Send(*serialized); err != nil {
		return err
	}

	delete(join.clients, message.ClientID)
	return nil
}
