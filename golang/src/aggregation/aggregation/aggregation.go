package aggregation

import (
	"fmt"
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruittop"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	sumAmount        int
	topSize          int
	inputQueue       middleware.Middleware
	outputQueue      middleware.Middleware
	totals           map[string]fruititem.FruitItem
	partialsReceived int
	currentClientID  string
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}
	inputQueueName := fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)

	inputQueue, err := middleware.CreateQueueMiddleware(inputQueueName, connSettings)
	if err != nil {
		return nil, err
	}
	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		sumAmount: config.SumAmount, topSize: config.TopSize,
		inputQueue: inputQueue, outputQueue: outputQueue,
		totals: map[string]fruititem.FruitItem{},
	}, nil
}

func (aggregation *Aggregation) Run() error {
	return aggregation.inputQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	message, err := inner.Deserialize(&msg)
	if err != nil {
		slog.Error("Descartando mensaje inválido", "err", err)
		ack()
		return
	}
	if message.Kind != inner.KindPartial {
		slog.Warn("Tipo de mensaje inesperado en Aggregation", "kind", message.Kind)
		ack()
		return
	}
	if err := aggregation.handlePartial(message); err != nil {
		slog.Error("Al procesar un parcial", "client", message.ClientID, "err", err)
		nack()
		return
	}
	ack()
}

func (aggregation *Aggregation) handlePartial(message inner.Message) error {
	aggregation.currentClientID = message.ClientID

	for _, item := range message.Items {
		previous, found := aggregation.totals[item.Fruit]
		if found {
			aggregation.totals[item.Fruit] = previous.Sum(item)
		} else {
			aggregation.totals[item.Fruit] = item
		}
	}
	aggregation.partialsReceived++

	if aggregation.partialsReceived < aggregation.sumAmount {
		return nil
	}

	consolidated := make([]fruititem.FruitItem, 0, len(aggregation.totals))
	for _, item := range aggregation.totals {
		consolidated = append(consolidated, item)
	}
	partialTop := fruittop.Top(consolidated, aggregation.topSize)

	serialized, err := inner.Serialize(inner.Message{
		ClientID: aggregation.currentClientID, Kind: inner.KindTop, Items: partialTop,
	})
	if err != nil {
		return err
	}
	if err := aggregation.outputQueue.Send(*serialized); err != nil {
		return err
	}

	aggregation.totals = map[string]fruititem.FruitItem{}
	aggregation.partialsReceived = 0
	return nil
}
