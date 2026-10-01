package aggregation

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

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
type clientState struct {
	totals           map[string]fruititem.FruitItem
	partialsReceived int
}

type Aggregation struct {
	sumAmount   int
	topSize     int
	inputQueue  middleware.Middleware
	outputQueue middleware.Middleware

	clients map[string]*clientState
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
		clients: map[string]*clientState{},
	}, nil
}

func (aggregation *Aggregation) Run() error {
	defer aggregation.Close()
	go aggregation.handleSIGTERM()
	return aggregation.inputQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}
func (aggregation *Aggregation) Close() {
	allqueues := []middleware.Middleware{aggregation.inputQueue, aggregation.outputQueue}
	for _, queue := range allqueues {
		queue.Close()
	}
}
func (aggregation *Aggregation) handleSIGTERM() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	slog.Info("SIGTERM signal received")
	err := aggregation.inputQueue.StopConsuming()
	if err != nil {
		slog.Error("Lost connection to middleware", "err", err)
	}
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	message, err := inner.Deserialize(&msg)
	if err != nil {
		slog.Error("Discarding invalid message", "err", err)
		ack()
		return
	}
	if message.Kind != inner.KindPartial {
		slog.Warn("Unexpected message type in Aggregation", "kind", message.Kind)
		ack()
		return
	}
	if err := aggregation.handlePartial(message); err != nil {
		slog.Error("Failed to process partial", "client", message.ClientID, "err", err)
		nack()
		return
	}
	ack()
}

func (aggregation *Aggregation) handlePartial(message inner.Message) error {
	state, exists := aggregation.clients[message.ClientID]
	if !exists {
		state = &clientState{totals: map[string]fruititem.FruitItem{}}
		aggregation.clients[message.ClientID] = state
	}

	for _, item := range message.Items {
		previous, found := state.totals[item.Fruit]
		if found {
			state.totals[item.Fruit] = previous.Sum(item)
		} else {
			state.totals[item.Fruit] = item
		}
	}
	state.partialsReceived++

	if state.partialsReceived < aggregation.sumAmount {
		return nil
	}

	consolidated := make([]fruititem.FruitItem, 0, len(state.totals))
	for _, item := range state.totals {
		consolidated = append(consolidated, item)
	}
	partialTop := fruittop.Top(consolidated, aggregation.topSize)

	serialized, err := inner.Serialize(inner.Message{ClientID: message.ClientID, Kind: inner.KindTop, Items: partialTop})
	if err != nil {
		return err
	}
	if err := aggregation.outputQueue.Send(*serialized); err != nil {
		return err
	}

	delete(aggregation.clients, message.ClientID)
	return nil
}
