package sum

import (
	"fmt"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
	"hash/fnv"
	"log/slog"
	"slices"
	"time"
)

const eofRedeliveryDelay = 10 * time.Millisecond

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	id                int
	sumAmount         int
	aggregationAmount int

	inputQueue        middleware.Middleware
	eofResendQueue    middleware.Middleware
	aggregationQueues []middleware.Middleware

	totalsByClient map[string]map[string]fruititem.FruitItem
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	sum := &Sum{
		id:                config.Id,
		sumAmount:         config.SumAmount,
		aggregationAmount: config.AggregationAmount,
		totalsByClient:    map[string]map[string]fruititem.FruitItem{},
	}

	var err error
	sum.inputQueue, err = middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	sum.eofResendQueue, err = middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	for i := 0; i < config.AggregationAmount; i++ {
		queueName := fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
		queue, err := middleware.CreateQueueMiddleware(queueName, connSettings)
		if err != nil {
			return nil, err
		}
		sum.aggregationQueues = append(sum.aggregationQueues, queue)
	}
	return sum, nil
}

func (sum *Sum) Run() error {
	return sum.inputQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	message, err := inner.Deserialize(&msg)
	if err != nil {
		slog.Error("Descartando mensaje inválido", "err", err)
		ack()
		return
	}

	switch message.Kind {
	case inner.KindData:
		sum.handleData(message)
	case inner.KindEOF:
		if err := sum.handleEOF(message); err != nil {
			slog.Error("Al procesar el EOF de un cliente", "client", message.ClientID, "err", err)
			nack()
			return
		}
	default:
		slog.Warn("Tipo de mensaje inesperado en Sum", "kind", message.Kind)
	}
	ack()
}

func (sum *Sum) handleData(message inner.Message) {
	totals, exists := sum.totalsByClient[message.ClientID]
	if !exists {
		totals = map[string]fruititem.FruitItem{}
		sum.totalsByClient[message.ClientID] = totals
	}
	for _, item := range message.Items {
		previous, found := totals[item.Fruit]
		if found {
			totals[item.Fruit] = previous.Sum(item)
		} else {
			totals[item.Fruit] = item
		}
	}
}

func (sum *Sum) handleEOF(eof inner.Message) error {
	alreadySeen := slices.Contains(eof.SeenBy, sum.id)

	if !alreadySeen {
		if err := sum.flushClient(eof.ClientID); err != nil {
			return err
		}
		eof.SeenBy = append(eof.SeenBy, sum.id)
		slog.Info("EOF procesado", "client", short(eof.ClientID), "sums_que_lo_vieron", len(eof.SeenBy), "de", sum.sumAmount)
	}

	if len(eof.SeenBy) >= sum.sumAmount {
		return nil
	}

	if alreadySeen {
		time.Sleep(eofRedeliveryDelay)
	}

	serialized, err := inner.Serialize(eof)
	if err != nil {
		return err
	}
	return sum.eofResendQueue.Send(*serialized)
}

func (sum *Sum) flushClient(clientID string) error {
	itemsPerAggregation := make([][]fruititem.FruitItem, sum.aggregationAmount)
	for _, item := range sum.totalsByClient[clientID] {
		target := aggregationFor(clientID, item.Fruit, sum.aggregationAmount)
		itemsPerAggregation[target] = append(itemsPerAggregation[target], item)
	}
	for target, items := range itemsPerAggregation {
		serialized, err := inner.Serialize(inner.Message{ClientID: clientID, Kind: inner.KindPartial, Items: items})
		if err != nil {
			return err
		}
		if err := sum.aggregationQueues[target].Send(*serialized); err != nil {
			return err
		}
	}
	delete(sum.totalsByClient, clientID)
	return nil
}

func aggregationFor(clientID string, fruit string, aggregationAmount int) int {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(clientID + "|" + fruit))
	return int(hasher.Sum32() % uint32(aggregationAmount))
}

func short(clientID string) string {
	if len(clientID) > 8 {
		return clientID[:8]
	}
	return clientID
}
