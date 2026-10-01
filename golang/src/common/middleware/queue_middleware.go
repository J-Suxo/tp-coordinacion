package middleware

import (
	"context"
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"sync"
)

type QueueMiddleware struct {
	channel     *amqp.Channel
	conn        *amqp.Connection
	queueName   string
	mutex       sync.Mutex
	consumerTag string
	ctxClose    context.Context
	close       context.CancelFunc
}

func NewQueueMiddleware(queueName string, connectionSettings ConnSettings) (*QueueMiddleware, error) {
	brokerURI := fmt.Sprintf("amqp://guest:guest@%s:%d/",
		connectionSettings.Hostname, connectionSettings.Port)

	conn, err := amqp.Dial(brokerURI)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	_, err = ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}
	return &QueueMiddleware{
		channel:   ch,
		conn:      conn,
		queueName: queueName,
	}, nil
}

func (queueMw *QueueMiddleware) Send(msg Message) error {
	err := queueMw.channel.PublishWithContext(
		context.Background(),
		"",
		queueMw.queueName,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		},
	)
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return fmt.Errorf("%w: %w", ErrMessageMiddlewareDisconnected, err)
		}
		return fmt.Errorf("%w: %w", ErrMessageMiddlewareMessage, err)
	}
	return nil
}

func (queueMw *QueueMiddleware) StartConsuming(
	callbackFunc func(msg Message, ack func(), nack func()),
) error {
	if err := queueMw.channel.Qos(1, 0, false); err != nil {
		return fmt.Errorf("failed to set QoS: %w: %w", ErrMessageMiddlewareMessage, err)
	}

	queueMw.mutex.Lock()
	queueMw.consumerTag = fmt.Sprintf("consumer-%s", queueMw.queueName)
	queueMw.mutex.Unlock()

	deliveries, err := queueMw.channel.Consume(
		queueMw.queueName,
		queueMw.consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %w: %w", ErrMessageMiddlewareMessage, err)
	}

	queueMw.mutex.Lock()
	queueMw.ctxClose, queueMw.close = context.WithCancel(context.Background())
	queueMw.mutex.Unlock()

	for {
		select {
		case <-queueMw.ctxClose.Done():
			return nil

		case delivery, ok := <-deliveries:
			if !ok {
				return ErrMessageMiddlewareDisconnected
			}

			message := Message{Body: string(delivery.Body)}
			log.Printf("Received a message: %s", message.Body)

			ack := func() {
				multiple := false
				if err := delivery.Ack(multiple); err != nil {
					log.Printf("Failed to ack message: %v", err)
				}
			}

			nack := func() {
				multiple := false
				requeue := true
				if err := delivery.Nack(multiple, requeue); err != nil {
					log.Printf("Failed to nack message: %v", err)
				}
			}

			callbackFunc(message, ack, nack)
		}
	}
}

func (queueMw *QueueMiddleware) StopConsuming() error {
	queueMw.mutex.Lock()
	consumerTag := queueMw.consumerTag
	cancel := queueMw.close
	queueMw.mutex.Unlock()

	if consumerTag != "" {
		noWait := false
		if err := queueMw.channel.Cancel(consumerTag, noWait); err != nil {
			if cancel != nil {
				cancel()
			}
			return fmt.Errorf("%w: %w", ErrMessageMiddlewareDisconnected, err)
		}
	}
	if cancel != nil {
		cancel()
	}
	return nil}

func (queueMw *QueueMiddleware) Close() error {
	var chErr, connErr error
	if queueMw.channel != nil {
		chErr = queueMw.channel.Close()
	}
	connErr = queueMw.conn.Close()

	if chErr != nil {
		return fmt.Errorf("%w: %w", ErrMessageMiddlewareClose, chErr)
	}
	if connErr != nil {
		return fmt.Errorf("%w: %w", ErrMessageMiddlewareClose, connErr)
	}
	return nil
}
