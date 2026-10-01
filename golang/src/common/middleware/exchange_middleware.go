package middleware

import (
	"context"
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"sync"
)

type ExchangeMiddleware struct {
	conn        *amqp.Connection
	channel     *amqp.Channel
	keys        []string
	exchange    string
	mutex       sync.Mutex
	ctxClose    context.Context
	close       context.CancelFunc
	consumerTag string
}

func NewExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (*ExchangeMiddleware, error) {
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
	if err := ch.ExchangeDeclare(
		exchange,
		"direct",
		false,
		false,
		false,
		false,
		nil,
	); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare an exchange: %w", err)
	}

	return &ExchangeMiddleware{
		conn:     conn,
		channel:  ch,
		keys:     keys,
		exchange: exchange,
	}, nil
}

func (exchangeMw *ExchangeMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	queueName, err := exchangeMw.channel.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)

	if err != nil {
		return fmt.Errorf("%w: failed to declare a queue: %w", ErrMessageMiddlewareMessage, err)
	}

	for _, key := range exchangeMw.keys {
		if err := exchangeMw.channel.QueueBind(
			queueName.Name,
			key,
			exchangeMw.exchange,
			false,
			nil,
		); err != nil {
			return fmt.Errorf("%w: failed to bind a queue: %w", ErrMessageMiddlewareMessage, err)
		}
	}

	exchangeMw.mutex.Lock()
	exchangeMw.consumerTag = fmt.Sprintf("consumer-%s", queueName.Name)
	exchangeMw.mutex.Unlock()

	delivery, err := exchangeMw.channel.Consume(
		queueName.Name,
		exchangeMw.consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("%w: failed to register a consumer: %w", ErrMessageMiddlewareMessage, err)
	}

	exchangeMw.mutex.Lock()
	exchangeMw.ctxClose, exchangeMw.close = context.WithCancel(context.Background())
	exchangeMw.mutex.Unlock()

	for {
		select {
		case <-exchangeMw.ctxClose.Done():
			return nil

		case delivery, ok := <-delivery:
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

func (exchangeMw *ExchangeMiddleware) Send(msg Message) error {
	for _, key := range exchangeMw.keys {
		err := exchangeMw.channel.PublishWithContext(
			context.Background(),
			exchangeMw.exchange,
			key,
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
	}
	return nil
}

func (exchangeMw *ExchangeMiddleware) StopConsuming() error {
	exchangeMw.mutex.Lock()
	consumerTag := exchangeMw.consumerTag
	cancel := exchangeMw.close
	exchangeMw.mutex.Unlock()

	if consumerTag != "" {
		noWait := false
		if err := exchangeMw.channel.Cancel(consumerTag, noWait); err != nil {
			if cancel != nil {
				cancel()
			}
			return fmt.Errorf("%w: %w", ErrMessageMiddlewareDisconnected, err)
		}
	}
	if cancel != nil {
		cancel()
	}
	return nil
}

func (exchangeMw *ExchangeMiddleware) Close() error {
	var chErr, connErr error
	if exchangeMw.channel != nil {
		chErr = exchangeMw.channel.Close()
	}
	connErr = exchangeMw.conn.Close()

	if chErr != nil {
		return fmt.Errorf("%w: %w", ErrMessageMiddlewareClose, chErr)
	}
	if connErr != nil {
		return fmt.Errorf("%w: %w", ErrMessageMiddlewareClose, connErr)
	}
	return nil
}
