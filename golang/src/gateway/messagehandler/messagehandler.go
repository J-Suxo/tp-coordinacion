package messagehandler

import (
	"crypto/rand"
	"encoding/hex"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type MessageHandler struct {
	clientID string
}

func newClientID() string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		panic("failed to generate client ID: " + err.Error())
	}
	return hex.EncodeToString(bytes[:])
}

func NewMessageHandler() MessageHandler {
	return MessageHandler{clientID: newClientID()}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	return inner.Serialize(inner.Message{
		ClientID: messageHandler.clientID,
		Kind:     inner.KindData,
		Items:    []fruititem.FruitItem{fruitRecord},
	})
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	return inner.Serialize(inner.Message{
		ClientID: messageHandler.clientID,
		Kind:     inner.KindEOF,
	})
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	internalMessage, err := inner.Deserialize(message)
	if err != nil {
		return nil, err
	}
	if internalMessage.Kind != inner.KindResult || internalMessage.ClientID != messageHandler.clientID {
		return nil, nil
	}
	return internalMessage.Items, nil
}
