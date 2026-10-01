package inner

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type FieldType byte

const (
	ClientIDField FieldType = 0x01
	KindField     FieldType = 0x02
	ItemsField    FieldType = 0x03
	SeenByField   FieldType = 0x04

	ItemField   FieldType = 0x01
	FruitField  FieldType = 0x01
	AmountField FieldType = 0x02

	SumIDField FieldType = 0x01
)

type Kind byte

const (
	KindData Kind = iota
	KindEOF
	KindPartial
	KindTop
	KindResult
)

type Message struct {
	ClientID string
	Kind     Kind
	Items    []fruititem.FruitItem
	SeenBy   []int
}

func encodeTLV(fieldType FieldType, value []byte) []byte {
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(fieldType))
	length := make([]byte, 2)
	binary.BigEndian.PutUint16(length, uint16(len(value)))
	buf.Write(length)
	buf.Write(value)
	return buf.Bytes()
}

func encodeUint32(value uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, value)
	return buf
}

func decodeUint32(value []byte) uint32 {
	return binary.BigEndian.Uint32(value)
}

func readTLV(data []byte, offset int) (FieldType, []byte, int, error) {
	if offset+3 > len(data) {
		return 0, nil, 0, errors.New("TLV truncado (header)")
	}
	fieldType := FieldType(data[offset])
	length := int(binary.BigEndian.Uint16(data[offset+1 : offset+3]))
	start := offset + 3
	if start+length > len(data) {
		return 0, nil, 0, errors.New("TLV truncado (value)")
	}
	return fieldType, data[start : start+length], start + length, nil
}

func encodeItem(item fruititem.FruitItem) []byte {
	buf := new(bytes.Buffer)
	buf.Write(encodeTLV(FruitField, []byte(item.Fruit)))
	buf.Write(encodeTLV(AmountField, encodeUint32(item.Amount)))
	return buf.Bytes()
}

func decodeItem(data []byte) (fruititem.FruitItem, error) {
	var item fruititem.FruitItem
	offset := 0
	for offset < len(data) {
		fieldType, value, next, err := readTLV(data, offset)
		if err != nil {
			return item, err
		}
		switch fieldType {
		case FruitField:
			item.Fruit = string(value)
		case AmountField:
			if len(value) != 4 {
				return item, errors.New("amount inválido")
			}
			item.Amount = decodeUint32(value)
		}
		offset = next
	}
	return item, nil
}

func Serialize(message Message) (*middleware.Message, error) {
	buf := new(bytes.Buffer)
	buf.Write(encodeTLV(ClientIDField, []byte(message.ClientID)))
	buf.Write(encodeTLV(KindField, []byte{byte(message.Kind)}))

	items := new(bytes.Buffer)
	for _, item := range message.Items {
		items.Write(encodeTLV(ItemField, encodeItem(item)))
	}
	buf.Write(encodeTLV(ItemsField, items.Bytes()))

	seenBy := new(bytes.Buffer)
	for _, id := range message.SeenBy {
		seenBy.Write(encodeTLV(SumIDField, encodeUint32(uint32(id))))
	}
	buf.Write(encodeTLV(SeenByField, seenBy.Bytes()))

	return &middleware.Message{Body: buf.String()}, nil
}

func Deserialize(raw *middleware.Message) (Message, error) {
	data := []byte(raw.Body)
	message := Message{Items: []fruititem.FruitItem{}}

	offset := 0
	for offset < len(data) {
		fieldType, value, next, err := readTLV(data, offset)
		if err != nil {
			return Message{}, fmt.Errorf("mensaje interno inválido: %w", err)
		}

		switch fieldType {
		case ClientIDField:
			message.ClientID = string(value)

		case KindField:
			if len(value) != 1 {
				return Message{}, errors.New("kind inválido")
			}
			message.Kind = Kind(value[0])

		case ItemsField:
			itemOffset := 0
			for itemOffset < len(value) {
				itemType, itemValue, itemNext, err := readTLV(value, itemOffset)
				if err != nil {
					return Message{}, err
				}
				if itemType == ItemField {
					item, err := decodeItem(itemValue)
					if err != nil {
						return Message{}, err
					}
					message.Items = append(message.Items, item)
				}
				itemOffset = itemNext
			}

		case SeenByField:
			seenOffset := 0
			for seenOffset < len(value) {
				seenType, seenValue, seenNext, err := readTLV(value, seenOffset)
				if err != nil {
					return Message{}, err
				}
				if seenType == SumIDField {
					message.SeenBy = append(message.SeenBy, int(decodeUint32(seenValue)))
				}
				seenOffset = seenNext
			}
		}
		offset = next
	}

	if message.ClientID == "" {
		return Message{}, errors.New("mensaje interno sin client_id")
	}
	if message.Kind > KindResult {
		return Message{}, fmt.Errorf("mensaje interno de tipo desconocido: %d", message.Kind)
	}
	return message, nil
}
