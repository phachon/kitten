package protocol

import (
	"encoding/binary"
	"bytes"
	"io"
	"errors"
)

// kitten protocol implement
//+---------+-----------+-----------+-------------+-------------+
//| Header  | len(meta) | meta data | len(payload)| payload data|
//+---------+-----------+-----------+-------------+-------------+
//| [12]byte|  [4]byte  |           |   [4]byte   |             |
//+-------------------------------------------------------------+

// protocol Header
// format:
// [0] magicNumber number
// [1] version kitten protocol version
// [2]: 8 bit
// +------1bit----+------1bit-----+----1bit----+-----3bit-----+---------2bit-------+
// | message type | is heart beat | is one way | compress type| message status type|
// +--------------+---------------+------------+--------------+--------------------+
// [3] serialize type
// +------4bit-----+------4bit-----+
// | serialize type|               |
// +---------------+---------------+
// [4] ~ [11] sequence number messageId uint64

var (
	// meta sep line separator
	lineSeparator = []byte("\r\n")
)

const (
	// header len
	Header_Len int = 12
	// magic number
	MagicNumber byte = 0x08
)

const (
	Message_Type_Request byte = iota
	Message_Type_Response
)

const (
	Compress_Type_None byte = iota
	Compress_Type_Gzip
)

const (
	Message_Status_Normal byte = iota
	Message_Status_Exception
)

const (
	Serialize_None byte = iota
	Serialize_Json
)

type Header [Header_Len]byte

// protocol Message header + body
type Message struct {
	Header *Header
	MetaData map[string]string
	Payload []byte
}

// Get Message instance
func NewMessage() *Message  {
	header := Header([Header_Len]byte{})
	header[0] = MagicNumber
	return &Message{
		Header: &header,
		MetaData: make(map[string]string),
		Payload: make([]byte, 1),
	}
}

// Check magic number
func (header *Header) CheckMagicNumber() bool {
	return header[0] == MagicNumber
}

// Set header version
func (header *Header) SetVersion(version byte)  {
	header[1] = version
}

// Get header version
func (header *Header) Version() byte {
	return header[1]
}

// Set header message type (Request or Response)
func (header *Header) SetMessageType(messageType byte)  {
	header[2] = header[2] | (messageType << 7)
}

// Get header message type
func (header *Header) MessageType() byte {
	return (header[2] & 0x80) >> 7
}

// Set heart beat
func (header *Header) SetHeartBeat(heartBeat bool)  {
	if heartBeat {
		header[2] = header[2] | 0x40
	}else {
		header[2] = header[2] &^ 0x40
	}
}

// Get heart beat
func (header *Header) IsHeartBeat() bool {
	return (header[2] & 0x40) == 0x40
}

// Set one way
func (header *Header) SetOneWay(oneWay bool) {
	if oneWay {
		header[2] = header[2] | 0x20
	}else {
		header[2] = header[2] &^ 0x20
	}
}

// Get is one way
func (header *Header) IsOneWay() bool {
	return (header[2] & 0x20) == 0x20
}

// Set compress type
func (header *Header) SetCompressType(compressType byte) {
	header[2] = header[2] | ((compressType << 2) & 0x1c)
}

// Get compress type
func (header *Header) CompressType() byte  {
	return (header[2] & 0x1c) >> 2
}

// Set message type
func (header *Header) SetMessageStatusType(messageType byte) {
	header[2] = header[2] | (messageType & 0x03)
}

// Get message type
func (header *Header) MessageStatusType() byte {
	return header[2] & 0x03
}

// Set serialize type
func (header *Header) SetSerializeType(serializeType byte) {
	header[3] = header[3] | (serializeType << 4)
}

// Get serialize type
func (header *Header) SerializeType() byte {
	return (header[3] & 0xF0) >> 4
}

// Set seq number
// BigEndian 大端
func (header *Header) SetSeq(seq uint64)  {
	binary.BigEndian.PutUint64(header[4:], seq)
}

// Get seq number
func (header *Header) Seq() uint64 {
	return binary.BigEndian.Uint64(header[4:])
}

// Set meta data
func (message *Message) SetMetaData(meta map[string]string) {
	message.MetaData = meta
}

// Set payload
func (message *Message) SetPayload(payload []byte)  {
	message.Payload = payload
}

// Encode message
func (message *Message) Encode() []byte {

	metaData := message.MetaData
	payload := message.Payload

	meta := encodeMeta(metaData)
	messageLen := Header_Len + 4 + len(meta) + 4 + len(payload)

	data := make([]byte, messageLen)
	copy(data, message.Header[:])

	binary.BigEndian.PutUint32(data[12:16], uint32(len(meta)))
	copy(data[16:], meta)

	binary.BigEndian.PutUint32(data[16+len(meta):], uint32(len(payload)))
	copy(data[20+len(meta):], payload)

	return data
}

// write to writers
func (message *Message) WriteTo(w io.Writer) error  {
	// write header
	_, err := w.Write(message.Header[:])
	if err != nil {
		return err
	}

	meta := encodeMeta(message.MetaData)
	err = binary.Write(w, binary.BigEndian, uint32(len(meta)))
	if err != nil {
		return err
	}

	_, err = w.Write(meta)
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.BigEndian, uint32(len(message.Payload)))
	if err != nil {
		return err
	}

	_, err = w.Write(message.Payload)

	return err
}

// encode metaData
func encodeMeta(encodeData map[string]string) []byte {
	var buf bytes.Buffer
	for k, v := range encodeData {
		buf.WriteString(k)
		buf.Write(lineSeparator)
		buf.WriteString(v)
		buf.Write(lineSeparator)
	}

	return buf.Bytes()
}

// read message from writer
func readMessage(r io.Reader)(*Message, error) {

	msg := NewMessage()

	// read header
	_, err := io.ReadFull(r, msg.Header[:])
	if err != nil {
		return nil, err
	}

	// read meta len and meta
	lenData := make([]byte, 4)
	msg.MetaData, err = decodeMeta(lenData, r)
	if err != nil {
		return nil, err
	}

	// read payload len
	_, err = io.ReadFull(r, lenData)
	if err != nil {
		return nil, err
	}
	l := binary.BigEndian.Uint32(lenData)
	msg.Payload = make([]byte, l)

	_, err = io.ReadFull(r, msg.Payload)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

// decode metaData
func decodeMeta(lenData []byte, r io.Reader) (map[string]string, error) {

	// read len meta
	_, err := io.ReadFull(r, lenData)
	if err != nil {
		return nil, err
	}
	// to uint32
	metaLen := binary.BigEndian.Uint32(lenData)
	if metaLen == 0 {
		return nil, err
	}

	metaByte := make([]byte, metaLen)
	_, err = io.ReadFull(r, metaByte)
	if err != nil {
		return nil, err
	}

	metaData := bytes.Split(metaByte, lineSeparator)
	if len(metaData) % 2 != 1 {
		return nil, errors.New("last element is empty!")
	}

	meta := make(map[string]string)
	for i := 0; i < len(metaData) - 1; i = i+2 {
		key := string(metaData[i])
		val := string(metaData[i+1])

		meta[key] = val
	}

	return meta, nil
}