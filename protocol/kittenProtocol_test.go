package protocol

import (
	"testing"
	"bytes"
)

func TestMessage(t *testing.T) {

	req := NewMessage()
	req.Header.SetVersion(0)
	req.Header.SetMessageType(Message_Type_Request)
	req.Header.SetHeartBeat(true)
	req.Header.SetOneWay(true)
	req.Header.SetCompressType(Compress_Type_None)
	req.Header.SetMessageStatusType(Message_Status_Normal)
	req.Header.SetSerializeType(Serialize_Json)
	req.Header.SetSeq(123456789)

	meta := make(map[string]string)
	meta["__METHOD"] = "Author.Login"
	meta["__ID"] = "10-9dad-11d1-80b4-00"
	req.SetMetaData(meta)

	payload := `{"A": 1, "B": 2,}`
	req.SetPayload([]byte(payload))

	m := req.Header.CheckMagicNumber()
	if m != true {
		t.Fatal("check magic number false")
	}

	var buf bytes.Buffer
	err := req.WriteTo(&buf)
	if err != nil {
		t.Fatal(err.Error())
	}

	res, err := readMessage(&buf)
	if err != nil {
		t.Fatal(err.Error())
	}

	res.Header.SetMessageType(Message_Type_Response)

	v := res.Header.Version()
	if v != byte(0) {
		t.Fatal("get version false")
	}

	hb := res.Header.IsHeartBeat()
	if hb != true {
		t.Fatal("is heart beat false")
	}

	ow := res.Header.IsOneWay()
	if ow != true {
		t.Fatal("is one way false")
	}

	compressType := res.Header.CompressType()
	if compressType != Compress_Type_None {
		t.Fatal("get compress type false")
	}

	messageStatus := res.Header.MessageStatusType()
	if messageStatus != Message_Status_Normal {
		t.Fatal("get message status false")
	}

	s := res.Header.SerializeType()
	if s != Serialize_Json {
		t.Fatal("get serialize false")
	}

	seq := res.Header.Seq()
	if seq != 123456789 {
		t.Fatal("get seq bumber false")
	}

	if res.MetaData["__METHOD"] != "Author.Login" && res.MetaData["__ID"] != "10-9dad-11d1-80b4-00" {
		t.Fatal("meta data error")
	}

	if string(res.Payload) != payload {
		t.Fatal("payload data error")
	}
}
