package transport

import (
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	troupe "github.com/ShiningRay/troupego"
)

func TestDebugRawWire(t *testing.T) {
	stage, _ := cartStage(t)
	srv := startServer(t, stage)
	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	id := troupe.MustActorID("app", "cart", "t1")
	env2 := Envelope{T: "req", ID: 2, Actor: id.Canonical(), M: "on_stage", A: json.RawMessage(`null`), DL: time.Now().Add(5 * time.Second).UnixMilli()}
	b2, _ := json.Marshal(env2)
	if err := WriteFrame(conn, b2); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	rep2, err := ReadFrame(conn, DefaultMaxFrame)
	fmt.Printf("rep2 err=%v payload=%s\n", err, string(rep2))
}
