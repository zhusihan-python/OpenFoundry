package main

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"
)

const deadline = 5 * time.Second

// echo answers every connection with what it was sent, and reports when the
// other end closes it.
type echo struct {
	address net.Addr
	closed  chan struct{}
}

func newEcho(t *testing.T) *echo {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	server := &echo{address: listener.Addr(), closed: make(chan struct{}, 8)}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				defer func() { server.closed <- struct{}{} }()
				buffer := make([]byte, 64)
				for {
					read, err := conn.Read(buffer)
					if read > 0 {
						if _, err := conn.Write(buffer[:read]); err != nil {
							return
						}
					}
					if err != nil {
						return
					}
				}
			}()
		}
	}()
	return server
}

// forwarder starts this command's forwarding on a loopback port it picks itself.
func forwarder(t *testing.T, target net.Addr) net.Addr {
	t.Helper()
	listener, err := listen(0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	// The listener closing is how the test ends the forwarding.
	go func() {
		_ = serve(listener, target.String())
	}()
	return listener.Addr()
}

// speak opens one connection, sends a message and requires it back.
func speak(t *testing.T, address net.Addr, message []byte) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", address.String(), deadline)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(deadline)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(message); err != nil {
		t.Fatal(err)
	}
	heard := make([]byte, len(message))
	if _, err := io.ReadFull(conn, heard); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(heard, message) {
		t.Fatalf("heard %q, want %q", heard, message)
	}
	return conn
}

func TestListenAnswersTheLoopbackInterfaceAlone(t *testing.T) {
	listener, err := listen(0)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	address, ok := listener.Addr().(*net.TCPAddr)
	if !ok || !address.IP.Equal(net.IPv4(127, 0, 0, 1)) {
		t.Fatalf("listening on %v, want 127.0.0.1 alone", listener.Addr())
	}
}

func TestForwardCarriesEachConnectionBothWays(t *testing.T) {
	server := newEcho(t)
	address := forwarder(t, server.address)
	speak(t, address, []byte("first connection"))
	speak(t, address, []byte("second connection"))
}

func TestForwardEndsTheTargetConnectionWithTheCaller(t *testing.T) {
	server := newEcho(t)
	address := forwarder(t, server.address)
	conn := speak(t, address, []byte("one connection"))
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-server.closed:
	case <-time.After(deadline):
		t.Fatal("the target connection stayed open after the caller closed")
	}
}
