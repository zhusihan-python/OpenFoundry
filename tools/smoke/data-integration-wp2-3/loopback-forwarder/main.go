// Command loopback-forwarder answers one port of its network namespace's
// loopback interface by connecting each caller to another address.
//
// The dataset service signs every download under the files base URL, which
// this stack keeps on the host's loopback. The edge gateway follows that
// redirect itself, from inside its container, where the host's loopback is
// out of reach. Started in the gateway's network namespace, this command
// answers the signed address there. It binds 127.0.0.1 alone, and takes no
// other interface.
package main

import (
	"flag"
	"io"
	"log"
	"net"
	"strconv"
)

func main() {
	log.SetFlags(0)
	listenPort := flag.Int("listen-port", 0, "loopback port to answer")
	target := flag.String("target", "", "address each answered connection is connected to, host:port")
	flag.Parse()
	if *listenPort < 1 || *listenPort > 65535 || *target == "" {
		log.Fatal("usage: loopback-forwarder -listen-port PORT -target HOST:PORT")
	}
	listener, err := listen(*listenPort)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("forwarding 127.0.0.1:%d to %s", *listenPort, *target)
	log.Fatal(serve(listener, *target))
}

// listen answers the port on the loopback interface alone.
func listen(port int) (net.Listener, error) {
	return net.Listen("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
}

// serve answers every connection the listener accepts, until the listener fails.
func serve(listener net.Listener, target string) error {
	for {
		answered, err := listener.Accept()
		if err != nil {
			return err
		}
		go forward(answered, target)
	}
}

// forward carries one answered connection to the target and back. The first
// direction to end closes both connections, and with them the other direction.
func forward(answered net.Conn, target string) {
	defer func() { _ = answered.Close() }()
	upstream, err := net.Dial("tcp", target)
	if err != nil {
		log.Printf("connect %s: %v", target, err)
		return
	}
	defer func() { _ = upstream.Close() }()
	ended := make(chan error, 2)
	go carry(ended, upstream, answered)
	go carry(ended, answered, upstream)
	if err := <-ended; err != nil {
		log.Printf("forward to %s: %v", target, err)
	}
}

func carry(ended chan<- error, to net.Conn, from net.Conn) {
	_, err := io.Copy(to, from)
	ended <- err
}
