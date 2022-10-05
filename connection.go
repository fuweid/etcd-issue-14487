package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"
	pb "google.golang.org/grpc/examples/helloworld/helloworld"
)

type Dialer interface {
	Dial() (net.Conn, error)
}

type dailerWrapper struct {
	network string
	addr    string
}

func (dw dailerWrapper) Dial() (net.Conn, error) {
	return net.Dial(dw.network, dw.addr)
}

// server is used to implement helloworld.GreeterServer.
//
// ref. https://github.com/grpc/grpc-go/blob/master/examples/helloworld/greeter_server/main.go
type server struct {
	pb.UnimplementedGreeterServer
}

// SayHello implements helloworld.GreeterServer
func (s *server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	return &pb.HelloReply{Message: "Hello " + in.GetName()}, nil
}

func newServer(lis net.Listener) Dialer {
	s := grpc.NewServer()
	pb.RegisterGreeterServer(s, &server{})

	go func() {
		log.Printf("server listening at %v", lis.Addr())
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()
	return dailerWrapper{
		network: lis.Addr().Network(),
		addr:    lis.Addr().String(),
	}
}

// bridge proxies connections between listener and dialer, making it possible
// to disconnect grpc network connections without closing the logical grpc connection.
//
// ref. https://github.com/etcd-io/etcd/blob/main/tests/framework/integration/bridge.go bridge
type bridge struct {
	mu sync.Mutex

	dialer Dialer
	l      net.Listener
	conns  map[*bridgeConn]struct{}
	wg     sync.WaitGroup
}

func newBridge(dialer Dialer, lis net.Listener) (*bridge, error) {
	b := &bridge{
		// bridge "port" is ("%05d%05d0", port, pid) since go1.8 expects the port to be a number
		dialer: dialer,
		l:      lis,
		conns:  make(map[*bridgeConn]struct{}),
	}
	b.wg.Add(1)
	go b.serveListen()
	return b, nil
}

func (b *bridge) DropConnections() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for bc := range b.conns {
		bc.Close()
	}
	b.conns = make(map[*bridgeConn]struct{})
}

func (b *bridge) serveListen() {
	defer func() {
		b.l.Close()
		b.mu.Lock()
		for bc := range b.conns {
			bc.Close()
		}
		b.mu.Unlock()
		b.wg.Done()
	}()

	for {
		inc, ierr := b.l.Accept()
		if ierr != nil {
			return
		}

		outc, oerr := b.dialer.Dial()
		if oerr != nil {
			inc.Close()
			return
		}

		bc := &bridgeConn{inc, outc, make(chan struct{})}

		b.wg.Add(1)
		b.mu.Lock()
		b.conns[bc] = struct{}{}
		go b.serveConn(bc)
		b.mu.Unlock()
	}
}

func (b *bridge) serveConn(bc *bridgeConn) {
	defer func() {
		close(bc.doneCh)
		bc.Close()
		b.mu.Lock()
		delete(b.conns, bc)
		b.mu.Unlock()
		b.wg.Done()

	}()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		b.ioCopy(bc.out, bc.in)
		bc.close()
		wg.Done()
	}()
	go func() {
		b.ioCopy(bc.in, bc.out)
		bc.close()
		wg.Done()
	}()
	wg.Wait()
}

func (b *bridge) ioCopy(dst io.Writer, src io.Reader) (err error) {
	buf := make([]byte, 32*1024)
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if ew != nil {
				return ew
			}
			if nr != nw {
				return io.ErrShortWrite
			}
		}
		if er != nil {
			err = er
			break
		}
	}
	return err
}

type bridgeConn struct {
	in     net.Conn
	out    net.Conn
	doneCh chan struct{}
}

func (bc *bridgeConn) Close() {
	bc.close()
	<-bc.doneCh
}

func (bc *bridgeConn) close() {
	bc.in.Close()
	bc.out.Close()
}

func newLocalListener() net.Listener {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(fmt.Sprintf("failed to listen on a port: %v", err))
	}
	return lis
}
