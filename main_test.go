package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "google.golang.org/grpc/examples/helloworld/helloworld"
	"google.golang.org/grpc/grpclog"
)

func init() {
	grpclog.SetLoggerV2(grpclog.NewLoggerV2(os.Stderr, os.Stderr, os.Stderr))
}

func TestETCDIssue11487(t *testing.T) {
	var (
		serverLis = newLocalListener()
		bridgeLis = newLocalListener()
	)

	b, err := newBridge(newServer(serverLis), bridgeLis)
	if err != nil {
		t.Fatal(err)
	}

	// Set up a connection to the server by bridge
	conn, err := grpc.Dial(
		bridgeLis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewGreeterClient(conn)

	ctx := context.TODO()
	for i := 0; i < 1000000; i++ {
		donec := make(chan struct{})
		stopc := make(chan struct{})

		// DropConnections will close the connection and force gRPC
		// subConn state to be in IDLE and reconnect.
		b.DropConnections()
		go func() {
			defer close(donec)
			for {
				select {
				case <-stopc:
					return
				default:
					b.DropConnections()
					time.Sleep(1 * time.Millisecond)
				}
			}
		}()

		for {
			// retry it if err isn't nil
			_, err := c.SayHello(ctx, &pb.HelloRequest{Name: "testing"})
			if err == nil {
				break

			}
			fmt.Println(err)
		}

		// the gRPC call is succefull and let's try it next round
		close(stopc)
		<-donec
		fmt.Println(i)
	}
}
