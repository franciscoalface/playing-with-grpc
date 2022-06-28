package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/franciscoalface/playing-with-grpc/pb"
	"google.golang.org/grpc"
)

func main() {
	connection, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Could not connect to gRPC Server: %v", err)
	}

	defer connection.Close()

	client := pb.NewUserServiceClient(connection)

	// Add a user
	// AddUser(client)

	// Server streaming
	// AddUserVerbose(client)

	// Client streaming
	// AddUsers(client)

	// Bi-directional streaming
	AddUserStreamBoth(client)
}

func AddUser(client pb.UserServiceClient) {
	req := &pb.User{
		Id:    "1",
		Name:  "Francisco",
		Email: "francisco@email.com",
	}

	res, err := client.AddUser(context.Background(), req)
	if err != nil {
		log.Fatalf("Could not make gRPC request: %v", err)
	}

	log.Println(res)
}

func AddUserVerbose(client pb.UserServiceClient) {
	req := &pb.User{
		Id:    "1",
		Name:  "Francisco",
		Email: "francisco@email.com",
	}

	res, err := client.AddUserVerbose(context.Background(), req)
	if err != nil {
		log.Fatalf("Could not make gRPC request: %v", err)
	}

	for {
		stream, err := res.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Could not receive the message: %v", err)
		}
		fmt.Println("Status:", stream.Status)
		fmt.Println("User:", stream.User)
	}
}

func AddUsers(client pb.UserServiceClient) {
	reqs := []*pb.User{
		&pb.User{
			Id:    "1",
			Name:  "Francisco",
			Email: "fancisco@email.com",
		},
		&pb.User{
			Id:    "2",
			Name:  "Chuck",
			Email: "chuck@email.com",
		},
		&pb.User{
			Id:    "3",
			Name:  "Henrique",
			Email: "henrique@email.com",
		},
		&pb.User{
			Id:    "4",
			Name:  "Mike",
			Email: "mike@email.com",
		},
		&pb.User{
			Id:    "5",
			Name:  "Marpin",
			Email: "marpin@email.com",
		},
		&pb.User{
			Id:    "6",
			Name:  "Doug",
			Email: "doug@email.com",
		},
	}

	stream, err := client.AddUsers(context.Background())
	if err != nil {
		log.Fatalf("Error creating request: %v", err)
	}

	for _, req := range reqs {
		stream.Send(req)
		time.Sleep(time.Second * 3)
	}

	res, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("Error receiving response: %v", err)
	}

	fmt.Println(res)
}

func AddUserStreamBoth(client pb.UserServiceClient) {
	stream, err := client.AddUserStreamBoth(context.Background())
	if err != nil {
		log.Fatalf("Error creating request: %v", err)
	}

	reqs := []*pb.User{
		&pb.User{
			Id:    "1",
			Name:  "Francisco",
			Email: "fancisco@email.com",
		},
		&pb.User{
			Id:    "2",
			Name:  "Chuck",
			Email: "chuck@email.com",
		},
		&pb.User{
			Id:    "3",
			Name:  "Henrique",
			Email: "henrique@email.com",
		},
		&pb.User{
			Id:    "4",
			Name:  "Mike",
			Email: "mike@email.com",
		},
		&pb.User{
			Id:    "5",
			Name:  "Marpin",
			Email: "marpin@email.com",
		},
		&pb.User{
			Id:    "6",
			Name:  "Doug",
			Email: "doug@email.com",
		},
	}

	wait := make(chan int)

	go func() {
		for _, req := range reqs {
			fmt.Println("Sending user:", req.Name)
			stream.Send(req)
			time.Sleep(time.Second * 3)
		}
		stream.CloseSend()
	}()

	go func() {
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("Error receiving data: %v", err)
			}
			fmt.Printf("Receiving user %v with status: %v \n", res.GetUser().GetName(), res.GetStatus())
		}
		close(wait)
	}()

	<-wait
}
