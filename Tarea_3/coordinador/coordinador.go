package main

import (
	"context"
	"log"
	"net"
	"sync"

	proto "heint/proto"
	"google.golang.org/grpc"
)

type Coordinador struct {
	proto.UnimplementedBrokerServiceServer
	mu        sync.Mutex
	asientos  map[string]bool
}

func (b *Coordinador) SendEstado(ctx context.Context, req *proto.Response) (*proto.Estado, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Responder con el estado actual del sistema (mapa de asientos)
	log.Printf("[BROKER] Nodo solicitó estado — %s", req.Mensaje)

	estado := &proto.Estado{
		Id: 1, // ID del sistema o del broker
		AsientosDisponibles: b.asientos,
	}

	return estado, nil
}

func (b *Coordinador) SendOffer(ctx context.Context, req *proto.Response) (*proto.Response, error) {
	log.Printf("[BROKER] Oferta recibida: %s", req.Mensaje)
	return &proto.Response{Mensaje: "Oferta procesada correctamente"}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":54000")
	if err != nil {
		log.Fatalf("Error al abrir puerto: %v", err)
	}

	server := grpc.NewServer()

	coordinador := &Coordinador{
		asientos: map[string]bool{
			"A1": true,
			"A2": false,
			"B1": true,
			"B2": true,
			"C1": false,
		},
	}

	proto.RegisterBrokerServiceServer(server, coordinador)

	log.Println("[BROKER] Servidor iniciado en :54000")
	if err := server.Serve(lis); err != nil {
		log.Fatalf("Error al iniciar gRPC server: %v", err)
	}
}
