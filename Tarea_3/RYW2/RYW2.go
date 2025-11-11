package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	proto "example1/proto"

	"google.golang.org/grpc"
)

const (
	// La dirección del servidor Lester   = "10.35.168.59:50051"
	// La dirección del servidor Franklin = "10.35.168.60:50052"
	// La dirección del servidor Trevor   = "10.35.168.61:50053"

	DB1Addr = "10.35.168.59:51000"
	DB3Addr = "10.35.168.59:57000"
)

// NodoDB representa un nodo de base de datos
type NodoDB struct {
	proto.UnimplementedBrokerServiceServer
	mu     sync.Mutex
	offers map[string]proto.Offer // Mapa para almacenar las ofertas
	nodeID string                 // Identificador del nodo
}

func (n *NodoDB) SendOffer(ctx context.Context, offer *proto.Offer) (*proto.Response, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Almacenar la oferta en el nodo
	n.offers[offer.OfertaId] = *offer

	// Responder al broker con un mensaje de confirmación
	response := &proto.Response{
		Mensaje: fmt.Sprintf("ACK: Oferta %s almacenada en %s", offer.OfertaId, n.nodeID),
	}
	return response, nil
}

func (n *NodoDB) GetHistory(ctx context.Context, req *proto.HistoryRequest) (*proto.HistoryResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Devolver todas las ofertas almacenadas en este nodo
	var offers []*proto.Offer
	for _, offer := range n.offers {
		o := offer
		offers = append(offers, &o)
	}

	response := &proto.HistoryResponse{
		Offers: offers,
	}
	return response, nil
}

func main() {
	server := grpc.NewServer()

	nodo := &NodoDB{
		nodeID: "DB2",
		offers: make(map[string]proto.Offer),
	}

	// Registrar el servidor del nodo
	proto.RegisterBrokerServiceServer(server, nodo)

	go func() {
		time.Sleep(500 * time.Millisecond)
		peers := []string{DB1Addr, DB3Addr}
		for _, p := range peers {
			cctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			conn, err := grpc.DialContext(cctx, p, grpc.WithInsecure())
			cancel()
			if err != nil {
				continue
			}
			client := proto.NewBrokerServiceClient(conn)
			ctx, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
			resp, err := client.GetHistory(ctx, &proto.HistoryRequest{ConsumidorId: "sync"})
			cancel2()
			_ = conn.Close()
			if err != nil || resp == nil {
				continue
			}
			nodo.mu.Lock()
			for _, of := range resp.Offers {
				nodo.offers[of.OfertaId] = *of
			}
			nodo.mu.Unlock()
		}
	}()

	listener, err := net.Listen("tcp", ":52000")
	if err != nil {
		log.Fatalf("Error al escuchar en el puerto: %v", err)
	}

	fmt.Printf("Nodo %s escuchando en el puerto 52000...\n", nodo.nodeID)
	if err := server.Serve(listener); err != nil {
		log.Fatalf("Error al iniciar el servidor: %v", err)
	}
}

