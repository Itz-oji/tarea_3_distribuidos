package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"

	proto "example1/proto"

	"google.golang.org/grpc"
)

const (
	// La dirección del servidor Lester   = "10.35.168.59:50051"
	// La dirección del servidor Franklin = "10.35.168.60:50052"
	// La dirección del servidor Trevor   = "10.35.168.61:50053"

	DB2Addr = "10.35.168.59:52000"
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

	// Almacenar la oferta en el nodo (override con la versión más reciente)
	n.offers[offer.OfertaId] = *offer

	// Simular almacenamiento persistente con marca de tiempo local
	_ = time.Now()

	// Responder al broker con un ACK
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

	log.Printf("Nodo DB1 iniciando recuperación tras caída...")
	time.Sleep(2 * time.Second)
	log.Printf("Nodo DB1 se ha reintegrado correctamente al sistema")

	server := grpc.NewServer()

	nodo := &NodoDB{
		nodeID: "DB1",
		offers: make(map[string]proto.Offer),
	}

	proto.RegisterBrokerServiceServer(server, nodo)

	go func() {
		time.Sleep(500 * time.Millisecond)
		peers := []string{DB2Addr, DB3Addr}
		for _, p := range peers {
			conn, err := grpc.Dial(p, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(1*time.Second))
			if err != nil {
				continue
			}
			client := proto.NewBrokerServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			resp, err := client.GetHistory(ctx, &proto.HistoryRequest{ConsumidorId: "sync"})
			cancel()
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

	go func() {
		rand.Seed(time.Now().UnixNano())
		for {
			delay := time.Duration(10+rand.Intn(45)) * time.Second
			time.Sleep(delay)

			log.Printf("[Simulación] Nodo %s falló temporalmente (shutdown simulado)", nodo.nodeID)
			os.Exit(1)
		}
	}()

	listener, err := net.Listen("tcp", ":51000")
	if err != nil {
		log.Fatalf("Error al escuchar en el puerto: %v", err)
	}

	fmt.Printf("Nodo %s escuchando en el puerto 51000...\n", nodo.nodeID)
	if err := server.Serve(listener); err != nil {
		log.Fatalf("Error al iniciar el servidor: %v", err)
	}
}

