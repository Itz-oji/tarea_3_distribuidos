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

type Node struct {
	nodeID string
	brokerAddr string
}

func(n *Node) EstadoSistema() {
	conn, err := grpc.Dial(n.brokerAddr, grpc.WithInsecure())
	if err != nil {
		log.Printf("[%s] Error al conectar con coordinador: %v", n.nodeID, err)
		return
	}

	defer conn.Close()

	client := proto.NewBrokerServiceClient(conn)

	resp, err := client.SendEstado(context.Background(), &proto.Response{Mensaje: "RYW2"})
	if err != nil {
		log.Printf("[%s] Error al consultar estado del sistema: %v", n.nodeID, err)
		return
	}

	log.Printf("[%s] Estado del sistema recibido (ID=%d):", n.nodeID, resp.Id)
	for asiento, disponible := range resp.AsientosDisponibles {
		estado := "Ocupado"
		if disponible {
			estado = "Libre"
		}
		log.Printf("  → %s: %s", asiento, estado)
	}
}

func main() {
	nodo := &Node{
		nodeID: "RYW2",
		brokerAddr: "localhost:54000",
	}

	for {
		nodo.EstadoSistema()
		time.Sleep(5 * time.Second)
	}
}

