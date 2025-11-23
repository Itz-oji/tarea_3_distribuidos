package main

import (
	"context"
	"log"
	"time"

	proto "heint/proto"

	"google.golang.org/grpc"
)

// Node representa un nodo RYW2
type Node struct {
	nodeID     string // ID del nodo
	brokerAddr string // Dirección del broker
}

// EstadoSistema consulta el estado del sistema al broker
func (n *Node) EstadoSistema() {
	// Conectar con el broker
	conn, err := grpc.Dial(n.brokerAddr, grpc.WithInsecure())

	// Manejo de errores
	if err != nil {
		log.Printf("[%s] Error al conectar con coordinador: %v", n.nodeID, err)
		return
	}

	defer conn.Close() // Cerrar la conexión al finalizar

	client := proto.NewBrokerServiceClient(conn) // Crear cliente gRPC

	resp, err := client.SendEstado(context.Background(), &proto.Response{Mensaje: "RYW2"}) // Enviar solicitud de estado

	// Manejo de errores
	if err != nil {
		log.Printf("[%s] Error al consultar estado del sistema: %v", n.nodeID, err)
		return
	}

	log.Printf("[%s] Estado del sistema recibido (ID=%d):", n.nodeID, resp.Id) // Imprimir estado recibido

	// Mostrar estado de los asientos
	for asiento, disponible := range resp.AsientosDisponibles {
		estado := "Ocupado"

		// Determinar estado del asiento
		if disponible {
			estado = "Libre"
		}
		log.Printf("  → %s: %s", asiento, estado)
	}
}

// Función principal
func main() {
	// Crear nodo RYW2
	nodo := &Node{
		nodeID:     "RYW2",
		brokerAddr: "localhost:54000",
	}

	// Bucle infinito para consultar estado periódicamente
	for {
		nodo.EstadoSistema()
		time.Sleep(5 * time.Second)
	}
}
