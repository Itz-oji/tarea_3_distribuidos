package main

import (
	"context"
	"log"
	"time"

	proto "heint/proto"

	"google.golang.org/grpc"
)

type Node struct {
	nodeID     string
	brokerAddr string
}

func (n *Node) EstadoSistema() (map[string]bool, bool) {
	conn, err := grpc.Dial(n.brokerAddr, grpc.WithInsecure())
	if err != nil {
		log.Printf("[%s] Error al conectar con coordinador: %v", n.nodeID, err)
		return nil, false
	}
	defer conn.Close()

	client := proto.NewBrokerServiceClient(conn)

	resp, err := client.SendEstado(context.Background(), &proto.Response{Mensaje: n.nodeID})
	if err != nil {
		log.Printf("[%s] Error al consultar estado del sistema: %v", n.nodeID, err)
		return nil, false
	}

	log.Printf("[%s] Estado del sistema recibido:", n.nodeID)
	for asiento, disponible := range resp.AsientosDisponibles {
		estado := "Ocupado"
		if disponible {
			estado = "Libre"
		}
		log.Printf("  → %s: %s", asiento, estado)
	}

	return resp.AsientosDisponibles, true
}

func (n *Node) ReservarAsiento(asiento string) bool {
	conn, err := grpc.Dial(n.brokerAddr, grpc.WithInsecure())
	if err != nil {
		log.Printf("[%s] Error al conectar con coordinador: %v", n.nodeID, err)
		return false
	}
	defer conn.Close()

	client := proto.NewBrokerServiceClient(conn)

	// Enviar reserva
	log.Printf("[%s] Intentando reservar asiento %s...", n.nodeID, asiento)
	resp, err := client.SendReserva(context.Background(), &proto.AsientoSelect{
		AsientoID: "REQ-RYW1",
		Asiento:   asiento,
	})
	if err != nil {
		log.Printf("[%s] Error al reservar asiento %s: %v", n.nodeID, asiento, err)
		return false
	}

	log.Printf("[%s] Coordinador respondió reserva: %s", n.nodeID, resp.Mensaje)
	return true
}

func main() {
	nodo := &Node{
		nodeID:     "RYW1",
		brokerAddr: "coordinador:54000",
	}

	for {
		// Paso 1: Consultar estado
		asientos, ok := nodo.EstadoSistema()
		if !ok {
			time.Sleep(3 * time.Second)
			continue
		}

		// Paso 2: Elegir el primer asiento libre
		asientoLibre := ""
		for asiento, libre := range asientos {
			if libre {
				asientoLibre = asiento
				break
			}
		}

		if asientoLibre == "" {
			log.Printf("[%s] No quedan asientos disponibles, esperando...", nodo.nodeID)
			time.Sleep(5 * time.Second)
			continue
		}

		// Paso 3: Enviar reserva de asiento
		if nodo.ReservarAsiento(asientoLibre) {
			log.Printf("[%s] Reserva enviada. Consultando estado actualizado...", nodo.nodeID)
			time.Sleep(2 * time.Second)

			// Paso 4: Ver nuevo estado del sistema
			nodo.EstadoSistema()
		}

		time.Sleep(5 * time.Second)
	}
}
