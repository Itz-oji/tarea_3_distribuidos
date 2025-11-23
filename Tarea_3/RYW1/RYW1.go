package main

import (
	"context"
	"log"
	"strconv"
	"time"

	proto "heint/proto"

	"google.golang.org/grpc"
)

type Node struct {
	nodeID          string
	brokerAddr      string
	lastRequestID   string
	lastSeatChoosed string
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

func (n *Node) generarID() string {
	// Ejemplo: REQ-RYW1-172899213123123
	return "REQ-" + n.nodeID + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)
}

func (n *Node) ReservarAsiento(asiento string) bool {
	conn, err := grpc.Dial(n.brokerAddr, grpc.WithInsecure())
	if err != nil {
		log.Printf("[%s] Error al conectar con coordinador: %v", n.nodeID, err)
		return false
	}
	defer conn.Close()

	client := proto.NewBrokerServiceClient(conn)

	reqID := n.generarID()
	n.lastRequestID = reqID
	n.lastSeatChoosed = asiento

	// Enviar reserva
	log.Printf("[%s] Reservando asiento %s con ID %s...", n.nodeID, asiento, reqID)
	resp, err := client.SendReserva(context.Background(), &proto.AsientoSelect{
		AsientoID: reqID,
		Asiento:   asiento,
	})
	if err != nil {
		log.Printf("[%s] ERROR al reservar asiento %s: %v", n.nodeID, asiento, err)
		return false
	}

	log.Printf("[%s] Coordinador respondió: %s", n.nodeID, resp.Mensaje)
	return true
}

func (n *Node) ValidarRYW() bool {
	asientos, ok := n.EstadoSistema()
	if !ok {
		log.Printf("[%s] No se pudo obtener estado para validar RYW.", n.nodeID)
		return false
	}

	// Validar que el asiento reservado esté ahora en "ocupado"
	if libre := asientos[n.lastSeatChoosed]; libre {
		log.Printf("[%s] RYW VIOLADO! Asiento %s sigue libre después de escribir con ID %s",
			n.nodeID, n.lastSeatChoosed, n.lastRequestID)
		return false
	}

	log.Printf("[%s] ✔ READ-YOUR-WRITES VALIDADO! Asiento %s aparece ocupado tras la reserva.",
		n.nodeID, n.lastSeatChoosed)
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
		if !nodo.ReservarAsiento(asientoLibre) {
			time.Sleep(2 * time.Second)
			continue
		}

		// Paso 3 & 4: Confirmación inmediata + Validación RYW
		time.Sleep(2 * time.Second)
		nodo.ValidarRYW()

		time.Sleep(5 * time.Second)
	}
}
