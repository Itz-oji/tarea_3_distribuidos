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
	lastSeatChoosed string
	localClock      map[string]int32
}

// =================== UTILIDADES DE RELOJ ===================

func (n *Node) initClock() {
	n.localClock = make(map[string]int32)
	n.localClock[n.nodeID] = 0
}

func (n *Node) tickClock() {
	n.localClock[n.nodeID]++
}

func (n *Node) generarClockMsg() *proto.VectorClock {
	return &proto.VectorClock{Clocks: n.localClock}
}

// =================== CONSULTAR ESTADO ===================

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

	// =================== IMPRIMIR MATRIZ ===================
	log.Printf("\n[%s] Estado del sistema (L=Libre, X=Ocupado)\n", n.nodeID)

	// Encabezado con números 1–21
	header := "      "
	for i := 1; i <= 21; i++ {
		header += strconv.Itoa(i)
		if i < 10 {
			header += "   "
		} else {
			header += "  "
		}
	}
	log.Println(header)

	// Filas de A a F
	for _, row := range []string{"A", "B", "C", "D", "E", "F"} {
		linea := row + "  |  "
		for num := 1; num <= 21; num++ {
			seatID := row + strconv.Itoa(num)
			libre, exists := resp.AsientosDisponibles[seatID]
			if !exists || libre {
				linea += "L   "
			} else {
				linea += "X   "
			}
		}
		log.Println(linea)
	}
	log.Println("")

	return resp.AsientosDisponibles, true
}

// =================== RESERVAR ASIENTO ===================

func (n *Node) ReservarAsiento(asiento string) bool {
	conn, err := grpc.Dial(n.brokerAddr, grpc.WithInsecure())
	if err != nil {
		log.Printf("[%s] Error al conectar con coordinador: %v", n.nodeID, err)
		return false
	}
	defer conn.Close()

	client := proto.NewBrokerServiceClient(conn)

	// Incremento de reloj
	n.tickClock()

	log.Printf("[%s] Reservando asiento %s...", n.nodeID, asiento)
	resp, err := client.SendReserva(context.Background(), &proto.AsientoSelect{
		ClienteId:   n.nodeID,
		Asiento:     asiento,
		VectorClock: n.generarClockMsg(),
	})

	if err != nil {
		log.Printf("[%s] ERROR al reservar asiento %s: %v", n.nodeID, asiento, err)
		return false
	}

	log.Printf("[%s] Coordinador respondió: %s", n.nodeID, resp.Mensaje)
	n.lastSeatChoosed = asiento

	return resp.Ok
}

// =================== VALIDAR READ-YOUR-WRITES ===================

func (n *Node) ValidarRYW() bool {
	asientos, ok := n.EstadoSistema()
	if !ok {
		log.Printf("[%s] No se pudo obtener estado para validar RYW.", n.nodeID)
		return false
	}

	if libre := asientos[n.lastSeatChoosed]; libre {
		log.Printf("[%s] ❌ RYW VIOLADO! Asiento %s sigue libre después de escribir.\n",
			n.nodeID, n.lastSeatChoosed)
		return false
	}

	log.Printf("[%s] ✔ RYW VALIDADO! Asiento %s aparece ocupado tras escribir.\n",
		n.nodeID, n.lastSeatChoosed)
	return true
}

// =================== MAIN ===================

func main() {
	nodo := &Node{
		nodeID:     "RYW3",
		brokerAddr: "coordinador:54000",
	}
	nodo.initClock()

	for {
		asientos, ok := nodo.EstadoSistema()
		if !ok {
			time.Sleep(3 * time.Second)
			continue
		}

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

		if !nodo.ReservarAsiento(asientoLibre) {
			time.Sleep(2 * time.Second)
			continue
		}

		time.Sleep(1 * time.Second)
		nodo.ValidarRYW()

		time.Sleep(4 * time.Second)
	}
}
