package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	proto "heint/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const MyNodeID = "datanode2:59000"
const DatanodePort = ":59000"
const ConsensusLeaderAddr = "consenso1:57770"
const CoordinatorAddr = "coordinador:54000"

type VectorClock map[string]int32

type FlightState struct {
	Status string
	Gate   string
	Clock  VectorClock
}

type DataNode struct {
	proto.UnimplementedDataNodeServiceServer
	id            string
	mu            sync.Mutex
	asientos      map[string]bool
	flightStatus  map[string]FlightState
	consensusAddr string
}

func mergeVectorClocks(vc1, vc2 VectorClock) VectorClock {
	merged := make(VectorClock)
	for k, v := range vc1 {
		merged[k] = v
	}
	for k, v := range vc2 {
		if val, ok := merged[k]; !ok || v > val {
			merged[k] = v
		}
	}
	return merged
}

func compareClocks(VC_in, VC_stored VectorClock) (bool, bool) {
	isGreater := false
	isLesser := false
	for k := range VC_in {
		if VC_in[k] > VC_stored[k] {
			isGreater = true
		} else if VC_in[k] < VC_stored[k] {
			isLesser = true
		}
	}
	for k := range VC_stored {
		if _, ok := VC_in[k]; !ok && VC_stored[k] > 0 {
			isLesser = true
		}
	}
	return isGreater, isLesser
}

func (d *DataNode) EscribirReserva(ctx context.Context, req *proto.AsientoSelect) (*proto.Response, error) {
	log.Printf("[%s] Intento de reserva para asiento %s", d.id, req.GetAsiento())
	d.mu.Lock()
	if ocupado, existe := d.asientos[req.GetAsiento()]; existe && !ocupado {
		d.mu.Unlock()
		return &proto.Response{Mensaje: "Asiento ya ocupado"}, nil
	}
	d.mu.Unlock()

	command := fmt.Sprintf("RESERVAR:%s", req.GetAsiento())

	conn, err := grpc.Dial(d.consensusAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return &proto.Response{Mensaje: "Error conectando a Consenso"}, nil
	}
	defer conn.Close()

	client := proto.NewConsensusClient(conn)
	ctxT, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.Propose(ctxT, &proto.Proposal{Command: command})
	if err != nil {
		return &proto.Response{Mensaje: "Error en Consenso"}, nil
	}

	if resp.GetResult() == "Not Leader" && resp.GetLeaderId() != "" {
		log.Println("Redirigiendo request al nuevo líder:", resp.GetLeaderId())
		return d.tryConsensus(req.GetAsiento(), resp.GetLeaderId()) // NUEVA FUNCIÓN
	}

	if !resp.GetSuccess() {
		return &proto.Response{Mensaje: "Consenso rechazó la operación"}, nil
	}

	d.mu.Lock()
	d.asientos[req.GetAsiento()] = false
	d.mu.Unlock()

	log.Printf("[%s] Reserva aplicada para %s", d.id, req.GetAsiento())
	return &proto.Response{Mensaje: "Reserva exitosa"}, nil
}

func (d *DataNode) tryConsensus(asiento string, leader string) (*proto.Response, error) {
	command := fmt.Sprintf("RESERVAR:%s", asiento)
	conn, err := grpc.Dial(leader, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return &proto.Response{Mensaje: "Error conectando al nuevo líder"}, nil
	}
	defer conn.Close()

	client := proto.NewConsensusClient(conn)
	ctxT, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err = client.Propose(ctxT, &proto.Proposal{Command: command})
	if err != nil {
		return &proto.Response{Mensaje: "Error en consenso"}, nil
	}

	d.mu.Lock()
	d.asientos[asiento] = false
	d.mu.Unlock()

	return &proto.Response{Mensaje: "Reserva exitosa"}, nil
}

func (d *DataNode) UpdateFlightState(ctx context.Context, req *proto.FlightUpdate) (*proto.Response, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	flightID := req.GetFlightId()
	VC_in := VectorClock(req.GetVectorClock().GetClocks())

	currentState, exists := d.flightStatus[flightID]
	VC_stored := currentState.Clock
	if !exists {
		VC_stored = make(VectorClock)
	}

	isGreater, isLesser := compareClocks(VC_in, VC_stored)
	apply := false

	if isGreater && !isLesser {
		apply = true
	} else if isGreater && isLesser {
		apply = true
	} else if !exists {
		apply = true
	}

	if apply {
		mergedVC := mergeVectorClocks(VC_in, VC_stored)
		d.flightStatus[flightID] = FlightState{
			Status: req.GetNewStatus(),
			Gate:   req.GetNewGate(),
			Clock:  mergedVC,
		}
		return &proto.Response{Mensaje: "Actualización aplicada"}, nil
	}

	return &proto.Response{Mensaje: "Actualización ignorada"}, nil
}

func (d *DataNode) ObtenerEstado(ctx context.Context, req *proto.SolicitudEstado) (*proto.ResponseEstadoAsignado, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	return &proto.ResponseEstadoAsignado{
		Datanode:  d.id,
		IdSistema: d.id,
		Asientos:  d.asientos,
	}, nil
}

func registerWithBroker(id string) {
	log.Printf("[%s] Intentando registrar con Broker...", id)
	for i := 0; i < 5; i++ {
		conn, err := grpc.Dial(CoordinatorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			client := proto.NewAsignadorServiceClient(conn)
			_, err := client.AsignarDataNode(context.Background(), &proto.RequestAsignar{Cliente: id})
			if err == nil {
				log.Printf("[%s] Registrado con Broker con éxito", id)
				conn.Close()
				return
			}
		}
		time.Sleep(2 * time.Second)
	}
	log.Printf("[%s] No se pudo registrar con Broker, intentando luego...", id)
}

func main() {
	lis, err := net.Listen("tcp", DatanodePort)
	if err != nil {
		log.Fatalf("Falló al escuchar puerto: %v", err)
	}

	server := grpc.NewServer()

	initialSeats := map[string]bool{
		"A1": true, "A2": true, "B1": true, "B2": true,
	}
	initialFlightState := map[string]FlightState{
		"LA-500": {Status: "Programado", Gate: "TBD", Clock: make(VectorClock)},
	}

	datanode := &DataNode{
		id:            MyNodeID,
		asientos:      initialSeats,
		flightStatus:  initialFlightState,
		consensusAddr: ConsensusLeaderAddr,
	}

	proto.RegisterDataNodeServiceServer(server, datanode)

	go registerWithBroker(MyNodeID)

	log.Printf("DATA NODE %s escuchando en %s", MyNodeID, DatanodePort)
	log.Println("Esperando peticiones gRPC...")

	if err := server.Serve(lis); err != nil {
		log.Fatalf("Falló servidor gRPC: %v", err)
	}
}
