package main

import (
	"context"
	"log"
	"net"
	"strconv"
	"sync"

	proto "heint/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	MyNodeID     = "datanode1:58000"
	DatanodePort = ":58000"
	BrokerAddr   = "broker:60000"
)

type VectorClock map[string]int32

// ------------------- ESTADO EVENTUAL -------------------

type SeatState struct {
	Taken    bool
	ClientID string
	Clock    VectorClock
}

type FlightState struct {
	Status    string
	Gate      string
	AirlineID string
	Clock     VectorClock
}

// ------------------- DATANODE -------------------

type DataNode struct {
	proto.UnimplementedDataNodeServiceServer
	id      string
	mu      sync.Mutex
	seats   map[string]SeatState
	flights map[string]FlightState // <--- soporte vuelos
	peers   []string
}

// ------------------- UTILIDADES VECTOR CLOCK -------------------

func mergeVC(a, b VectorClock) VectorClock {
	merged := make(VectorClock)
	for k, v := range a {
		merged[k] = v
	}
	for k, v := range b {
		if merged[k] < v {
			merged[k] = v
		}
	}
	return merged
}

// comparar relojes
func compareVC(a, b VectorClock) (greater, less bool) {
	keys := map[string]bool{}
	for k := range a {
		keys[k] = true
	}
	for k := range b {
		keys[k] = true
	}
	for k := range keys {
		if a[k] > b[k] {
			greater = true
		}
		if a[k] < b[k] {
			less = true
		}
	}
	return
}

// ----------------------- GENERAR MATRIZ A–F × 1–10 -----------------------

func generateSeatMatrix() map[string]SeatState {
	seats := make(map[string]SeatState)
	for _, row := range []string{"A", "B", "C", "D", "E", "F"} {
		for num := 1; num <= 10; num++ {
			seat := row + strconv.Itoa(num)
			seats[seat] = SeatState{
				Taken:    false,
				ClientID: "",
				Clock:    make(VectorClock),
			}
		}
	}
	return seats
}

// ----------------------- ASIENTOS EVENTUALES -----------------------

func (d *DataNode) EscribirReserva(ctx context.Context, req *proto.AsientoSelect) (*proto.Response, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	asiento := req.GetAsiento()
	client := req.GetClienteId()
	vcIn := VectorClock(req.GetVectorClock().GetClocks())

	current, exists := d.seats[asiento]
	vcStored := current.Clock
	if !exists {
		vcStored = make(VectorClock)
	}

	isGreater, isLesser := compareVC(vcIn, vcStored)
	apply := false

	switch {
	case !exists:
		apply = true
	case isGreater && !isLesser:
		apply = true
	case !isGreater && isLesser:
		apply = false
	default:
		if client > current.ClientID {
			apply = true
		}
	}

	if apply {
		merged := mergeVC(vcIn, vcStored)
		d.seats[asiento] = SeatState{
			Taken:    true,
			ClientID: client,
			Clock:    merged,
		}
		log.Printf("[%s] Reserva aplicada asiento %s por %s", d.id, asiento, client)
		return &proto.Response{Mensaje: "Reserva aplicada", Ok: true}, nil
	}

	log.Printf("[%s] Reserva ignorada asiento %s por %s", d.id, asiento, client)
	return &proto.Response{Mensaje: "Reserva ignorada", Ok: false}, nil
}

// ----------------------- OBTENER ESTADO (RWY + Coordinador) -----------------------

func (d *DataNode) ObtenerEstado(ctx context.Context, req *proto.SolicitudEstado) (*proto.ResponseEstadoAsignado, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	asientosMap := make(map[string]bool)
	for id, st := range d.seats {
		asientosMap[id] = !st.Taken
	}

	return &proto.ResponseEstadoAsignado{
		Datanode:  d.id,
		IdSistema: d.id,
		Asientos:  asientosMap,
	}, nil
}

// ----------------------- ACTUALIZACIÓN DE VUELOS -----------------------

func (d *DataNode) UpdateFlightState(ctx context.Context, req *proto.FlightUpdate) (*proto.Response, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	f, exists := d.flights[req.FlightId]
	vcIn := VectorClock(req.GetVectorClock().GetClocks())
	vcStored := f.Clock

	if !exists {
		vcStored = make(VectorClock)
	}

	isGreater, isLesser := compareVC(vcIn, vcStored)
	apply := false

	switch {
	case !exists:
		apply = true
	case isGreater && !isLesser:
		apply = true
	}

	if apply {
		merged := mergeVC(vcIn, vcStored)
		newState := f

		if req.NewStatus != "" {
			newState.Status = req.NewStatus
		}
		if req.NewGate != "" {
			newState.Gate = req.NewGate
		}
		newState.AirlineID = req.AirlineId
		newState.Clock = merged
		d.flights[req.FlightId] = newState

		log.Printf("[%s] ✈ Update vuelo %s => (%s, %s) VC=%v",
			d.id, req.FlightId, newState.Status, newState.Gate, merged)
	}

	return &proto.Response{Mensaje: "OK", Ok: true}, nil
}

// ----------------------- REPLICAS REMOTAS (aplicar asiento de otro nodo) -----------------------

func (d *DataNode) applyRemoteSeat(s *proto.SeatReplica) bool {
	asiento := s.GetAsiento()
	client := s.GetClienteId()
	vcIn := s.GetVectorClock().GetClocks()

	current, exists := d.seats[asiento]
	vcStored := current.Clock
	if !exists {
		vcStored = make(VectorClock)
	}

	isGreater, isLesser := compareVC(vcIn, vcStored)

	switch {
	case !exists, isGreater && !isLesser, (isGreater && isLesser && client > current.ClientID):
		d.seats[asiento] = SeatState{
			Taken:    s.GetTaken(),
			ClientID: client,
			Clock:    mergeVC(vcIn, vcStored),
		}
		return true
	}
	return false
}

// ----------------------- CONSULTA DE VUELO -----------------------

func (d *DataNode) ObtenerVuelo(ctx context.Context, req *proto.FlightRequest) (*proto.FlightResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	f, exists := d.flights[req.FlightId]
	if !exists {
		return &proto.FlightResponse{
			FlightId: req.FlightId,
			State:    "UNKNOWN",
			Version:  0,
		}, nil
	}

	var v int32
	for _, val := range f.Clock {
		if val > v {
			v = val
		}
	}

	return &proto.FlightResponse{
		FlightId: req.FlightId,
		State:    f.Status + "@" + f.Gate,
		Version:  int64(v),
	}, nil
}

// ----------------------- GOSSIP -----------------------

func (d *DataNode) Gossip(ctx context.Context, req *proto.GossipRequest) (*proto.GossipResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, s := range req.GetSeats() {
		_ = d.applyRemoteSeat(s)
	}
	return &proto.GossipResponse{}, nil
}

// ----------------------- REGISTRO BROKER -----------------------

func registerWithBroker(id string) {
	log.Printf("[%s] Registrando con Broker...", id)
	conn, err := grpc.Dial(BrokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err == nil {
		client := proto.NewAsignadorServiceClient(conn)
		client.AsignarDataNode(context.Background(), &proto.RequestAsignar{Cliente: id})
		conn.Close()
	}
}

// ----------------------- MAIN -----------------------

func main() {
	lis, _ := net.Listen("tcp", DatanodePort)
	server := grpc.NewServer()

	datanode := &DataNode{
		id:      MyNodeID,
		seats:   generateSeatMatrix(),
		flights: map[string]FlightState{},
		peers:   []string{"datanode1:58000", "datanode2:59000", "datanode3:57000"},
	}

	proto.RegisterDataNodeServiceServer(server, datanode)

	go registerWithBroker(MyNodeID)

	log.Println("Datanode listo:", MyNodeID)
	server.Serve(lis)
}
