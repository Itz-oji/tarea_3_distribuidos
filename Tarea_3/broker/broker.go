package main

import (
	"context"
	"encoding/csv"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	proto "heint/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ==================== BROKER CENTRAL ====================

type Broker struct {
	proto.UnimplementedAsignadorServiceServer

	mu        sync.Mutex
	datanodes []string
	rrIndex   int
}

// Servicio para Monotonic Reads
type InfoServer struct {
	broker *Broker
	proto.UnimplementedInfoServiceServer
}

// Evento programado desde el CSV
type ScheduledUpdate struct {
	TimeSec int // sim_time_sec
	Update  *proto.FlightUpdate
}

// ==================== ROUND ROBIN ====================

func (b *Broker) pickDataNodeRR() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.datanodes) == 0 {
		return ""
	}
	node := b.datanodes[b.rrIndex]
	b.rrIndex = (b.rrIndex + 1) % len(b.datanodes)
	return node
}

// ==================== RPC: ASIGNAR DATANODE ====================

func (b *Broker) AsignarDataNode(ctx context.Context, req *proto.RequestAsignar) (*proto.ResponseAsignar, error) {
	node := b.pickDataNodeRR()
	if node == "" {
		log.Printf("[BROKER] No hay datanodes registrados")
		return &proto.ResponseAsignar{Datanode: ""}, nil
	}
	log.Printf("[BROKER] Cliente %s asignado al nodo %s", req.Cliente, node)
	return &proto.ResponseAsignar{Datanode: node}, nil
}

// ==================== RPC: OBTENER ESTADO (para Coordinador / RYW) ====================

func (b *Broker) ObtenerEstadoYAsignacion(ctx context.Context, req *proto.SolicitudEstado) (*proto.ResponseEstadoAsignado, error) {
	node := b.pickDataNodeRR()
	if node == "" {
		log.Printf("[BROKER] Sin nodos disponibles para obtener estado")
		return &proto.ResponseEstadoAsignado{}, nil
	}

	conn, err := grpc.Dial(node, grpc.WithInsecure())
	if err != nil {
		log.Printf("[BROKER] Error conectando a %s: %v", node, err)
		return nil, err
	}
	defer conn.Close()

	client := proto.NewDataNodeServiceClient(conn)
	resp, err := client.ObtenerEstado(context.Background(), req)
	if err != nil {
		log.Printf("[BROKER] Error obteniendo estado de %s: %v", node, err)
		return nil, err
	}
	return resp, nil
}

// ==================== CSV LECTOR ====================

func readCSV() []ScheduledUpdate {
	file, err := os.Open("/flight_updates.csv")
	if err != nil {
		log.Printf("[BROKER] No se pudo abrir /flight_updates.csv: %v", err)
		return nil
	}
	defer file.Close()

	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()
	if err != nil {
		log.Printf("[BROKER] Error leyendo flight_updates.csv: %v", err)
		return nil
	}

	if len(rows) <= 1 {
		log.Printf("[BROKER] CSV vacío o sin datos")
		return nil
	}

	log.Printf("[BROKER] CSV cargado con %d eventos de vuelo", len(rows)-1)

	var schedule []ScheduledUpdate

	for i := 1; i < len(rows); i++ { // saltar encabezado
		row := rows[i]
		if len(row) < 4 {
			continue
		}

		secStr := strings.TrimSpace(row[0])
		flightID := strings.TrimSpace(row[1])
		updateType := strings.ToLower(strings.TrimSpace(row[2]))
		updateValue := strings.TrimSpace(row[3])

		if flightID == "" || secStr == "" {
			continue
		}

		sec, err := strconv.Atoi(secStr)
		if err != nil || sec < 0 {
			continue
		}

		if len(flightID) < 2 {
			continue
		}
		airline := flightID[:2] // deducimos aerolínea del prefijo

		update := &proto.FlightUpdate{
			FlightId:  flightID,
			AirlineId: airline,
			VectorClock: &proto.VectorClock{
				Clocks: map[string]int32{
					airline: int32(sec), // usamos sim_time_sec como versión lógica
				},
			},
		}

		switch updateType {
		case "estado", "status":
			update.NewStatus = updateValue
		case "puerta", "gate":
			update.NewGate = updateValue
		default:
			// Tipo de update que no nos interesa
			continue
		}

		schedule = append(schedule, ScheduledUpdate{
			TimeSec: sec,
			Update:  update,
		})
	}

	log.Printf("[BROKER] Preparados %d updates para envío", len(schedule))
	return schedule
}

// ==================== BROADCAST A TODOS LOS DATANODES ====================

func (b *Broker) broadcastFlightUpdate(u *proto.FlightUpdate) {
	for _, node := range b.datanodes {
		go func(nd string) {
			conn, err := grpc.Dial(nd, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("[BROKER] Error conectando a datanode %s: %v", nd, err)
				return
			}
			defer conn.Close()

			client := proto.NewDataNodeServiceClient(conn)
			_, err = client.UpdateFlightState(context.Background(), u)
			if err != nil {
				log.Printf("[BROKER] Error enviando update a %s: %v", nd, err)
			}
		}(node)
	}
}

// ==================== SIMULACIÓN DE ACTUALIZACIONES DESDE CSV ====================
//
// Reloj interno: cada segundo currentSec++
// Si existe algún evento con TimeSec == currentSec → se hace broadcast

func (b *Broker) startFlightUpdates() {
	events := readCSV()
	if len(events) == 0 {
		log.Printf("[BROKER] No hay eventos de vuelo para simular")
		return
	}

	log.Printf("[BROKER] Iniciando simulación de vuelos con %d eventos", len(events))

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	currentSec := 0
	idx := 0

	for range ticker.C {
		currentSec++

		// Disparar todos los eventos cuyo TimeSec == currentSec
		for idx < len(events) && events[idx].TimeSec == currentSec {
			ev := events[idx]
			tipo := "desconocido"
			valor := ""
			if ev.Update.NewStatus != "" {
				tipo = "estado"
				valor = ev.Update.NewStatus
			} else if ev.Update.NewGate != "" {
				tipo = "puerta"
				valor = ev.Update.NewGate
			}

			log.Printf("[BROKER] t=%d → update vuelo %s (%s=%s) VC=%v",
				currentSec,
				ev.Update.FlightId,
				tipo,
				valor,
				ev.Update.VectorClock.Clocks,
			)

			b.broadcastFlightUpdate(ev.Update)
			idx++
		}

		if idx >= len(events) {
			log.Printf("[BROKER] Todos los eventos del CSV fueron procesados. Fin de la simulación.")
			return
		}
	}
}

// ==================== InfoService: Monotonic Reads ====================

func (s *InfoServer) GetFlightStatus(ctx context.Context, req *proto.FlightRequest) (*proto.FlightResponse, error) {
	// Elegimos un datanode vía round-robin
	node := s.broker.pickDataNodeRR()
	if node == "" {
		return &proto.FlightResponse{
			FlightId: req.FlightId,
			State:    "UNKNOWN",
			Version:  req.LastVersion,
		}, nil
	}

	conn, err := grpc.Dial(node, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	dn := proto.NewDataNodeServiceClient(conn)

	// Pedimos el estado del vuelo al datanode
	resp, err := dn.ObtenerVuelo(ctx, &proto.FlightRequest{FlightId: req.FlightId})
	if err != nil {
		log.Printf("[BROKER] Error obteniendo vuelo %s en %s: %v", req.FlightId, node, err)
		return &proto.FlightResponse{
			FlightId: req.FlightId,
			State:    "UNKNOWN",
			Version:  req.LastVersion,
		}, nil
	}

	// Garantizar Monotonic Reads: no devolver versión menor
	if resp.Version < req.LastVersion {
		log.Printf("[BROKER] MR: evitando retroceso para %s (last=%d, resp=%d)",
			req.FlightId, req.LastVersion, resp.Version)

		return &proto.FlightResponse{
			FlightId: resp.FlightId,
			Version:  req.LastVersion,
			State:    resp.State,
		}, nil
	}

	// 👌 Versión válida (≥ last_version del cliente)
	return resp, nil
}

// ==================== MAIN ====================

func main() {
	lis, err := net.Listen("tcp", ":60000")
	if err != nil {
		log.Fatalf("[BROKER] Error al abrir puerto: %v", err)
	}

	broker := &Broker{
		datanodes: []string{
			"datanode1:58000",
			"datanode2:59000",
			"datanode3:57000",
		},
		rrIndex: 0,
	}

	server := grpc.NewServer()

	// Broker "clásico"
	proto.RegisterAsignadorServiceServer(server, broker)

	// InfoService para Monotonic Reads
	infoSrv := &InfoServer{broker: broker}
	proto.RegisterInfoServiceServer(server, infoSrv)

	// Simulación de updates desde CSV
	go broker.startFlightUpdates()

	log.Println("[BROKER] Servidor iniciado en puerto :60000")
	if err := server.Serve(lis); err != nil {
		log.Fatalf("[BROKER] Error en gRPC: %v", err)
	}
}
