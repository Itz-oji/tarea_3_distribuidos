package main

import (
	"context"
	"encoding/csv"
	"fmt"
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

// Broker que asigna datanodes y maneja requests
type Broker struct {
	proto.UnimplementedAsignadorServiceServer

	mu        sync.Mutex // protege rrIndex
	datanodes []string   // direcciones de datanodes
	rrIndex   int        // índice para round-robin

	runways   []string          // nombres de las pistas disponibles
	assigned  map[string]string // vuelo → pista asignada
	free      map[string]bool   // pista → true si está libre
	runwaysMu sync.Mutex        // protege assigned/free

	consensus []string

	reportMu sync.Mutex
}

// Servicio para Monotonic Reads
type InfoServer struct {
	broker                               *Broker // referencia al broker
	proto.UnimplementedInfoServiceServer         // compatibilidad gRPC
}

// Evento programado desde el CSV
type ScheduledUpdate struct {
	TimeSec int                 // segundo de simulación
	Update  *proto.FlightUpdate // update a enviar
}

// ==================== ROUND ROBIN ====================

// Selecciona un datanode usando round-robin
func (b *Broker) pickDataNodeRR() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	// No hay datanodes registrados
	if len(b.datanodes) == 0 {
		return ""
	}

	node := b.datanodes[b.rrIndex]                 // seleccionar nodo actual
	b.rrIndex = (b.rrIndex + 1) % len(b.datanodes) // actualizar índice
	return node
}

// ==================== RPC: ASIGNAR DATANODE ====================

// Asigna un datanode a un cliente
func (b *Broker) AsignarDataNode(ctx context.Context, req *proto.RequestAsignar) (*proto.ResponseAsignar, error) {
	node := b.pickDataNodeRR() // elegir datanode

	// No hay datanodes disponibles
	if node == "" {
		log.Printf("[BROKER] No hay datanodes registrados")
		return &proto.ResponseAsignar{Datanode: ""}, nil // respuesta vacía
	}

	log.Printf("[BROKER] Cliente %s asignado al nodo %s", req.Cliente, node)
	return &proto.ResponseAsignar{Datanode: node}, nil // respuesta con datanode
}

// ==================== RPC: OBTENER ESTADO (para Coordinador / RYW) ====================

// Obtiene el estado de un vuelo desde un datanode
func (b *Broker) ObtenerEstadoYAsignacion(ctx context.Context, req *proto.SolicitudEstado) (*proto.ResponseEstadoAsignado, error) {
	node := b.pickDataNodeRR() // elegir datanode

	// No hay datanodes disponibles
	if node == "" {
		log.Printf("[BROKER] Sin nodos disponibles para obtener estado")
		return &proto.ResponseEstadoAsignado{}, nil
	}

	// Conectar al datanode seleccionado
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

// Lee el CSV de actualizaciones de vuelo y devuelve una lista de eventos programados
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

	var schedule []ScheduledUpdate // lista de eventos programados

	for i := 1; i < len(rows); i++ { // saltar encabezado
		row := rows[i]

		// Validar fila
		if len(row) < 4 {
			continue
		}

		secStr := strings.TrimSpace(row[0])                      // tiempo en segundos
		flightID := strings.TrimSpace(row[1])                    // ID del vuelo
		updateType := strings.ToLower(strings.TrimSpace(row[2])) // tipo de actualización
		updateValue := strings.TrimSpace(row[3])                 // valor de la actualización

		// Validaciones básicas
		if flightID == "" || secStr == "" {
			continue
		}

		// Convertir tiempo a entero
		sec, err := strconv.Atoi(secStr)
		if err != nil || sec < 0 {
			continue
		}

		// Validar longitud del ID de vuelo
		if len(flightID) < 2 {
			continue
		}
		airline := flightID[:2] // deducimos aerolínea del prefijo

		// Crear el FlightUpdate correspondiente
		update := &proto.FlightUpdate{
			FlightId:  flightID,
			AirlineId: airline,
			VectorClock: &proto.VectorClock{
				Clocks: map[string]int32{
					airline: int32(sec), // usamos sim_time_sec como versión lógica
				},
			},
		}

		// Asignar el tipo de actualización
		switch updateType {
		// En caso de estado o puerta
		case "estado", "status":
			update.NewStatus = updateValue
		// En caso de puerta
		case "puerta", "gate":
			update.NewGate = updateValue
		default:
			// Tipo de update que no nos interesa
			continue
		}

		// Agregar evento a la lista de programación
		schedule = append(schedule, ScheduledUpdate{
			TimeSec: sec,
			Update:  update,
		})
	}

	log.Printf("[BROKER] Preparados %d updates para envío", len(schedule))
	return schedule
}

// ==================== BROADCAST A TODOS LOS DATANODES ====================

// Envía un FlightUpdate a todos los datanodes registrados
func (b *Broker) broadcastFlightUpdate(u *proto.FlightUpdate) {
	// Enviar update a todos los datanodes
	for _, node := range b.datanodes {
		// Enviar de forma concurrente
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

// ==================== CONSENSO PARA ASIGNACIONES ====================

func (b *Broker) proposeCommand(cmd string) bool {
	for _, addr := range b.consensus {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("[BROKER] Error conectando a nodo de consenso %s: %v", addr, err)
			continue
		}
		client := proto.NewConsensusClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		resp, err := client.Propose(ctx, &proto.Proposal{Command: cmd})
		cancel()
		conn.Close()
		if err != nil {
			log.Printf("[BROKER] Error proponiendo en %s: %v", addr, err)
			continue
		}
		// Si el líder aceptó y replicó la propuesta, devolvemos éxito
		if resp.GetSuccess() {
			return true
		}
		// Si la propuesta no fue aceptada, probamos con el siguiente nodo
		log.Printf("[BROKER] Propuesta '%s' rechazada por %s: %s", cmd, addr, resp.GetResult())
	}
	log.Printf("[BROKER] No se logró consenso para el comando: %s", cmd)
	return false
}

func (b *Broker) assignRunway(flightID string) {
	// Verificar si ya tiene una pista asignada
	b.runwaysMu.Lock()
	if _, exists := b.assigned[flightID]; exists {
		b.runwaysMu.Unlock()
		return
	}
	// Elegir una pista libre
	var runway string
	for _, r := range b.runways {
		if free, ok := b.free[r]; ok && free {
			runway = r
			break
		}
	}
	b.runwaysMu.Unlock()
	if runway == "" {
		log.Printf("[BROKER] No hay pistas disponibles para el vuelo %s", flightID)
		return
	}
	// Proponer asignación vía consenso
	cmd := fmt.Sprintf("assign_runway:%s:%s", runway, flightID)
	if b.proposeCommand(cmd) {
		// Actualizar estado local
		b.runwaysMu.Lock()
		b.assigned[flightID] = runway
		b.free[runway] = false
		b.runwaysMu.Unlock()

		// Registrar en reporte
		b.appendReport(flightID, runway)
		log.Printf("[BROKER] Pista %s asignada a vuelo %s", runway, flightID)
	} else {
		log.Printf("[BROKER] Falló la asignación consensuada de pista %s para vuelo %s", runway, flightID)
	}
}

func (b *Broker) releaseRunway(flightID string) {
	b.runwaysMu.Lock()
	runway, ok := b.assigned[flightID]
	if !ok {
		b.runwaysMu.Unlock()
		return
	}
	// Marcar pista como libre y eliminar asignación
	delete(b.assigned, flightID)
	b.free[runway] = true
	b.runwaysMu.Unlock()
	// Proponer liberación vía consenso
	cmd := fmt.Sprintf("release_runway:%s:%s", runway, flightID)
	if b.proposeCommand(cmd) {
		log.Printf("[BROKER] Pista %s liberada por vuelo %s", runway, flightID)
	} else {
		log.Printf("[BROKER] Falló la liberación consensuada de pista %s para vuelo %s", runway, flightID)
	}
}

// appendReport agrega una línea al archivo report.txt con la
// asignación de pista realizada. El formato es "flightID,runway,timestamp".
func (b *Broker) appendReport(flightID, runway string) {
	b.reportMu.Lock()
	defer b.reportMu.Unlock()
	f, err := os.OpenFile("/report.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("[BROKER] Error abriendo/creando report.txt: %v", err)
		return
	}
	defer f.Close()
	timestamp := time.Now().Format(time.RFC3339)
	line := fmt.Sprintf("%s,%s,%s\n", flightID, runway, timestamp)
	if _, err := f.WriteString(line); err != nil {
		log.Printf("[BROKER] Error escribiendo en report.txt: %v", err)
	}
}

// ==================== SIMULACIÓN DE ACTUALIZACIONES DESDE CSV ====================
// Reloj interno: cada segundo currentSec++
// Si existe algún evento con TimeSec == currentSec → se hace broadcast

// De ese update a todos los datanodes
func (b *Broker) startFlightUpdates() {
	events := readCSV()

	// No hay eventos para simular
	if len(events) == 0 {
		log.Printf("[BROKER] No hay eventos de vuelo para simular")
		return
	}

	log.Printf("[BROKER] Iniciando simulación de vuelos con %d eventos", len(events))

	ticker := time.NewTicker(1 * time.Second) // reloj de simulación
	defer ticker.Stop()

	currentSec := 0
	idx := 0

	// Reloj interno: cada segundo currentSec++
	for range ticker.C {
		currentSec++

		// Disparar todos los eventos cuyo TimeSec == currentSec
		for idx < len(events) && events[idx].TimeSec == currentSec {
			ev := events[idx]
			tipo := "desconocido"
			valor := ""

			// Determinar tipo y valor del update
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

			// Según el estado del vuelo, decidir si asignar o liberar una pista.  La
			// asignación se solicita cuando el vuelo está en "En vuelo" (aterrizaje)
			// o "Embarcando" (despegue).  La liberación ocurre cuando el vuelo
			// llega a destino o se cancela.  Se ignoran otros estados.
			status := ev.Update.NewStatus
			if status != "" {
				// Comparaciones insensibles a mayúsculas
				if strings.EqualFold(status, "En vuelo") || strings.EqualFold(status, "Embarcando") {
					// Lanzar asignación en goroutine para no bloquear el bucle
					go b.assignRunway(ev.Update.FlightId)
				} else if strings.EqualFold(status, "Llegó") || strings.EqualFold(status, "Cancelado") {
					// Liberar la pista si aplica
					go b.releaseRunway(ev.Update.FlightId)
				}
			}

			idx++
		}

		// Si ya procesamos todos los eventos, terminamos la simulación
		if idx >= len(events) {
			log.Printf("[BROKER] Todos los eventos del CSV fueron procesados. Fin de la simulación.")
			return
		}
	}
}

// ==================== InfoService: Monotonic Reads ====================

// Maneja la solicitud GetFlightStatus de un cliente MR
func (s *InfoServer) GetFlightStatus(ctx context.Context, req *proto.FlightRequest) (*proto.FlightResponse, error) {
	// Elegimos un datanode vía round-robin
	node := s.broker.pickDataNodeRR()

	// No hay datanodes disponibles
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

	// Versión válida
	return resp, nil
}

// ==================== MAIN ====================

func main() {
	lis, err := net.Listen("tcp", ":60000")
	if err != nil {
		log.Fatalf("[BROKER] Error al abrir puerto: %v", err)
	}

	// Inicializar broker con datanodes conocidos
	// Inicializar broker con datanodes conocidos, pistas y nodos de consenso.
	broker := &Broker{
		datanodes: []string{
			"datanode1:58000",
			"datanode2:59000",
			"datanode3:57000",
		},
		rrIndex: 0,
		// Configurar pistas disponibles.  Estas son los recursos
		// críticos cuya asignación se decide mediante consenso.  Si
		// deseas más o menos pistas puedes modificar este slice.
		runways:  []string{"Pista01", "Pista02", "Pista03"},
		assigned: make(map[string]string),
		free: map[string]bool{
			"Pista01": true,
			"Pista02": true,
			"Pista03": true,
		},
		consensus: []string{
			"consenso1:57770",
			"consenso2:57771",
			"consenso3:57772",
		},
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
