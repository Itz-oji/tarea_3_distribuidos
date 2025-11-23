package main

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	flightpb "heint/proto"
)

// MRClient representa un cliente que consulta el estado de vuelos al broker.
type MRClient struct {
	id       string                     // identificador del cliente
	client   flightpb.InfoServiceClient // cliente gRPC al broker
	flights  []string                   // lista de vuelos a consultar
	versions map[string]int64           // versiones conocidas de cada vuelo
	interval time.Duration              // intervalo entre consultas
}

// NewMRClient crea un nuevo cliente MR.
func NewMRClient(id string, cli flightpb.InfoServiceClient, flights []string, interval time.Duration) *MRClient {
	versions := make(map[string]int64) // mapa de versiones por vuelo

	// Inicializar versiones en 0
	for _, f := range flights {
		versions[f] = 0
	}

	// Retornar nuevo cliente
	return &MRClient{
		id:       id,
		client:   cli,
		flights:  flights,
		versions: versions,
		interval: interval,
	}
}

// run inicia el ciclo de consultas periódicas al broker.
func (m *MRClient) run() {
	ticker := time.NewTicker(m.interval) // ticker para intervalos
	defer ticker.Stop()                  // asegurar detener ticker al final

	// Bucle infinito de consultas
	for {

		// Mezclar orden de vuelos para evitar patrones
		rand.Shuffle(len(m.flights), func(i, j int) {
			m.flights[i], m.flights[j] = m.flights[j], m.flights[i]
		})

		// Consultar estado de cada vuelo
		for _, flight := range m.flights {
			lastVer := m.versions[flight]                                           // última versión conocida
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second) // timeout
			req := &flightpb.FlightRequest{FlightId: flight, LastVersion: lastVer}  // crear solicitud
			resp, err := m.client.GetFlightStatus(ctx, req)                         // hacer llamada RPC
			cancel()                                                                // cancelar contexto

			// Manejar errores de consulta
			if err != nil {
				log.Printf("%s: error querying %s: %v", m.id, flight, err)
				continue
			}

			// Procesar respuesta si hay nueva versión
			if resp.Version >= lastVer {

				// Actualizar estado si es nueva versión
				if resp.Version > lastVer {
					m.versions[flight] = resp.Version
					log.Printf("%s: flight %s updated: state=%s version=%d", m.id, flight, resp.State, resp.Version)
					// Versión igual, solo informar
				} else {
					log.Printf("%s: flight %s seen: state=%s version=%d", m.id, flight, resp.State, resp.Version)
				}
				// Ignorar versiones antiguas
			} else {
				log.Printf("%s: ignoring stale version for %s: got %d < known %d", m.id, flight, resp.Version, lastVer)
			}
		}
		<-ticker.C // esperar siguiente tick
	}
}

// inicia el cliente MR.
func main() {

	// Parsear flags de línea de comando
	id := flag.String("id", "mr1", "identifier for this MR client")
	brokerAddr := flag.String("broker", "localhost:6000", "broker gRPC endpoint (host:port)")
	flightsFlag := flag.String("flights", "IB6833", "comma‑separated list of flight codes to observe")
	intervalSec := flag.Int("interval", 5, "interval between successive queries (in seconds)")
	flag.Parse()

	flightList := make([]string, 0)

	// Construir lista de vuelos desde flag
	for _, f := range strings.Split(*flightsFlag, ",") {
		f = strings.TrimSpace(f)
		if f != "" {
			flightList = append(flightList, f)
		}
	}

	// Validar que haya vuelos especificados
	if len(flightList) == 0 {
		log.Fatal("no flights specified")
	}

	// Conectarse al broker
	conn, err := grpc.Dial(*brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to broker %s: %v", *brokerAddr, err)
	}
	defer conn.Close()

	// Crear cliente MR y correrlo
	cli := flightpb.NewInfoServiceClient(conn)
	client := NewMRClient(*id, cli, flightList, time.Duration(*intervalSec)*time.Second)
	log.Printf("MR client %s started, watching flights: %s", client.id, strings.Join(flightList, ", "))
	client.run()
}
