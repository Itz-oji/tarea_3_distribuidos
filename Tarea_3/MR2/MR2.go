package main

import (
	"context"
	"log"
	"time"

	proto "heint/proto"

	"google.golang.org/grpc"
)

type MRClient struct {
	id          string
	brokerAddr  string
	lastVersion map[string]int64 // Versiones recordadas por vuelo
}

// =============== Inicialización =====================

func NewMRClient(id, broker string) *MRClient {
	return &MRClient{
		id:          id,
		brokerAddr:  broker,
		lastVersion: make(map[string]int64),
	}
}

// =============== Consultar vuelo =====================

func (c *MRClient) ConsultarVuelo(flightID string) bool {
	conn, err := grpc.Dial(c.brokerAddr, grpc.WithInsecure())
	if err != nil {
		log.Printf("[%s] Error al conectar con broker: %v", c.id, err)
		return false
	}
	defer conn.Close()

	client := proto.NewInfoServiceClient(conn)

	// Última versión conocida (si no existe, será 0)
	vLocal := c.lastVersion[flightID]

	resp, err := client.GetFlightStatus(context.Background(),
		&proto.FlightRequest{
			FlightId:    flightID,
			LastVersion: vLocal, // 👈 Enviamos la versión esperada
		})

	if err != nil {
		log.Printf("[%s] Error consultando estado del vuelo %s: %v", c.id, flightID, err)
		return false
	}

	// Validación Monotónica
	if resp.Version < vLocal {
		log.Printf("[%s] VIOLACIÓN MR! Se recibió versión %d < %d",
			c.id, resp.Version, vLocal)
		return false
	}

	// Actualizamos versión si es mayor
	if resp.Version > vLocal {
		c.lastVersion[flightID] = resp.Version
	}

	log.Printf("[%s] Vuelo %s → Estado: %s (versión=%d)",
		c.id, resp.FlightId, resp.State, resp.Version)

	return true
}

// ================= MAIN ==============================

func main() {
	clienteMR := NewMRClient("MR2", "broker:60000")

	for {
		clienteMR.ConsultarVuelo("AF-021")
		time.Sleep(3 * time.Second)
	}
}
