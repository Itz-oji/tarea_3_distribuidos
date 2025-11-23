package main

import (
	"context"
	"log"
	"net"

	proto "heint/proto"

	"google.golang.org/grpc"
)

type DataNode struct {
	proto.UnimplementedDataNodeServiceServer

	id       string
	asientos map[string]bool
}

// =================== RPC: Obtener Estado =====================

func (d *DataNode) ObtenerEstado(ctx context.Context, req *proto.SolicitudEstado) (*proto.ResponseEstadoAsignado, error) {
	log.Printf("[%s] Estado solicitado por %s", d.id, req.Cliente)

	return &proto.ResponseEstadoAsignado{
		Datanode:  d.id,
		IdSistema: d.id,
		Asientos:  d.asientos,
	}, nil
}

// ==================== RPC: Reservar Asiento ==================

func (d *DataNode) EscribirReserva(ctx context.Context, req *proto.AsientoSelect) (*proto.Response, error) {
	log.Printf("[%s] Reserva solicitada por %s", d.id, req.AsientoID)

	_, existe := d.asientos[req.Asiento]
	if !existe {
		return &proto.Response{Mensaje: "Asiento no existe"}, nil
	}

	if !d.asientos[req.Asiento] {
		return &proto.Response{Mensaje: "Asiento ocupado"}, nil
	}

	// Marcar asiento como ocupado
	d.asientos[req.Asiento] = false

	log.Printf("[%s] Asiento %s reservado exitosamente", d.id, req.Asiento)
	return &proto.Response{Mensaje: "Reserva exitosa"}, nil
}

// ======================= Registrarse en el Broker ========================

// opcional: notificar al broker
func registerWithBroker(id string, addr string) {
	conn, err := grpc.Dial("broker:60000", grpc.WithInsecure())
	if err != nil {
		log.Printf("[%s] No se pudo conectar al broker: %v", id, err)
		return
	}
	defer conn.Close()

	client := proto.NewAsignadorServiceClient(conn)

	// Notificar simplemente pidiendo asignación dummy
	_, err = client.AsignarDataNode(context.Background(), &proto.RequestAsignar{Cliente: id})
	if err != nil {
		log.Printf("[%s] Error notificando al broker: %v", id, err)
		return
	}

	log.Printf("[%s] Notificado al broker en arranque", id)
}

// ================================ MAIN ================================

func main() {
	id := "datanode1:58000"

	lis, err := net.Listen("tcp", ":58000")
	if err != nil {
		log.Fatalf("[%s] Error abriendo puerto: %v", id, err)
	}

	server := grpc.NewServer()

	datanode := &DataNode{
		id: id,
		asientos: map[string]bool{
			"A1": true,
			"A2": true,
			"A3": true,
			"B1": true,
			"B2": true,
			"C1": true,
		},
	}

	proto.RegisterDataNodeServiceServer(server, datanode)

	// registrar con el broker (no indispensable)
	go registerWithBroker(id, ":58000")

	log.Printf("[%s] DataNode iniciado en :58000", id)

	if err := server.Serve(lis); err != nil {
		log.Fatalf("[%s] Error en gRPC: %v", id, err)
	}
}
