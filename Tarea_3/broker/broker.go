package main

import (
	"context"
	"log"
	"net"
	"sync"

	proto "heint/proto"

	"google.golang.org/grpc"
)

type Broker struct {
	proto.UnimplementedAsignadorServiceServer

	mu        sync.Mutex
	datanodes []string // lista de datanodes
	rrIndex   int      // índice para Round Robin
}

// ================= Round Robin ==================

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

// ================= RPC: asignar datanode ==================

func (b *Broker) AsignarDataNode(ctx context.Context, req *proto.RequestAsignar) (*proto.ResponseAsignar, error) {
	node := b.pickDataNodeRR()
	if node == "" {
		log.Printf("[BROKER] No hay datanodes registrados")
		return &proto.ResponseAsignar{Datanode: ""}, nil
	}

	log.Printf("[BROKER] Asignado %s → %s", req.Cliente, node)
	return &proto.ResponseAsignar{Datanode: node}, nil
}

// ================= RPC: obtener estado real ==================

func (b *Broker) ObtenerEstadoYAsignacion(ctx context.Context, req *proto.SolicitudEstado) (*proto.ResponseEstadoAsignado, error) {
	// Elegir datanode por R.R.
	node := b.pickDataNodeRR()
	if node == "" {
		log.Printf("[BROKER] Sin datanodes para preguntar estado")
		return &proto.ResponseEstadoAsignado{}, nil
	}

	// Conectarse al datanode seleccionado
	conn, err := grpc.Dial(node, grpc.WithInsecure())
	if err != nil {
		log.Printf("[BROKER] Error conectando a %s: %v", node, err)
		return nil, err
	}
	defer conn.Close()

	dataNodeClient := proto.NewDataNodeServiceClient(conn)

	// Pedir estado al datanode
	resp, err := dataNodeClient.ObtenerEstado(context.Background(), req)
	if err != nil {
		log.Printf("[BROKER] Error obteniendo estado de %s: %v", node, err)
		return nil, err
	}

	// Reenviar EXACTAMENTE lo del datanode
	return &proto.ResponseEstadoAsignado{
		Datanode:  node,
		IdSistema: resp.IdSistema,
		Asientos:  resp.Asientos,
	}, nil
}

// ================= MAIN ==================

func main() {
	lis, err := net.Listen("tcp", ":60000")
	if err != nil {
		log.Fatalf("[BROKER] Error escuchando: %v", err)
	}

	server := grpc.NewServer()

	// ▶ REGISTRAR BROKER con lista de nodos
	broker := &Broker{
		datanodes: []string{
			"datanode1:58000",
			"datanode2:52000",
			"datanode3:57000",
		},
		rrIndex: 0,
	}

	proto.RegisterAsignadorServiceServer(server, broker)

	log.Println("[BROKER] Servidor iniciado en :60000")
	if err := server.Serve(lis); err != nil {
		log.Fatalf("[BROKER] Error en gRPC: %v", err)
	}
}
