package main

import (
	"context"
	"log"
	"net"
	"sync"
	"time"

	proto "heint/proto"

	"google.golang.org/grpc"
)

type StickySession struct {
	Datanode string
	Expire   time.Time
}

const TTL_STICKY = 2 * time.Minute

type Coordinador struct {
	proto.UnimplementedBrokerServiceServer
	mu            sync.Mutex
	stickySession map[string]StickySession
}

func (c *Coordinador) requestDataNodeFromBroker(client string) string {
	// Conectar hacia el Broker real
	conn, err := grpc.Dial("broker:60000", grpc.WithInsecure()) // <--- broker verdadero
	if err != nil {
		log.Printf("[COORD] Error pidiendo nodo al broker: %v", err)
		return ""
	}
	defer conn.Close()

	brokerClient := proto.NewAsignadorServiceClient(conn) // <- servicio que elige DataNode

	resp, err := brokerClient.AsignarDataNode(context.Background(),
		&proto.RequestAsignar{Cliente: client})
	if err != nil {
		log.Printf("[COORD] Broker no pudo asignar nodo: %v", err)
		return ""
	}

	return resp.Datanode
}

func (c *Coordinador) getDataNodeForClient(client string) string {
	c.mu.Lock()
	info, exists := c.stickySession[client]
	c.mu.Unlock()

	if exists && time.Now().Before(info.Expire) {
		return info.Datanode
	}

	assigned := c.requestDataNodeFromBroker(client)
	if assigned == "" {
		return ""
	}

	c.saveSticky(client, assigned)
	return assigned
}

func (c *Coordinador) SendEstado(ctx context.Context, req *proto.Response) (*proto.Estado, error) {
	clientID := req.Mensaje

	// 1️⃣ Verificar si ya existe sticky session
	datanode := c.getSticky(clientID)
	if datanode != "" {
		// 1.A️⃣ Ya existe DataNode → Preguntar directo al DataNode
		return c.forwardEstadoToDataNode(clientID, datanode)
	}

	// 2️⃣ NO EXISTE sticky → pedir al Broker asignación + estado
	return c.requestEstadoFromBroker(clientID)
}

func (c *Coordinador) getSticky(client string) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	info, exists := c.stickySession[client]
	if !exists || time.Now().After(info.Expire) {
		return ""
	}
	return info.Datanode
}

func (c *Coordinador) saveSticky(client, datanode string) {
	c.mu.Lock()
	c.stickySession[client] = StickySession{
		Datanode: datanode,
		Expire:   time.Now().Add(TTL_STICKY),
	}
	c.mu.Unlock()
}

func (c *Coordinador) SendReserva(ctx context.Context, req *proto.AsientoSelect) (*proto.Response, error) {
	clientID := req.AsientoID

	datanode := c.getDataNodeForClient(clientID)
	if datanode == "" {
		return &proto.Response{Mensaje: "No disponible"}, nil
	}

	log.Printf("[COORD] Enrutando reserva de %s a %s", clientID, datanode)

	// Aquí el Coordinador solo reenvía, no valida
	conn, err := grpc.Dial(datanode, grpc.WithInsecure())
	if err != nil {
		log.Printf("[COORD] Error reenviando a datanode: %v", err)
		return &proto.Response{Mensaje: "Error de red"}, nil
	}
	defer conn.Close()

	nodeClient := proto.NewDataNodeServiceClient(conn)
	return nodeClient.EscribirReserva(ctx, req)
}

func (c *Coordinador) forwardEstadoToDataNode(client, datanode string) (*proto.Estado, error) {
	log.Printf("[COORD] Cliente %s tiene sticky → preguntar a %s", client, datanode)

	conn, err := grpc.Dial(datanode, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	nodeClient := proto.NewDataNodeServiceClient(conn)
	resp, err := nodeClient.ObtenerEstado(context.Background(), &proto.SolicitudEstado{Cliente: client})
	if err != nil {
		return nil, err
	}

	return &proto.Estado{
		Id:                  resp.IdSistema,
		AsientosDisponibles: resp.Asientos,
	}, nil
}

func (c *Coordinador) requestEstadoFromBroker(client string) (*proto.Estado, error) {
	log.Printf("[COORD] Cliente %s sin sticky → pedir al broker", client)

	conn, err := grpc.Dial("broker:60000", grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	brokerClient := proto.NewAsignadorServiceClient(conn)

	resp, err := brokerClient.ObtenerEstadoYAsignacion(context.Background(), &proto.SolicitudEstado{Cliente: client})
	if err != nil {
		return nil, err
	}

	// Guardar Sticky asignado por el broker
	c.saveSticky(client, resp.Datanode)

	return &proto.Estado{
		Id:                  resp.IdSistema,
		AsientosDisponibles: resp.Asientos,
	}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":54000")
	if err != nil {
		log.Fatalf("Error al abrir puerto: %v", err)
	}

	server := grpc.NewServer()
	coordinador := &Coordinador{
		stickySession: make(map[string]StickySession),
	}

	proto.RegisterBrokerServiceServer(server, coordinador)

	log.Println("[COORD] Servidor iniciado en :54000")
	if err := server.Serve(lis); err != nil {
		log.Fatalf("Error al iniciar gRPC server: %v", err)
	}
}
