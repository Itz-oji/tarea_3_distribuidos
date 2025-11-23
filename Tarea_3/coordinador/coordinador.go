package main

import (
	"context"
	"log"
	"net"
	"sync"
	"time"

	proto "heint/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// -------------------- Sticky Session --------------------

type StickySession struct {
	Datanode string
	Expire   time.Time
}

const TTL_STICKY = 2 * time.Minute

type Coordinador struct {
	proto.UnimplementedBrokerServiceServer
	mu            sync.Mutex
	stickySession map[string]StickySession // clave = cliente_id
}

// -------------------- Utilidades Sticky --------------------

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

// -------------------- Pedir asignación al Broker --------------------

func (c *Coordinador) requestDataNodeFromBroker(client string) string {
	conn, err := grpc.Dial("broker:60000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("[COORD] Error conectando a broker: %v", err)
		return ""
	}
	defer conn.Close()

	brokerClient := proto.NewAsignadorServiceClient(conn)
	resp, err := brokerClient.AsignarDataNode(context.Background(), &proto.RequestAsignar{Cliente: client})
	if err != nil {
		log.Printf("[COORD] Error asignando nodo: %v", err)
		return ""
	}
	return resp.Datanode
}

// Devuelve DataNode al que debe ir este cliente
func (c *Coordinador) getDataNodeForClient(client string) string {
	// revisar sticky existente
	if dn := c.getSticky(client); dn != "" {
		return dn
	}
	// pedir asignación nueva
	assigned := c.requestDataNodeFromBroker(client)
	if assigned != "" {
		c.saveSticky(client, assigned)
	}
	return assigned
}

// -------------------- Estado del sistema --------------------

func (c *Coordinador) SendEstado(ctx context.Context, req *proto.Response) (*proto.Estado, error) {
	clientID := req.Mensaje // << Cliente envía su ID en Response.Mensaje

	datanode := c.getSticky(clientID)
	if datanode != "" {
		return c.forwardEstadoToDataNode(clientID, datanode)
	}
	return c.requestEstadoFromBroker(clientID)
}

func (c *Coordinador) forwardEstadoToDataNode(client, datanode string) (*proto.Estado, error) {
	log.Printf("[COORD] Cliente %s → leyendo desde sticky en %s", client, datanode)

	conn, err := grpc.Dial(datanode, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	clientDN := proto.NewDataNodeServiceClient(conn)
	resp, err := clientDN.ObtenerEstado(context.Background(), &proto.SolicitudEstado{Cliente: client})
	if err != nil {
		return nil, err
	}

	return &proto.Estado{
		Id:                  resp.IdSistema,
		AsientosDisponibles: resp.Asientos,
	}, nil
}

func (c *Coordinador) requestEstadoFromBroker(client string) (*proto.Estado, error) {
	log.Printf("[COORD] Cliente %s sin sticky → consultando broker", client)

	conn, err := grpc.Dial("broker:60000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	brokerClient := proto.NewAsignadorServiceClient(conn)
	resp, err := brokerClient.ObtenerEstadoYAsignacion(context.Background(), &proto.SolicitudEstado{Cliente: client})
	if err != nil {
		return nil, err
	}

	// guardar sticky con el nodo asignado
	c.saveSticky(client, resp.Datanode)

	return &proto.Estado{
		Id:                  resp.IdSistema,
		AsientosDisponibles: resp.Asientos,
	}, nil
}

// -------------------- Redirigir Escrituras con RYW --------------------

func (c *Coordinador) SendReserva(ctx context.Context, req *proto.AsientoSelect) (*proto.Response, error) {
	clientID := req.ClienteId // << NUEVO: se usa cliente_id, no AsientoID

	datanode := c.getDataNodeForClient(clientID)
	if datanode == "" {
		return &proto.Response{Mensaje: "No hay DataNodes disponibles", Ok: false}, nil
	}

	conn, err := grpc.Dial(datanode, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return &proto.Response{Mensaje: "Error de red", Ok: false}, nil
	}
	defer conn.Close()

	clientDN := proto.NewDataNodeServiceClient(conn)
	resp, err := clientDN.EscribirReserva(ctx, req)

	if err == nil && resp.Ok {
		// Guardar que este cliente debe seguir leyendo de este nodo
		c.saveSticky(clientID, datanode)
		log.Printf("[COORD] Sticky aplicado → %s leerá desde %s", clientID, datanode)
	}

	return resp, err
}

// -------------------- MAIN --------------------

func main() {
	lis, err := net.Listen("tcp", ":54000")
	if err != nil {
		log.Fatalf("Error al abrir puerto: %v", err)
	}

	server := grpc.NewServer()
	coord := &Coordinador{
		stickySession: make(map[string]StickySession),
	}

	proto.RegisterBrokerServiceServer(server, coord)

	log.Println("[COORD] Servidor RYW iniciado en :54000")
	server.Serve(lis)
}
