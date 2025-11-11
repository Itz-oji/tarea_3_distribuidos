package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	proto "example1/proto"

	"google.golang.org/grpc"
)

const (
	// La dirección del servidor Lester   = "10.35.168.59:50051"
	// La dirección del servidor Franklin = "10.35.168.60:50052"
	// La dirección del servidor Trevor   = "10.35.168.61:50053"

	DB1Addr = "10.35.168.59:51000"
	DB2Addr = "10.35.168.59:52000"
	DB3Addr = "10.35.168.59:57000"

	N = 3
	W = 2
	R = 2
)

type subscriber struct {
	consumerID string
	ch         chan *proto.Offer
	categorias []string
	tiendas    []string
	precioMax  float64
}

type BrokerServer struct {
	proto.UnimplementedBrokerServiceServer

	offers    []*proto.Offer
	producers map[string]bool
	nodes     map[string]bool
	processed map[string]map[string]bool
	consumers map[string]bool

	stateMu     sync.Mutex
	subMu       sync.RWMutex
	subscribers map[string][]*subscriber
}

func (s *BrokerServer) addSubscriber(consumerID string, sub *subscriber) {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	if s.subscribers == nil {
		s.subscribers = make(map[string][]*subscriber)
	}
	s.subscribers[consumerID] = append(s.subscribers[consumerID], sub)
}

func (s *BrokerServer) removeSubscriber(consumerID string, sub *subscriber) {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	list := s.subscribers[consumerID]
	if len(list) == 0 {
		return
	}
	n := 0
	for _, x := range list {
		if x != sub {
			list[n] = x
			n++
		}
	}
	list = list[:n]
	if n == 0 {
		delete(s.subscribers, consumerID)
	} else {
		s.subscribers[consumerID] = list
	}
}

func (s *BrokerServer) broadcastToSubscribers(of *proto.Offer) {
	s.subMu.RLock()
	defer s.subMu.RUnlock()

	delivered := 0
	for consumerID, list := range s.subscribers {
		for _, sub := range list {
			if !matchFilters(of, sub) {
				continue // no cumple las preferencias
			}
			select {
			case sub.ch <- of:
				delivered++
			default:
				log.Printf("⚠️ Canal lleno para consumidor %s; descartando oferta %s",
					consumerID, of.GetOfertaId())
			}
		}
	}
	log.Printf("📣 Broadcast oferta %s a %d suscripciones activas", of.GetOfertaId(), delivered)
}

func matchFilters(of *proto.Offer, sub *subscriber) bool {
	// Filtro de categoría
	if len(sub.categorias) > 0 {
		ok := false
		for _, c := range sub.categorias {
			if strings.EqualFold(c, of.GetCategoria()) {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}

	// Filtro de tienda
	if len(sub.tiendas) > 0 {
		ok := false
		for _, t := range sub.tiendas {
			if strings.EqualFold(t, of.GetTienda()) {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}

	// Filtro de precio máximo
	if sub.precioMax > 0 && of.GetPrecio() > sub.precioMax {
		return false
	}

	return true
}

func (s *BrokerServer) sendToNodesQuorum(ctx context.Context, offer *proto.Offer) (int, error) {
	nodes := []string{DB1Addr, DB2Addr, DB3Addr}
	var mu sync.Mutex
	var wg sync.WaitGroup
	ackCount := 0

	// timeout por cada llamada
	timeout := 2 * time.Second

	for _, node := range nodes {

		s.stateMu.Lock()
		if s.nodes == nil || !s.nodes[node] {
			s.stateMu.Unlock()
			log.Printf("❌ Nodo %s no registrado, no se enviará la oferta", node)
			continue
		}
		s.stateMu.Unlock()

		wg.Add(1)
		go func(nodeAddr string) {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			conn, err := grpc.DialContext(cctx, nodeAddr, grpc.WithInsecure(), grpc.WithBlock())
			if err != nil {
				log.Printf("No se pudo conectar con el nodo %s: %v", nodeAddr, err)
				return
			}
			defer conn.Close()

			client := proto.NewBrokerServiceClient(conn)
			_, err = client.SendOffer(cctx, offer)
			if err != nil {
				log.Printf("Error al enviar la oferta al nodo %s: %v", nodeAddr, err)
				return
			}

			mu.Lock()
			ackCount++
			mu.Unlock()
		}(node)
	}

	// Esperar a que terminen las llamadas (pero no más de un timeout razonable)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout + 500*time.Millisecond):
		// continue, contaremos los ACKs recibidos hasta ahora
	}

	if ackCount < W {
		return ackCount, fmt.Errorf("no se alcanzó quorum de escritura: %d/%d", ackCount, W)
	}
	return ackCount, nil
}

func (s *BrokerServer) SendOffer(ctx context.Context, offer *proto.Offer) (*proto.Response, error) {
	log.Printf("📦 Oferta recibida: ID=%s | Tienda=%s | Producto=%s | Categoria=%s | Precio=%.2f | Stock=%d | Fecha=%s",
		offer.OfertaId, offer.Tienda, offer.Producto, offer.Categoria, offer.Precio, offer.Stock,
		time.Unix(offer.Fecha, 0).Format("2006-01-02 15:04:05"))

	if offer.OfertaId == "" {
		return nil, fmt.Errorf("offer_id is required")
	}

	// ---- sección crítica: validar producer e idempotencia ----
	s.stateMu.Lock()
	if s.producers == nil || !s.producers[offer.ProductoId] {
		s.stateMu.Unlock()
		log.Printf("❌ Productor no registrado: %s", offer.ProductoId)
		return nil, fmt.Errorf("producer %s not registered", offer.ProductoId)
	}
	if s.processed == nil {
		s.processed = make(map[string]map[string]bool)
	}
	if _, ok := s.processed[offer.ProductoId]; !ok {
		s.processed[offer.ProductoId] = make(map[string]bool)
	}
	if s.processed[offer.ProductoId][offer.OfertaId] {
		s.stateMu.Unlock()
		log.Printf("⚠️ Oferta duplicada ignorada: %s", offer.OfertaId)
		return &proto.Response{Mensaje: fmt.Sprintf("duplicate offer %s ignored", offer.OfertaId)}, nil
	}
	s.stateMu.Unlock()
	// ---- fin sección crítica ----

	// Replicar a nodos con quórum W
	ackCount, err := s.sendToNodesQuorum(ctx, offer)
	if err != nil {
		log.Printf("⚠️ No se alcanzó quorum al replicar la oferta %s: %v", offer.OfertaId, err)
		return nil, fmt.Errorf("no se pudo almacenar la oferta en quorum: %v", err)
	}

	// Guardar local e idempotencia con lock
	s.stateMu.Lock()
	s.offers = append(s.offers, offer)
	s.processed[offer.ProductoId][offer.OfertaId] = true
	s.stateMu.Unlock()

	// 🔊 **AQUÍ estaba el faltante**: notificar a los suscriptores
	s.broadcastToSubscribers(offer)

	log.Printf("✅ Oferta procesada correctamente: %s (%d ACKs)", offer.OfertaId, ackCount)
	return &proto.Response{
		Mensaje: fmt.Sprintf("Oferta %s recibida y replicada (%d acks)", offer.OfertaId, ackCount),
	}, nil
}

func (s *BrokerServer) GetHistory(ctx context.Context, req *proto.HistoryRequest) (*proto.HistoryResponse, error) {
	// Implementación de lectura por quorum R: consultar los N nodos y devolver
	// solo las ofertas que aparecen en al menos R respuestas.
	nodes := []string{DB1Addr, DB2Addr, DB3Addr}
	type result struct {
		offers []*proto.Offer
		err    error
	}

	ch := make(chan result, len(nodes))
	timeout := 2 * time.Second

	// Consultar cada nodo en paralelo
	for _, node := range nodes {
		go func(nodeAddr string) {
			cctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			conn, err := grpc.DialContext(cctx, nodeAddr, grpc.WithInsecure(), grpc.WithBlock())
			if err != nil {
				ch <- result{nil, err}
				return
			}
			defer conn.Close()
			client := proto.NewBrokerServiceClient(conn)
			resp, err := client.GetHistory(cctx, req)
			if err != nil {
				ch <- result{nil, err}
				return
			}
			ch <- result{resp.Offers, nil}
		}(node)
	}

	// Recolectar respuestas con un deadline
	collected := 0
	offerCounts := make(map[string]int)
	offerSamples := make(map[string]*proto.Offer)

	deadline := time.After(timeout + 500*time.Millisecond)
	for collected < len(nodes) {
		select {
		case r := <-ch:
			collected++
			if r.err != nil || r.offers == nil {
				continue
			}
			for _, of := range r.offers {
				id := of.OfertaId
				offerCounts[id]++
				// Guardar una muestra para devolver más tarde
				if _, ok := offerSamples[id]; !ok {
					// Hacer una copia ligera
					tmp := *of
					offerSamples[id] = &tmp
				}
			}
		case <-deadline:
			// tiempo agotado, procesar lo que tengamos
			collected = len(nodes)
		}
	}

	// Seleccionar ofertas que alcanzaron quorum R
	var resultOffers []*proto.Offer
	for id, cnt := range offerCounts {
		if cnt >= R {
			if sample, ok := offerSamples[id]; ok {
				resultOffers = append(resultOffers, sample)
			}
		}
	}

	// También podríamos complementar con las ofertas locales del broker si lo deseamos,
	// pero aquí seguimos la regla: lectura válida sólo si al menos R nodos coinciden.

	return &proto.HistoryResponse{Offers: resultOffers}, nil
}

func (s *BrokerServer) RegisterProducer(ctx context.Context, req *proto.RegisterRequest) (*proto.RegisterResponse, error) {
	if req == nil || req.ProducerId == "" {
		return &proto.RegisterResponse{Ok: false, Message: "producer_id required"}, nil
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.producers == nil {
		s.producers = make(map[string]bool)
	}
	s.producers[req.ProducerId] = true
	return &proto.RegisterResponse{Ok: true, Message: "registered"}, nil
}

func (s *BrokerServer) RegisterNode(ctx context.Context, req *proto.RegisterRequest) (*proto.RegisterResponse, error) {
	if req == nil || req.ProducerId == "" {
		return &proto.RegisterResponse{Ok: false, Message: "node_id required"}, nil
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.nodes == nil {
		s.nodes = make(map[string]bool)
	}
	s.nodes[req.ProducerId] = true
	log.Printf("✅ Nodo registrado: %s", req.ProducerId)
	return &proto.RegisterResponse{Ok: true, Message: "node registered"}, nil
}

func (s *BrokerServer) RegisterConsumer(ctx context.Context, req *proto.RegisterRequest) (*proto.RegisterResponse, error) {
	if req == nil || req.ProducerId == "" {
		return &proto.RegisterResponse{Ok: false, Message: "consumer_id required"}, nil
	}

	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	if s.consumers == nil {
		s.consumers = make(map[string]bool)
	}

	s.consumers[req.ProducerId] = true
	log.Printf("✅ Consumidor registrado: %s", req.ProducerId)

	return &proto.RegisterResponse{Ok: true, Message: "consumer registered"}, nil
}

func (s *BrokerServer) SendOfferToNodes(ctx context.Context, offer *proto.Offer) (*proto.Response, error) {
	// Conectar con cada uno de los nodos
	nodes := []string{DB1Addr, DB2Addr, DB3Addr}
	var lastErr error
	for _, node := range nodes {
		conn, err := grpc.DialContext(ctx, node, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			log.Printf("No se pudo conectar con el nodo %s: %v", node, err)
			lastErr = err
			continue
		}

		client := proto.NewBrokerServiceClient(conn)
		_, err = client.SendOffer(ctx, offer)
		// Close the connection immediately after use
		_ = conn.Close()
		if err != nil {
			log.Printf("Error al enviar la oferta al nodo %s: %v", node, err)
			lastErr = err
		}
	}

	if lastErr != nil {
		return &proto.Response{
			Mensaje: fmt.Sprintf("Oferta %s enviada con errores", offer.OfertaId),
		}, nil
	}

	return &proto.Response{
		Mensaje: fmt.Sprintf("Oferta %s enviada a todos los nodos", offer.OfertaId),
	}, nil
}

func (s *BrokerServer) SubscribeOffers(req *proto.SubscriptionRequest, stream proto.BrokerService_SubscribeOffersServer) error {
	if req == nil || req.ConsumidorId == "" {
		return fmt.Errorf("consumidor_id requerido")
	}
	consumerID := req.GetConsumidorId()

	s.stateMu.Lock()
	registered := s.consumers != nil && s.consumers[consumerID]
	s.stateMu.Unlock()

	if !registered {
		log.Printf("❌ Consumidor %s no registrado, suscripción rechazada", consumerID)
		return fmt.Errorf("consumidor %s no registrado", consumerID)
	}

	log.Printf("📡 Nuevo suscriptor conectado: consumidor_id=%s", consumerID)

	// Canal bufferizado para no bloquear el broker
	sub := &subscriber{
		consumerID: consumerID,
		ch:         make(chan *proto.Offer, 128),
		categorias: req.Categorias,
		tiendas:    req.Tiendas,
		precioMax:  req.PrecioMax,
	}
	s.addSubscriber(consumerID, sub)
	defer func() {
		s.removeSubscriber(consumerID, sub)
		close(sub.ch)
		log.Printf("👋 Suscriptor desconectado: consumidor_id=%s", consumerID)
	}()

	ctx := stream.Context()

	for {
		select {
		case <-ctx.Done():
			// El cliente cerró la conexión o se canceló el contexto
			return nil
		case of := <-sub.ch:
			// Enviar oferta al cliente
			if err := stream.Send(of); err != nil {
				log.Printf("⚠️ Error enviando oferta a %s: %v", consumerID, err)
				return err
			}
		}
	}
}

func main() {
	server := &BrokerServer{
		producers: make(map[string]bool),
		nodes: map[string]bool{
			DB1Addr: true,
			DB2Addr: true,
			DB3Addr: true,
		},
	}

	brokenServer := grpc.NewServer()

	proto.RegisterBrokerServiceServer(brokenServer, server)

	listener, err := net.Listen("tcp", ":54000")
	if err != nil {
		log.Fatalf("Fallo al escuchar: %v", err)
	}

	log.Printf("Broker escuchando en %v", listener.Addr())
	if err := brokenServer.Serve(listener); err != nil {
		log.Fatalf("Fallo al servir: %v", err)
	}

}

