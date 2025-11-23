package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	consensuspb "heint/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Estado de un nodo de consenso.
type State int

// Definición de los posibles estados de un nodo Raft.
const (
	// Seguidor es un nodo pasivo que responde a las solicitudes del líder.
	Follower State = iota
	// Candidato es un nodo que intenta convertirse en líder.
	Candidate
	// Líder es el nodo responsable de aceptar propuestas y replicar entradas.
	Leader
)

// RaftNode representa un nodo en el protocolo de consenso Raft.
type RaftNode struct {
	consensuspb.UnimplementedConsensusServer
	mu                sync.Mutex                             // mutex para proteger el estado del nodo
	id                string                                 // ID del nodo
	state             State                                  // estado actual del nodo
	currentTerm       int32                                  // término actual
	votedFor          string                                 // ID del candidato al que se le ha otorgado el voto en el término actual
	logEntries        []consensuspb.LogEntry                 // entradas del registro
	commitIndex       int32                                  // índice de la última entrada confirmada
	lastApplied       int32                                  // índice de la última entrada aplicada al estado
	peers             map[string]string                      // mapeo de ID de nodo a dirección de red
	clients           map[string]consensuspb.ConsensusClient // clientes gRPC para comunicarse con pares
	nextIndex         map[string]int32                       // próximo índice de entrada para enviar a cada par
	matchIndex        map[string]int32                       // índice máximo de entrada confirmada para cada par
	electionTimer     *time.Timer                            // temporizador para elecciones
	heartbeatInterval time.Duration                          // intervalo entre latidos del líder
	commitWaiters     map[int]chan struct{}                  // canales para notificar cuando una entrada se confirma
	appliedState      []string                               // estado aplicado al nodo
	leaderId          string                                 // ID del líder actual
}

// NewRaftNode crea e inicializa un nuevo nodo Raft con el ID y los pares dados.
func NewRaftNode(id string, peers map[string]string) *RaftNode {
	rn := &RaftNode{
		id:                id,
		state:             Follower,
		currentTerm:       0,
		votedFor:          "",
		logEntries:        make([]consensuspb.LogEntry, 0),
		commitIndex:       -1,
		lastApplied:       -1,
		peers:             peers,
		clients:           make(map[string]consensuspb.ConsensusClient),
		nextIndex:         make(map[string]int32),
		matchIndex:        make(map[string]int32),
		heartbeatInterval: 100 * time.Millisecond,
		commitWaiters:     make(map[int]chan struct{}),
		appliedState:      make([]string, 0),
		leaderId:          "",
	}
	return rn
}

// Obtiene un tiempo de espera de elección aleatorio entre 150 y 300 ms para evitar colisiones.
func (rn *RaftNode) getRandomElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// Reinicia el temporizador de elección con un nuevo tiempo de espera aleatorio para evitar colisiones.
func (rn *RaftNode) resetElectionTimer() {
	if rn.electionTimer == nil {
		rn.electionTimer = time.NewTimer(rn.getRandomElectionTimeout())
		go rn.runElectionWatcher()
		return
	}
	if !rn.electionTimer.Stop() {
		select {
		case <-rn.electionTimer.C:
		default:
		}
	}
	rn.electionTimer.Reset(rn.getRandomElectionTimeout())
}

// Observa el temporizador de elección y comienza una nueva elección si expira, para nodos que no son líderes.
func (rn *RaftNode) runElectionWatcher() {
	for {
		<-rn.electionTimer.C
		rn.mu.Lock()
		if rn.state != Leader {
			rn.startElection()
		}
		rn.mu.Unlock()
	}
}

// Inicia una nueva elección incrementando el término, votando por sí mismo y solicitando votos a los pares.
func (rn *RaftNode) startElection() {
	rn.state = Candidate                          // Cambia al estado de candidato
	rn.currentTerm++                              // Incrementa el término actual
	rn.votedFor = rn.id                           // Vota por sí mismo
	rn.leaderId = ""                              // Limpia el ID del líder
	votes := 1                                    // Cuenta el voto propio
	lastLogIndex := int32(len(rn.logEntries) - 1) // Índice de la última entrada del registro
	var lastLogTerm int32                         // Término de la última entrada del registro

	// Obtiene el término de la última entrada del registro si existe
	if lastLogIndex >= 0 {
		lastLogTerm = rn.logEntries[lastLogIndex].Term
	}

	// Reinicia el temporizador de elección para evitar nuevas elecciones prematuras
	rn.resetElectionTimer()
	termAtStart := rn.currentTerm // Guarda el término al inicio de la elección
	var wg sync.WaitGroup         // Grupo de espera para las solicitudes de voto

	// Envía solicitudes de voto a todos los pares
	for pid, client := range rn.clients {

		// Omite enviarse a sí mismo
		if pid == rn.id {
			continue
		}

		wg.Add(1) // Incrementa el contador del grupo de espera

		// Envía la solicitud de voto en una goroutine separada
		go func(peerId string, cli consensuspb.ConsensusClient) {
			defer wg.Done()                                                         // Marca la goroutine como completada al finalizar
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second) // Contexto con tiempo de espera
			defer cancel()                                                          // Cancela el contexto al finalizar

			// Crea y envía la solicitud de voto
			req := &consensuspb.VoteRequest{
				Term:         termAtStart,
				CandidateId:  rn.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			resp, err := cli.RequestVote(ctx, req) // Envía la solicitud de voto y recibe la respuesta

			// Maneja errores de comunicación
			if err != nil {
				return
			}
			rn.mu.Lock()
			defer rn.mu.Unlock()
			if resp.Term > rn.currentTerm {
				rn.currentTerm = resp.Term
				rn.state = Follower
				rn.votedFor = ""
				rn.resetElectionTimer()
				return
			}

			// Si el nodo ya no es candidato o el término ha cambiado, ignora la respuesta
			if rn.state != Candidate || rn.currentTerm != termAtStart {
				return
			}

			// Cuenta el voto si fue concedido
			if resp.VoteGranted {
				votes++
				n := len(rn.peers) + 1 // Número total de nodos

				// Si obtiene la mayoría de votos, se convierte en líder
				if votes > n/2 {
					rn.becomeLeader()
					return
				}
			}
		}(pid, client)
	}

	rn.mu.Unlock()
	wg.Wait()
	rn.mu.Lock()

	rn.mu.Unlock()
}

// Transforma el nodo en líder, inicializa los índices y envía latidos iniciales.
func (rn *RaftNode) becomeLeader() {
	rn.state = Leader                      // Cambia al estado de líder
	rn.leaderId = rn.id                    // Establece su propio ID como líder
	lastIndex := int32(len(rn.logEntries)) // Índice de la próxima entrada a replicar

	// Inicializa los índices nextIndex y matchIndex para cada par
	for pid := range rn.peers {
		rn.nextIndex[pid] = lastIndex
		rn.matchIndex[pid] = -1
	}

	rn.matchIndex[rn.id] = lastIndex - 1 // Inicializa el índice de coincidencia para sí mismo
	rn.nextIndex[rn.id] = lastIndex      // Inicializa el índice siguiente para sí mismo

	rn.broadcastHeartbeats() // Envía latidos iniciales a los pares
}

// Envía latidos a todos los pares para mantener la autoridad del líder.
func (rn *RaftNode) broadcastHeartbeats() {
	rn.broadcastAppendEntries(nil) // Envía entradas vacías como latidos
}

// Envía solicitudes de AppendEntries a todos los pares con las entradas proporcionadas.
func (rn *RaftNode) broadcastAppendEntries(entries []consensuspb.LogEntry) {

	// Solo el líder debe enviar AppendEntries
	if rn.state != Leader {
		return
	}

	term := rn.currentTerm         // Término actual del líder
	leaderId := rn.id              // ID del líder
	leaderCommit := rn.commitIndex // Índice de la última entrada confirmada

	// Copia las entradas del registro para evitar condiciones de carrera
	logCopy := make([]consensuspb.LogEntry, len(rn.logEntries))
	copy(logCopy, rn.logEntries) // Copia segura del registro

	// Envía AppendEntries a cada par en una goroutine separada
	for pid, client := range rn.clients {

		// Omite enviarse a sí mismo
		if pid == rn.id {
			continue
		}

		peerId := pid // Captura el ID del par
		cli := client // Captura el cliente gRPC del par

		// Envía la solicitud de AppendEntries en una goroutine separada
		go func() {
			rn.mu.Lock()                    // Bloquea el estado del nodo para acceder a nextIndex
			nextIdx := rn.nextIndex[peerId] // Próximo índice para enviar al par
			prevLogIndex := nextIdx - 1     // Índice de la entrada anterior
			var prevLogTerm int32           // Término de la entrada anterior

			// Obtiene el término de la entrada anterior si existe
			if prevLogIndex >= 0 && int(prevLogIndex) < len(logCopy) {
				prevLogTerm = logCopy[prevLogIndex].Term
			}

			ents := make([]*consensuspb.LogEntry, 0) // Entradas a enviar al par

			// Prepara las entradas a enviar comenzando desde nextIdx
			for i := nextIdx; i < int32(len(logCopy)); i++ {
				e := logCopy[i] // Entrada del registro

				// Crea una copia de la entrada para evitar condiciones de carrera
				entCopy := &consensuspb.LogEntry{
					Term:    e.Term,
					Command: e.Command,
				}
				ents = append(ents, entCopy) // Agrega la entrada copiada a la lista
			}
			rn.mu.Unlock() // Desbloquea el estado del nodo

			// Crea y envía la solicitud de AppendEntries
			req := &consensuspb.AppendRequest{
				Term:         term,
				LeaderId:     leaderId,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      ents,
				LeaderCommit: leaderCommit,
			}

			// Envía la solicitud con un contexto de tiempo de espera
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			resp, err := cli.AppendEntries(ctx, req)

			// Maneja errores de comunicación
			if err != nil {
				return
			}
			rn.mu.Lock()
			defer rn.mu.Unlock()
			if resp.Term > rn.currentTerm {
				rn.currentTerm = resp.Term
				rn.state = Follower
				rn.votedFor = ""
				rn.leaderId = ""
				rn.resetElectionTimer()
				return
			}

			// Si el nodo ya no es líder o el término ha cambiado, ignora la respuesta
			if rn.state != Leader || rn.currentTerm != term {
				return
			}

			// Actualiza los índices según el éxito de la replicación
			if resp.Success {

				rn.nextIndex[peerId] = prevLogIndex + int32(len(ents)) + 1 // Actualiza nextIndex
				rn.matchIndex[peerId] = rn.nextIndex[peerId] - 1           // Actualiza matchIndex

				// Verifica si se puede avanzar el commitIndex
				matchIndices := make([]int, 0, len(rn.matchIndex))

				// Recolecta todos los matchIndex
				for _, idx := range rn.matchIndex {
					matchIndices = append(matchIndices, int(idx)) // Convierte a int para ordenar
				}
				sort.Ints(matchIndices)                            // Ordena los índices de coincidencia
				majorityIndex := matchIndices[len(matchIndices)/2] // Índice de mayoría

				// Avanza commitIndex si la mayoría ha replicado una entrada del término actual
				if majorityIndex > int(rn.commitIndex) &&
					majorityIndex < len(rn.logEntries) &&
					rn.logEntries[majorityIndex].Term == rn.currentTerm {
					rn.commitIndex = int32(majorityIndex)
					rn.applyLog()
				}
			} else {
				// Retrocede nextIndex en caso de fallo
				rn.nextIndex[peerId]--

				// Asegura que nextIndex no sea negativo
				if rn.nextIndex[peerId] < 0 {
					rn.nextIndex[peerId] = 0
				}
			}
		}()
	}
}

// Aplica las entradas confirmadas al estado del nodo.
func (rn *RaftNode) applyLog() {

	// Aplica las entradas desde lastApplied + 1 hasta commitIndex
	for rn.lastApplied < rn.commitIndex {
		rn.lastApplied++
		idx := rn.lastApplied

		// Asegura que el índice esté dentro de los límites del registro
		if idx < 0 || int(idx) >= len(rn.logEntries) {
			continue
		}
		entry := rn.logEntries[idx]
		rn.appliedState = append(rn.appliedState, entry.Command)

		// Notifica a cualquier espera de confirmación que la entrada ha sido aplicada
		if ch, ok := rn.commitWaiters[int(idx)]; ok {
			close(ch)
			delete(rn.commitWaiters, int(idx))
		}
	}
}

// Maneja las solicitudes de voto entrantes de otros nodos.
func (rn *RaftNode) RequestVote(ctx context.Context, req *consensuspb.VoteRequest) (*consensuspb.VoteResponse, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	resp := &consensuspb.VoteResponse{
		Term:        rn.currentTerm,
		VoteGranted: false,
	}

	// Si el término de la solicitud es menor, rechaza el voto
	if req.Term < rn.currentTerm {
		return resp, nil
	}

	// Si el término de la solicitud es mayor, actualiza el término y reinicia el estado
	if req.Term > rn.currentTerm {
		rn.currentTerm = req.Term
		rn.votedFor = ""
		rn.state = Follower
		rn.leaderId = ""
	}

	var lastLogTerm int32  // Término de la última entrada del registro
	var lastLogIndex int32 // Índice de la última entrada del registro

	// Obtiene el término e índice de la última entrada del registro
	if len(rn.logEntries) > 0 {
		lastLogIndex = int32(len(rn.logEntries) - 1)   // Índice de la última entrada
		lastLogTerm = rn.logEntries[lastLogIndex].Term // Término de la última entrada
		// Si no hay entradas, establece los valores en -1
	} else {
		lastLogIndex = -1
		lastLogTerm = -1
	}

	// Verifica si el registro del candidato está actualizado
	upToDate := req.LastLogTerm > lastLogTerm ||
		(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)

		// Concede el voto si no ha votado aún o ya votó por el candidato, y el registro está actualizado
	if (rn.votedFor == "" || rn.votedFor == req.CandidateId) && upToDate {
		rn.votedFor = req.CandidateId
		rn.state = Follower
		rn.resetElectionTimer()
		resp.Term = rn.currentTerm
		resp.VoteGranted = true
		return resp, nil
	}

	// Rechaza el voto si no se cumplen las condiciones
	resp.Term = rn.currentTerm
	resp.VoteGranted = false
	return resp, nil
}

// Maneja las solicitudes de AppendEntries entrantes de otros nodos.
func (rn *RaftNode) AppendEntries(ctx context.Context, req *consensuspb.AppendRequest) (*consensuspb.AppendResponse, error) {
	// Bloquea el estado del nodo para manejar la solicitud
	rn.mu.Lock()
	defer rn.mu.Unlock()
	resp := &consensuspb.AppendResponse{
		Term:       rn.currentTerm,
		Success:    false,
		MatchIndex: -1,
	}

	// Si el término de la solicitud es menor, rechaza la solicitud
	if req.Term < rn.currentTerm {
		return resp, nil
	}

	// Si el término de la solicitud es mayor, actualiza el término y reinicia el estado
	if req.Term > rn.currentTerm {
		rn.currentTerm = req.Term
		rn.votedFor = ""
		rn.state = Follower
	}
	rn.leaderId = req.LeaderId
	rn.resetElectionTimer() // Reinicia el temporizador de elección

	// Verifica la coincidencia del registro
	if req.PrevLogIndex >= 0 {

		// Si el índice previo es mayor que el último índice del registro, rechaza la solicitud
		if int(req.PrevLogIndex) >= len(rn.logEntries) {
			return resp, nil
		}

		// Si el término en el índice previo no coincide, rechaza la solicitud
		if rn.logEntries[req.PrevLogIndex].Term != req.PrevLogTerm {
			rn.logEntries = rn.logEntries[:req.PrevLogIndex]
			return resp, nil
		}
	}
	// Agrega nuevas entradas al registro
	for i, ent := range req.Entries {
		idx := req.PrevLogIndex + 1 + int32(i) // Índice donde se debe agregar la entrada

		// Si ya existe una entrada en ese índice con un término diferente, trunca el registro
		if int(idx) < len(rn.logEntries) {
			// Trunca el registro si hay un conflicto de términos
			if rn.logEntries[idx].Term != ent.Term {
				rn.logEntries = rn.logEntries[:idx]
			}
		}
		// Agrega la nueva entrada al registro si es un nuevo índice
		if int(idx) >= len(rn.logEntries) {
			rn.logEntries = append(rn.logEntries, *ent)
		}
	}
	// Actualiza el índice de confirmación si es necesario
	if req.LeaderCommit > rn.commitIndex {
		lastIndex := int32(len(rn.logEntries) - 1)

		// Avanza commitIndex hasta el mínimo entre LeaderCommit y el último índice del registro
		if req.LeaderCommit < lastIndex {
			rn.commitIndex = req.LeaderCommit

			// Si LeaderCommit es mayor, avanza hasta el último índice
		} else {
			rn.commitIndex = lastIndex
		}
		rn.applyLog() // Aplica las entradas confirmadas al estado
	}

	// Responde con éxito y el índice de la última entrada confirmada
	resp.Term = rn.currentTerm
	resp.Success = true
	resp.MatchIndex = int32(len(rn.logEntries) - 1) // Último índice del registro
	return resp, nil                                // Responde con éxito
}

// Maneja las propuestas de clientes para agregar nuevas entradas al registro.
func (rn *RaftNode) Propose(ctx context.Context, req *consensuspb.Proposal) (*consensuspb.ConsensusReply, error) {
	rn.mu.Lock() // Bloquea el estado del nodo para manejar la propuesta

	// Solo el líder puede aceptar propuestas
	if rn.state != Leader {
		leader := rn.leaderId
		rn.mu.Unlock()
		msg := "not leader"

		// Si se conoce el líder, incluye un mensaje de redirección
		if leader != "" {
			msg = fmt.Sprintf("redirect to leader %s", leader)
		}

		// Responde indicando que no es el líder
		return &consensuspb.ConsensusReply{
			Success: false,
			Result:  msg,
		}, nil
	}

	// Agrega la nueva entrada al registro
	entry := consensuspb.LogEntry{
		Term:    rn.currentTerm,
		Command: req.Command,
	}

	// Añade la entrada al registro y prepara un canal para esperar la confirmación
	rn.logEntries = append(rn.logEntries, entry)
	idx := len(rn.logEntries) - 1
	waiter := make(chan struct{})
	rn.commitWaiters[idx] = waiter
	rn.mu.Unlock()

	// Inicia la replicación de la nueva entrada a los pares
	rn.broadcastAppendEntries([]consensuspb.LogEntry{entry})

	// Espera a que la entrada sea confirmada o a que ocurra un tiempo de espera
	select {

	// Si la entrada es confirmada, responde con éxito
	case <-waiter:
		return &consensuspb.ConsensusReply{
			Success: true,
			Result:  "committed",
		}, nil

		// Si ocurre un tiempo de espera, responde con un error
	case <-time.After(5 * time.Second):
		rn.mu.Lock()
		delete(rn.commitWaiters, idx)
		rn.mu.Unlock()
		return &consensuspb.ConsensusReply{
			Success: false,
			Result:  "commit timed out",
		}, nil
	}
}

// Punto de entrada principal para iniciar el nodo de consenso Raft.
func main() {
	// Inicializa la semilla aleatoria y analiza las banderas de línea de comandos
	rand.Seed(time.Now().UnixNano())
	idFlag := flag.String("id", "", "node ID (e.g. consenso1)")
	portFlag := flag.String("port", "", "port to listen on (e.g. 50051)")
	peersFlag := flag.String("peers", "", "comma separated list of peer mappings id=address")
	flag.Parse()

	// Permite configurar los parámetros mediante variables de entorno
	if env := os.Getenv("CONSENSUS_ID"); env != "" {
		*idFlag = env
	}

	// Permite configurar los parámetros mediante variables de entorno
	if env := os.Getenv("CONSENSUS_PORT"); env != "" {
		*portFlag = env
	}

	// Permite configurar los parámetros mediante variables de entorno
	if env := os.Getenv("CONSENSUS_PEERS"); env != "" {
		*peersFlag = env
	}

	// Verifica que se hayan proporcionado ID y puerto
	if *idFlag == "" || *portFlag == "" {
		log.Fatalf("id and port must be specified via flags or environment variables")
	}
	peers := make(map[string]string) // Mapeo de ID de nodo a dirección de red

	// Parsea la lista de pares proporcionada
	if *peersFlag != "" {
		items := strings.Split(*peersFlag, ",")

		// Procesa cada par en la lista
		for _, item := range items {
			item = strings.TrimSpace(item)

			// Omite entradas vacías
			if item == "" {
				continue
			}

			parts := strings.Split(item, "=") // Divide en ID y dirección

			// Verifica que el formato sea correcto
			if len(parts) != 2 {
				log.Fatalf("invalid peer format: %s", item)
			}

			// Agrega el par al mapeo
			pid := parts[0]
			addr := parts[1]
			peers[pid] = addr
		}
	}
	rn := NewRaftNode(*idFlag, peers) // Crea un nuevo nodo Raft

	// Establece conexiones gRPC con todos los pares
	for pid, addr := range peers {
		// Establece la conexión gRPC
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))

		// Maneja errores de conexión
		if err != nil {
			log.Printf("failed to connect to peer %s at %s: %v", pid, addr, err)
			continue
		}
		rn.clients[pid] = consensuspb.NewConsensusClient(conn)
	}

	// Inicia el temporizador de elección
	rn.mu.Lock()
	rn.resetElectionTimer()
	rn.mu.Unlock()
	lis, err := net.Listen("tcp", ":"+*portFlag)

	// Maneja errores al iniciar el servidor
	if err != nil {
		log.Fatalf("failed to listen on port %s: %v", *portFlag, err)
	}

	// Crea y arranca el servidor gRPC
	grpcServer := grpc.NewServer()
	consensuspb.RegisterConsensusServer(grpcServer, rn)
	log.Printf("consensus node %s listening on port %s", *idFlag, *portFlag)

	// Inicia el servidor gRPC
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
