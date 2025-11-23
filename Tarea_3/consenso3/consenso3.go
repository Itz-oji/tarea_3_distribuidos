package main

import (
	"context"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	proto "heint/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

type LogEntry struct {
	Term    int32
	Command string
}

type Node struct {
	proto.UnimplementedConsensusServer

	id    string
	peers []string
	role  int

	currentTerm int32
	votedFor    string
	log         []LogEntry

	commitIndex   int
	leaderId      string
	mutex         sync.Mutex
	lastHeartbeat time.Time
}

func newNode(id string, peers []string) *Node {
	return &Node{
		id:            id,
		peers:         peers,
		role:          FOLLOWER,
		currentTerm:   0,
		votedFor:      "",
		log:           make([]LogEntry, 0),
		commitIndex:   -1,
		leaderId:      "",
		lastHeartbeat: time.Now(),
	}
}

func (n *Node) RequestVote(ctx context.Context, req *proto.VoteRequest) (*proto.VoteResponse, error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	if req.Term < n.currentTerm {
		return &proto.VoteResponse{Term: n.currentTerm, VoteGranted: false}, nil
	}

	// Nueva elección
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = ""
		n.role = FOLLOWER
	}

	// Si no he votado o voto por el candidato
	if n.votedFor == "" || n.votedFor == req.CandidateId {
		n.votedFor = req.CandidateId
		return &proto.VoteResponse{Term: n.currentTerm, VoteGranted: true}, nil
	}

	return &proto.VoteResponse{Term: n.currentTerm, VoteGranted: false}, nil
}

func (n *Node) AppendEntries(ctx context.Context, req *proto.AppendRequest) (*proto.AppendResponse, error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	if req.Term < n.currentTerm {
		return &proto.AppendResponse{Term: n.currentTerm, Success: false}, nil
	}

	n.role = FOLLOWER
	n.currentTerm = req.Term
	n.leaderId = req.LeaderId
	n.lastHeartbeat = time.Now()

	// append entries
	for _, e := range req.Entries {
		n.log = append(n.log, LogEntry{Term: e.Term, Command: e.Command})
	}

	return &proto.AppendResponse{
		Term:       n.currentTerm,
		Success:    true,
		MatchIndex: int32(len(n.log) - 1),
	}, nil
}

func (n *Node) Propose(ctx context.Context, req *proto.Proposal) (*proto.ConsensusReply, error) {
	n.mutex.Lock()
	if n.role != LEADER {
		leader := n.leaderId
		if leader == "" {
			leader = "UNKNOWN"
		}
		n.mutex.Unlock()
		return &proto.ConsensusReply{Success: false, Result: "Not Leader", LeaderId: leader}, nil
	}

	entry := LogEntry{Term: n.currentTerm, Command: req.Command}
	n.log = append(n.log, entry)
	index := len(n.log) - 1
	n.mutex.Unlock()

	commitCount := 1 // líder

	var wg sync.WaitGroup
	for _, peer := range n.peers {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			if replicateToPeerAndConfirm(p, entry, index, n.currentTerm) {
				commitCount++
			}
		}(peer)
	}
	wg.Wait()

	if commitCount >= (len(n.peers)+1)/2+1 {
		n.mutex.Lock()
		n.commitIndex = index

		log.Printf("[%s] CONSENSO ALCANZADO → Comando aplicado: %s (Término %d)", n.id, req.Command, n.currentTerm)

		n.mutex.Unlock()
		return &proto.ConsensusReply{Success: true, Result: "Committed"}, nil
	}
	return &proto.ConsensusReply{Success: false, Result: "No quorum"}, nil
}

func replicateToPeerAndConfirm(peer string, entry LogEntry, index int, term int32) bool {
	conn, err := grpc.Dial(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return false
	}
	defer conn.Close()

	client := proto.NewConsensusClient(conn)
	ctxT, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.AppendEntries(ctxT, &proto.AppendRequest{
		Term:         term,
		Entries:      []*proto.LogEntry{{Term: entry.Term, Command: entry.Command}},
		PrevLogIndex: int32(index - 1),
		PrevLogTerm:  entry.Term,
	})
	return err == nil && resp.Success
}

func askVote(peer, candidateID string, term int32) bool {
	conn, err := grpc.Dial(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return false
	}
	defer conn.Close()

	client := proto.NewConsensusClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.RequestVote(ctx, &proto.VoteRequest{
		Term:         term,
		CandidateId:  candidateID,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if err != nil {
		return false
	}
	return resp.GetVoteGranted()
}

func (n *Node) startElectionLoop() {
	go func() {
		// timeout base entre 300 y 500 ms
		electionTimeout := func() time.Duration {
			return time.Duration(300+rand.Intn(200)) * time.Millisecond
		}()

		for {
			time.Sleep(50 * time.Millisecond)

			n.mutex.Lock()
			role := n.role
			sinceLastHB := time.Since(n.lastHeartbeat)
			n.mutex.Unlock()

			// Sólo followers y candidatos pueden iniciar elección
			if role == LEADER {
				continue
			}

			if sinceLastHB >= electionTimeout {
				// inicia una ronda de elección
				n.startElectionRound()

				// nuevo timeout aleatorio para evitar colisiones
				electionTimeout = time.Duration(300+rand.Intn(200)) * time.Millisecond
			}
		}
	}()
}

func (n *Node) startElectionRound() {
	n.mutex.Lock()
	// Nos volvemos candidato
	n.role = CANDIDATE
	n.currentTerm++
	term := n.currentTerm
	n.votedFor = n.id
	n.lastHeartbeat = time.Now() // marcamos inicio de la elección
	n.mutex.Unlock()

	votes := 1
	voteCh := make(chan bool, len(n.peers))

	for _, peer := range n.peers {
		go func(p string) {
			voteCh <- askVote(p, n.id, term)
		}(peer)
	}

	timeout := time.After(200 * time.Millisecond)
	needed := (len(n.peers)+1)/2 + 1

	for received := 0; received < len(n.peers); received++ {
		select {
		case granted := <-voteCh:
			if granted {
				votes++
				if votes >= needed {
					n.mutex.Lock()
					// Si no nos han "bajado" de término mientras tanto
					if n.currentTerm == term {
						n.role = LEADER
						n.leaderId = n.id
						log.Printf("[%s] NUEVO LÍDER (término %d)", n.id, term)
						n.mutex.Unlock()
						go n.startHeartbeats()
						return
					}
					n.mutex.Unlock()
					return
				}
			}
		case <-timeout:
			// No se logró mayoría en el tiempo dado, se abandona esta ronda
			return
		}
	}
}

func (n *Node) startHeartbeats() {
	for n.role == LEADER {
		for _, peer := range n.peers {
			go func(p string) {
				conn, err := grpc.Dial(p, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					return
				}
				defer conn.Close()

				client := proto.NewConsensusClient(conn)
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				defer cancel()

				client.AppendEntries(ctx, &proto.AppendRequest{
					Term:     n.currentTerm,
					LeaderId: n.id,
					Entries:  []*proto.LogEntry{}, // heartbeat vacío
				})
			}(peer)
		}
		time.Sleep(150 * time.Millisecond)
	}
}

func main() {
	id := "consenso3"
	peers := []string{
		"consenso1:57770",
		"consenso2:57771",
	}

	node := newNode(id, peers)
	node.startElectionLoop()

	lis, err := net.Listen("tcp", ":57772")
	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer()
	proto.RegisterConsensusServer(grpcServer, node)

	log.Println("Nodo consenso", id, "escuchando en :57772")
	grpcServer.Serve(lis)
}
