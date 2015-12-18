package meta

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

// raftState is a consensus strategy that uses a local raft implementation for
// consensus operations.
type raftState struct {
	wg        sync.WaitGroup
	config    *Config
	closing   chan struct{}
	raft      *raft.Raft
	transport *raft.NetworkTransport
	peerStore raft.PeerStore
	raftStore *raftboltdb.BoltStore
	raftLayer *raftLayer
	peers     []string
	ln        *net.Listner
	logger    *log.Logger
}

func newRaftState(c *Config, peers []string, ln *net.Listener, l *log.Logger) *raftState {
	return &raftState{
		config: c,
		peers:  peers,
		logger: l,
		ln:     ln,
	}
}

func (r *raftState) open() error {
	r.closing = make(chan struct{})

	// Setup raft configuration.
	config := raft.DefaultConfig()
	config.LogOutput = ioutil.Discard

	if s.clusterTracingEnabled {
		config.Logger = s.logger
	}
	config.HeartbeatTimeout = r.config.HeartbeatTimeout
	config.ElectionTimeout = r.config.ElectionTimeout
	config.LeaderLeaseTimeout = r.config.LeaderLeaseTimeout
	config.CommitTimeout = r.config.CommitTimeout
	// Since we actually never call `removePeer` this is safe.
	// If in the future we decide to call remove peer we have to re-evaluate how to handle this
	config.ShutdownOnRemove = false

	// If no peers are set in the config or there is one and we are it, then start as a single server.
	if len(s.peers) <= 1 {
		config.EnableSingleNode = true
		// Ensure we can always become the leader
		config.DisableBootstrapAfterElect = false
	}

	// Build raft layer to multiplex listener.
	r.raftLayer = newRaftLayer(r.ln, r.remoteAddr)

	// Create a transport layer
	r.transport = raft.NewNetworkTransport(r.raftLayer, 3, 10*time.Second, config.LogOutput)

	// Create peer storage.
	r.peerStore = raft.NewJSONPeers(s.path, r.transport)

	peers, err := r.peerStore.Peers()
	if err != nil {
		return err
	}

	// For single-node clusters, we can update the raft peers before we start the cluster if the hostname
	// has changed.
	if config.EnableSingleNode {
		if err := r.peerStore.SetPeers([]string{s.RemoteAddr.String()}); err != nil {
			return err
		}
		peers = []string{s.RemoteAddr.String()}
	}

	// If we have multiple nodes in the cluster, make sure our address is in the raft peers or
	// we won't be able to boot into the cluster because the other peers will reject our new hostname.  This
	// is difficult to resolve automatically because we need to have all the raft peers agree on the current members
	// of the cluster before we can change them.
	if len(peers) > 0 && !raft.PeerContained(peers, s.RemoteAddr.String()) {
		s.logger.Printf("%s is not in the list of raft peers. Please update %v/peers.json on all raft nodes to have the same contents.", s.RemoteAddr.String(), s.Path())
		return fmt.Errorf("peers out of sync: %v not in %v", s.RemoteAddr.String(), peers)
	}

	// Create the log store and stable store.
	store, err := raftboltdb.NewBoltStore(filepath.Join(s.path, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}
	r.raftStore = store

	// Create the snapshot store.
	snapshots, err := raft.NewFileSnapshotStore(s.path, raftSnapshotsRetained, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create raft log.
	ra, err := raft.NewRaft(config, (*storeFSM)(s), store, store, snapshots, r.peerStore, r.transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	r.raft = ra

	r.wg.Add(1)
	go r.logLeaderChanges()

	return nil
}

func (r *raftState) logLeaderChanges() {
	defer r.wg.Done()
	// Logs our current state (Node at 1.2.3.4:8088 [Follower])
	r.logger.Printf(r.raft.String())
	for {
		select {
		case <-r.closing:
			return
		case <-r.raft.LeaderCh():
			peers, err := r.peers()
			if err != nil {
				r.logger.Printf("failed to lookup peers: %v", err)
			}
			r.logger.Printf("%v. peers=%v", r.raft.String(), peers)
		}
	}
}

func (r *raftState) close() error {
	if r.closing != nil {
		close(r.closing)
	}
	r.wg.Wait()

	if r.transport != nil {
		r.transport.Close()
		r.transport = nil
	}

	// Shutdown raft.
	if r.raft != nil {
		if err := r.raft.Shutdown().Error(); err != nil {
			return err
		}
		r.raft = nil
	}

	if r.raftStore != nil {
		r.raftStore.Close()
		r.raftStore = nil
	}

	return nil
}

func (r *raftState) initialize() error {
	// If we have committed entries then the store is already in the cluster.
	if index, err := r.raftStore.LastIndex(); err != nil {
		return fmt.Errorf("last index: %s", err)
	} else if index > 0 {
		return nil
	}

	// Force set peers.
	if err := r.setPeers(r.peers); err != nil {
		return fmt.Errorf("set raft peers: %s", err)
	}

	return nil
}

// apply applies a serialized command to the raft log.
func (r *raftState) apply(b []byte) error {
	// Apply to raft log.
	f := r.raft.Apply(b, 0)
	if err := f.Error(); err != nil {
		return err
	}

	// Return response if it's an error.
	// No other non-nil objects should be returned.
	resp := f.Response()
	if err, ok := resp.(error); ok {
		return lookupError(err)
	}
	assert(resp == nil, "unexpected response: %#v", resp)

	return nil
}

func (r *raftState) lastIndex() uint64 {
	return r.raft.LastIndex()
}

func (r *raftState) snapshot() error {
	future := r.raft.Snapshot()
	return future.Error()
}

// addPeer adds addr to the list of peers in the cluster.
func (r *raftState) addPeer(addr string) error {
	peers, err := r.peerStore.Peers()
	if err != nil {
		return err
	}

	if len(peers) >= 3 {
		return nil
	}

	if fut := r.raft.AddPeer(addr); fut.Error() != nil {
		return fut.Error()
	}
	return nil
}

// removePeer removes addr from the list of peers in the cluster.
func (r *raftState) removePeer(addr string) error {
	// Only do this on the leader
	if !r.isLeader() {
		return errors.New("not the leader")
	}
	if fut := r.raft.RemovePeer(addr); fut.Error() != nil {
		return fut.Error()
	}
	return nil
}

// setPeers sets a list of peers in the cluster.
func (r *raftState) setPeers(addrs []string) error {
	return r.raft.SetPeers(addrs).Error()
}

func (r *raftState) peers() ([]string, error) {
	return r.peerStore.Peers()
}

func (r *raftState) leader() string {
	if r.raft == nil {
		return ""
	}

	return r.raft.Leader()
}

func (r *raftState) isLeader() bool {
	if r.raft == nil {
		return false
	}
	return r.raft.State() == raft.Leader
}

// raftLayer wraps the connection so it can be re-used for forwarding.
type raftLayer struct {
	ln     net.Listener
	addr   net.Addr
	conn   chan net.Conn
	closed chan struct{}
}

// newRaftLayer returns a new instance of raftLayer.
func newRaftLayer(ln net.Listener, addr net.Addr) *raftLayer {
	return &raftLayer{
		ln:     ln,
		addr:   addr,
		conn:   make(chan net.Conn),
		closed: make(chan struct{}),
	}
}

// Addr returns the local address for the layer.
func (l *raftLayer) Addr() net.Addr { return l.addr }

// Dial creates a new network connection.
func (l *raftLayer) Dial(addr string, timeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}

	// Write a marker byte for raft messages.
	_, err = conn.Write([]byte{MuxRaftHeader})
	if err != nil {
		conn.Close()
		return nil, err
	}
	return conn, err
}

// Accept waits for the next connection.
func (l *raftLayer) Accept() (net.Conn, error) { return l.ln.Accept() }

// Close closes the layer.
func (l *raftLayer) Close() error { return l.ln.Close() }