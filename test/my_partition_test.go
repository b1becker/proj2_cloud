package test

import (
	"bytes"
	raft "raft/pkg"
	"testing"
	"time"
)

func s311Link(a, b []*raft.Node, up bool) {
	for _, x := range a {
		for _, y := range b {
			x.NetworkPolicy.RegisterPolicy(x.Self, y.Self, up)
			y.NetworkPolicy.RegisterPolicy(y.Self, x.Self, up)
		}
	}
}

func s311Dump(t *testing.T, label string, cluster []*raft.Node) {
	t.Logf("--- %s", label)
	for _, n := range cluster {
		t.Logf("%s state=%v term=%d lastLog=%d commit=%d",
			n.Self.Addr, n.GetState(), n.GetCurrentTerm(), n.LastLogIndex(), n.CommitIndex.Load())
	}
}

func s311Append(n *raft.Node, data []byte) {
	n.LeaderMutex.Lock()
	defer n.LeaderMutex.Unlock()
	n.StoreLog(&raft.LogEntry{
		Index:  n.LastLogIndex() + 1,
		TermId: n.GetCurrentTerm(),
		Type:   raft.CommandType_NOOP,
		Data:   data,
	})
}

func s311Has(n *raft.Node, data []byte) bool {
	for i := uint64(1); i <= n.LastLogIndex(); i++ {
		if e := n.GetLog(i); e != nil && bytes.Equal(e.Data, data) {
			return true
		}
	}
	return false
}

func s311Run(t *testing.T, ports []int, leaderIsolated bool) {
	suppressLoggers()
	cluster, err := createTestCluster(ports)
	defer cleanupCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second * WaitPeriod)
	oldLeader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}
	followers := []*raft.Node{}
	for _, n := range cluster {
		if n != oldLeader {
			followers = append(followers, n)
		}
	}

	var majority []*raft.Node
	var lone1, lone2 *raft.Node
	if leaderIsolated {
		majority, lone1, lone2 = followers[:3], oldLeader, followers[3]
	} else {
		majority = []*raft.Node{oldLeader, followers[0], followers[1]}
		lone1, lone2 = followers[2], followers[3]
	}

	s311Link(majority, []*raft.Node{lone1}, false)
	s311Link(majority, []*raft.Node{lone2}, false)
	s311Link([]*raft.Node{lone1}, []*raft.Node{lone2}, false)
	time.Sleep(time.Second * WaitPeriod)
	s311Dump(t, "after partition", cluster)

	majLeader, err := findLeader(majority)
	if err != nil {
		t.Fatalf("3-node group has no leader: %v", err)
	}
	majMarker, oldMarker := []byte{5, 6, 7, 8}, []byte{1, 2, 3, 4}
	s311Append(majLeader, majMarker)
	if leaderIsolated {
		s311Append(oldLeader, oldMarker) // must never commit
	}
	time.Sleep(time.Second * WaitPeriod)
	s311Dump(t, "before heal", cluster)

	// staggered heal: first lone node rejoins, then the second
	s311Link(majority, []*raft.Node{lone1}, true)
	time.Sleep(time.Second * WaitPeriod)
	s311Dump(t, "after first rejoin", cluster)

	s311Link(majority, []*raft.Node{lone2}, true)
	s311Link([]*raft.Node{lone1}, []*raft.Node{lone2}, true)
	time.Sleep(time.Second * WaitPeriod)
	s311Dump(t, "after second rejoin", cluster)

	leaders := 0
	for _, n := range cluster {
		if n.GetState() == raft.LeaderState {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", leaders)
	}
	leader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}
	if !logsMatch(leader, cluster) {
		t.Errorf("logs do not match after heal")
	}
	if !s311Has(leader, majMarker) {
		t.Errorf("majority's entry was lost")
	}
	if leaderIsolated && s311Has(leader, oldMarker) {
		t.Errorf("deposed leader's uncommitted entry survived")
	}
}

func TestMy311LeaderInMajority(t *testing.T) {
	s311Run(t, []int{5401, 5402, 5403, 5404, 5405}, false)
}

func TestMy311LeaderIsolated(t *testing.T) {
	s311Run(t, []int{5501, 5502, 5503, 5504, 5505}, true)
}