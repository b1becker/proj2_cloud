package pkg

import (
	// "context"
	// "sort"
	"strconv"
	"strings"
	// "time"
)

// doLeader implements the logic for a Raft node in the leader state.
// doLeader implements the logic for a Raft node in the leader state.
func (n *Node) doLeader() stateFunction {
	n.Out("Transitioning to LeaderState")
	n.setState(LeaderState)

	n.nextIndex = make(map[string]uint64)
	n.matchIndex = make(map[string]uint64)
	for _, peer := range n.getPeers() {
		if peer.GetId() == n.Self.GetId() {
			continue
		}
		n.nextIndex[peer.GetId()] = n.LastLogIndex() + 1 // before the NOOP is appended
		n.matchIndex[peer.GetId()] = 0
	}

	// new leader appends a NOOP for its term
	n.StoreLog(&LogEntry{
		Index:  n.LastLogIndex() + 1,
		TermId: n.GetCurrentTerm(),
		Type:   CommandType_NOOP,
	})

	// initial heartbeat right after winning the election
	if n.sendHeartbeats() {
		n.failPendingRequests()
		return n.doFollower
	}

	heartbeatTimer := randomTimeout(n.Config.HeartbeatTimeout)

	for {
		select {
		case shutdown := <-n.gracefulExit:
			if shutdown {
				return nil
			}

		case <-heartbeatTimer:
			if n.sendHeartbeats() {
				n.failPendingRequests()
				return n.doFollower
			}
			heartbeatTimer = randomTimeout(n.Config.HeartbeatTimeout)

		case msg := <-n.appendEntries:
			// handleAppendEntries returns (resetTimeout, fallback)
			_, fallback := n.handleAppendEntries(msg)
			if fallback {
				n.failPendingRequests()
				return n.doFollower
			}

		case msg := <-n.requestVote:
			if n.handleRequestVote(msg) {
				n.failPendingRequests()
				return n.doFollower
			}

		case msg := <-n.clientRequest:
			var entry *LogEntry
			var key string
			idx := n.LastLogIndex() + 1

			if msg.request == nil || (msg.request.ClientId == 0 && msg.request.SequenceNum == 0) {
				// client registration
				key = "register-" + strconv.FormatUint(idx, 10)
				entry = &LogEntry{
					Index:   idx,
					TermId:  n.GetCurrentTerm(),
					Type:    CommandType_CLIENT_REGISTRATION,
					CacheId: key,
				}
			} else {
				// state machine command
				key = CreateCacheID(msg.request.ClientId, msg.request.SequenceNum)
				entry = &LogEntry{
					Index:   idx,
					TermId:  n.GetCurrentTerm(),
					Type:    CommandType_STATE_MACHINE_COMMAND,
					Command: msg.request.StateMachineCmd,
					Data:    msg.request.Data,
					CacheId: key,
				}
			}

			// register the reply channel BEFORE storing the entry
			n.requestsMutex.Lock()
			n.requestsByCacheID[key] = append(n.requestsByCacheID[key], msg.reply)
			n.requestsMutex.Unlock()

			n.StoreLog(entry)
			if n.sendHeartbeats() {
				n.failPendingRequests()
				return n.doFollower
			}
		}
	}
}

// failPendingRequests answers every waiting client with NOT_LEADER when we lose leadership.
func (n *Node) failPendingRequests() {
	n.requestsMutex.Lock()
	defer n.requestsMutex.Unlock()
	for id, chans := range n.requestsByCacheID {
		for _, ch := range chans {
			ch <- ClientReply{Status: ClientStatus_NOT_LEADER}
		}
		delete(n.requestsByCacheID, id)
	}
}

// sendHeartbeats is used by the leader to send out heartbeats to each of
// the other nodes. It returns true if the leader should fall back to the
// follower state. (This happens if we discover that we are in an old term.)
//
// If another node isn't up-to-date, then the leader should attempt to
// update them, and, if an index has made it to a quorum of nodes, commit
// up to that index. Once committed to that index, the replicated state
// machine should be given the new log entries via processLogEntry.
func (n *Node) sendHeartbeats() (fallback bool) {
	term := n.GetCurrentTerm()
	lastIndex := n.LastLogIndex()

	for _, peer := range n.getPeers() {
		id := peer.GetId()
		if id == n.Self.GetId() {
			continue
		}

		for {
			// keep nextIndex inside the valid range
			if n.nextIndex[id] == 0 || n.nextIndex[id] > lastIndex+1 {
				n.nextIndex[id] = lastIndex + 1
			}
			next := n.nextIndex[id]
			prevLogIndex := next - 1

			var prevLogTerm uint64
			if prevLogIndex > 0 {
				prev := n.GetLog(prevLogIndex)
				if prev == nil {
					break
				}
				prevLogTerm = prev.TermId
			}

			entries := []*LogEntry{}
			for i := next; i <= lastIndex; i++ {
				entries = append(entries, n.GetLog(i))
			}

			request := AppendEntriesRequest{
				Term:         term,
				Leader:       n.Self,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: n.CommitIndex.Load(),
			}

			reply, err := peer.AppendEntriesRPC(n, &request)
			if err != nil {
				break // unreachable: try again next heartbeat
			}
			if reply.Term > term {
				n.SetCurrentTerm(reply.Term)
				n.setVotedFor("")
				return true
			}
			if reply.Success {
				n.matchIndex[id] = prevLogIndex + uint64(len(entries))
				n.nextIndex[id] = n.matchIndex[id] + 1
				break
			}
			if next <= 1 {
				break
			}
			n.nextIndex[id] = next - 1 // log mismatch: back up and retry right away
		}
	}

	// commit once, after all peers have been handled
	majority := len(n.getPeers())/2 + 1
	for i := lastIndex; i > n.CommitIndex.Load(); i-- {
		if n.GetLog(i).TermId != term {
			continue
		}
		count := 1 // the leader itself
		for _, m := range n.matchIndex {
			if m >= i {
				count++
			}
		}
		if count >= majority {
			n.CommitIndex.Store(i)
			for n.LastApplied.Load() < i {
				n.processLogEntry(n.LastApplied.Inc())
			}
			break
		}
	}
	return false
}

// processLogEntry applies a single log entry to the finite state machine. It is
// called once a log entry has been replicated to a majority and committed by
// the leader. Once the entry has been applied, the leader responds to the client
// with the result, and also caches the response.
func (n *Node) processLogEntry(logIndex uint64) (fallback bool) {
	fallback = false
	entry := n.GetLog(logIndex)
	n.Out("Processing log index: %v, entry: %v", logIndex, entry)

	status := ClientStatus_OK
	var response []byte
	var err error
	var clientId uint64

	switch entry.Type {
	case CommandType_NOOP:
		return
	case CommandType_CLIENT_REGISTRATION:
		clientId = logIndex
	case CommandType_STATE_MACHINE_COMMAND:
		if clientId, err = strconv.ParseUint(strings.Split(entry.GetCacheId(), "-")[0], 10, 64); err != nil {
			panic(err)
		}
		if resp, ok := n.GetCachedReply(entry.GetCacheId()); ok {
			status = resp.GetStatus()
			response = resp.GetResponse()
		} else {
			response, err = n.StateMachine.ApplyCommand(entry.Command, entry.Data)
			if err != nil {
				status = ClientStatus_REQ_FAILED
				response = []byte(err.Error())
			}
		}
	}

	// Construct reply
	reply := ClientReply{
		Status:     status,
		ClientId:   clientId,
		Response:   response,
		LeaderHint: &RemoteNode{Addr: n.Self.Addr, Id: n.Self.Id},
	}

	// Send reply to client
	n.requestsMutex.Lock()
	defer n.requestsMutex.Unlock()
	// Add reply to cache
	if entry.CacheId != "" {
		if err = n.CacheClientReply(entry.CacheId, reply); err != nil {
			panic(err)
		}
	}
	if replies, exists := n.requestsByCacheID[entry.CacheId]; exists {
		for _, ch := range replies {
			ch <- reply
		}
		delete(n.requestsByCacheID, entry.CacheId)
	}

	return
}
