package pkg

import (
	// "context"
	// "sort"
	"strconv"
	"strings"
	// "time"
)

// doLeader implements the logic for a Raft node in the leader state.
func (n *Node) doLeader() stateFunction {
	n.Out("Transitioning to LeaderState")
	n.setState(LeaderState)

	// peers := n.getPeers()
	_ = n.getPeers()
	// TODO: Students should implement this method
	// Hint: perform any initial work, and then consider what a node in the
	// leader state should do when it receives an incoming message on every
	// possible channel.
	n.nextIndex = make(map[string]uint64)
	n.matchIndex = make(map[string]uint64)
	
	for _, peer := range n.getPeers() {
		if peer.GetId() == n.Self.GetId() {
			continue
		}
		n.nextIndex[peer.GetId()] = n.LastLogIndex() + 1
		n.matchIndex[peer.GetId()] = 0
	}

	heartbeat_timer := randomTimeout(n.Config.HeartbeatTimeout)

	for {
		select {
		case shutdown := <-n.gracefulExit:
			if shutdown {
				return nil
			}

		case <- heartbeat_timer:
			fallback := n.sendHeartbeats()

			if fallback {
				return n.doFollower
			}
			heartbeat_timer = randomTimeout(n.Config.HeartbeatTimeout)


		
		case msg := <-n.appendEntries:
			fallback, _ := n.handleAppendEntries(msg)
			if fallback {
				return n.doFollower
			}

		case msg := <-n.requestVote: 
			fallback := n.handleRequestVote(msg)
			if fallback {
				return n.doFollower
			}
			

		case msg := <-n.clientRequest:
			// msg.reply <- ClientReply{
			// 	Status: ClientStatus_NOT_LEADER}

			cacheId := strconv.FormatUint(msg.request.ClientId, 10) + "-" +
        strconv.FormatUint(msg.request.SequenceNum, 10)

			
			// append to leader's log
			logEntry := &LogEntry{
				Index: n.LastLogIndex() + 1,
				TermId: n.GetCurrentTerm(),
				Type: CommandType_STATE_MACHINE_COMMAND,
				Command: msg.request.StateMachineCmd,
				Data: msg.request.Data,
				CacheId: cacheId,
			}
			n.StoreLog(logEntry)

			// send reply to client, entering critical section
			n.requestsMutex.Lock()
			n.requestsByCacheID[cacheId] = append(n.requestsByCacheID[cacheId], msg.reply)
			n.requestsMutex.Unlock()

			n.StoreLog(logEntry)
			fallback := n.sendHeartbeats()
			if fallback {
				return n.doFollower
			}
		
		}
		


	}
	return nil
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
	// TODO: Students should implement this method
	term := n.GetCurrentTerm()

	for _, peer := range n.getPeers() {
		if peer.GetId() == n.Self.GetId() {
			continue
		}

		prevLogIndex := n.nextIndex[peer.GetId()] - 1

		var prevLogTerm uint64

		if prevLogIndex > 0 {
			prevlog := n.GetLog(prevLogIndex)
			if prevlog == nil {
				panic("prevLogIndex is greater than last log index")
			} else {
				prevLogTerm = prevlog.TermId
			}
			
		}
		
		entries := []*LogEntry{}

		if n.LastLogIndex() >= n.nextIndex[peer.GetId()] {
			for i := n.nextIndex[peer.GetId()]; i <= n.LastLogIndex(); i++ {
				entries = append(entries, n.GetLog(i))
			}
		}

		request := AppendEntriesRequest{
			Term:         term,
			Leader:     n.Self,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: n.CommitIndex.Load(),
		}

		reply, err := peer.AppendEntriesRPC(n, &request)
		if err != nil {
			continue
		}

		if reply.Term > term {
			n.SetCurrentTerm(reply.Term)
			n.setVotedFor("")
			return true
		}

		if reply.Success {
			n.nextIndex[peer.GetId()] = n.LastLogIndex() + 1
			n.matchIndex[peer.GetId()] = n.LastLogIndex()
		} else if n.nextIndex[peer.GetId()] > 1 {
			// NOTE THIS IS an extension and some guard against unsigned integer underflow
			n.nextIndex[peer.GetId()]--
		}
		
		old_commit_index := n.CommitIndex.Load()
		for i := n.LastLogIndex(); i > old_commit_index; i-- {
			count := 1 // count itself
			if n.GetLog(i).TermId != term {
				continue
			}

			for _, peer := range n.getPeers() {
				if peer.GetId() == n.Self.GetId() {
					continue
				}

				if n.matchIndex[peer.GetId()] >= i {
					count++
				}
			}

			majority := ((len(n.getPeers()) + 1) / 2) + 1
			if count >= majority {
				n.CommitIndex.Store(i)
				for n.LastApplied.Load() < n.CommitIndex.Load() {
					n.processLogEntry(n.LastApplied.Inc())
				}
				break
			}
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
