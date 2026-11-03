package pkg



// doCandidate implements the logic for a Raft node in the candidate state.
func (n *Node) doCandidate() stateFunction {
	n.Out("Transitioning to CANDIDATE_STATE")
	n.setState(CandidateState)

	// TODO: Students should implement this method
	// Hint: perform any initial work, and then consider what a node in the
	// candidate state should do when it receives an incoming message on every
	// possible channel.

	// following state, get curr term, increment
	curr_term := n.GetCurrentTerm()
	curr_term++
	n.SetCurrentTerm(curr_term)

	ownId := n.Self.GetId()
	n.setVotedFor(ownId)

	timer := randomTimeout(n.Config.ElectionTimeout)

	// CHANGED: requestVotes now returns a channel of replies instead of blocking
	voteCh := n.requestVotes(curr_term)

	// NEW: the vote tally moved here since replies arrive inside the loop
	votes := 1
	majority := (len(n.getPeers()))/2 + 1
	if votes >= majority {
		n.Out("the current node has won the election")
		return n.doLeader
	}

	// i learned that for loop is necessary to keep node in candidate state
	// at the bottom of the loop i had .doleader so it would automatically put the candidate to a leader after failing checks
	for {

	select {
	case <-timer:
		n.Out("Election timeout, starting new election")
		return n.doCandidate

	// NEW: handles vote replies as they come in
	case reply := <-voteCh:
		// A peer with a higher term means we are no longer up to date
		if reply.Term > n.GetCurrentTerm() {
			n.SetCurrentTerm(reply.Term)
			n.setVotedFor("")
			return n.doFollower
		}
		if reply.Term == curr_term && reply.VoteGranted {
			votes++
			if votes >= majority {
				n.Out("the current node has won the election")
				return n.doLeader
			}
		}

	// handles the incoming append entries messages
	case msg := <-n.appendEntries:
		fallback, _ := n.handleAppendEntries(msg)
		if fallback {
			return n.doFollower
		}

	// evaluates the incoming vote requests
	case msg := <-n.requestVote:
		fallback := n.handleRequestVote(msg)
		if fallback {
			return n.doFollower
		}
	case shutdown := <-n.gracefulExit:
		if shutdown {
			return nil
		}
	
	}

	

}

}

// requestVotes is called to request votes from all other nodes. It takes in a
// channel on which the result of the vote should be sent over: true for a
// successful election, false otherwise.

// helper for doCandidate
// CHANGED: sends each RPC in its own goroutine and returns the reply channel
func (n *Node) requestVotes(currTerm uint64) chan *RequestVoteReply {
    peers := n.getPeers()

    // buffered so goroutines never block (or leak) if the candidate has moved on
    voteCh := make(chan *RequestVoteReply, len(peers))

    var lastLogTerm uint64
    lastLogIndex := n.LastLogIndex()
    if lastLogIndex == 0 {
        lastLogTerm = 0
    } else {
        lastLogTerm = n.GetLog(lastLogIndex).TermId
    }

    for _, peer := range peers {

        if peer.Id == n.Self.Id {
            continue
        }

        request := RequestVoteRequest{
            Term:         currTerm,
            Candidate:    n.Self,
            LastLogIndex: lastLogIndex,
            LastLogTerm:  lastLogTerm,
        }

        go func(p *RemoteNode, req RequestVoteRequest) {
            reply, err := p.RequestVoteRPC(n, &req)
            if err != nil {
                return
            }
            voteCh <- reply
        }(peer, request)
    }

    return voteCh
}

// handleRequestVote handles an incoming vote request. It returns true if the caller
// should fall back to the follower state, false otherwise.
func (n *Node) handleRequestVote(msg RequestVoteMsg) (fallback bool) {
	// TODO: Students should implement this method
	request := msg.request
	request_term := request.Term
	curr_term := n.GetCurrentTerm()


	// old terms are rejected by default
	if request_term < curr_term {
		msg.reply <- RequestVoteReply{
			Term: curr_term,
			VoteGranted: false,
		}
		return false
	}

	higherTerm := request_term > curr_term

	if higherTerm{
		n.SetCurrentTerm(request_term)
		n.setVotedFor("")
		curr_term = request_term
	}

	// if voted for another canditate reject
	votedFor := n.GetVotedFor()
	if votedFor != "" && votedFor != request.Candidate.Id {
		msg.reply <- RequestVoteReply{
			Term: curr_term,
			VoteGranted: false,
		}
		return higherTerm
	}

	// this checks if a potential candidate's log is up to date
	localLastLogIndex := n.LastLogIndex()
	localLastLogTerm := uint64(0)

	if localLastLogIndex > 0 {
        localLastLogTerm = n.GetLog(localLastLogIndex).TermId
    }

	logIsUpToDate := request.LastLogTerm > localLastLogTerm || (request.LastLogTerm == localLastLogTerm && request.LastLogIndex >= localLastLogIndex)

	if !logIsUpToDate {
		msg.reply <- RequestVoteReply{
			Term: curr_term,
			VoteGranted: false,
		}
		return higherTerm
	}

	// if passes all checks set the vote
	n.setVotedFor(request.Candidate.Id)

	msg.reply <- RequestVoteReply{
		Term: curr_term,
		VoteGranted: true,
	}
	
	return higherTerm
}
