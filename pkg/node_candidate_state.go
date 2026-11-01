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
	n.SetCurrentTerm(curr_term + 1)

	ownId := n.Self.GetId()
	n.setVotedFor(ownId)

	timer := randomTimeout(n.Config.ElectionTimeout)

	// send request to each individual node
	for peer, _ := range n.Peers {
		if peer == ownId {
			continue
		}

		request := RequestVoteRequest{
			Term:         n.GetCurrentTerm(),
			Candidate:    ownId,
			LastLogIndex: n.LastLogIndex(),
			LastLogTerm:  n.LastLogTerm(),
		}
		reply, err := n.sendRequestVote(peer, request)
		if err != nil {
			n.Out("Error sending request vote to peer %v: %v", peer, err)
			continue
		}
		if reply.VoteGranted {
			n.Out("Received vote from peer %v", peer)
		}
	}

	select {
	case <-timer:
		n.Out("Election timeout, starting new election")
		return n.doCandidate

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
	}

	return nil
}

// requestVotes is called to request votes from all other nodes. It takes in a
// channel on which the result of the vote should be sent over: true for a
// successful election, false otherwise.
func (n *Node) requestVotes(currTerm uint64) (fallback, electionResult bool) {
	// TODO: Students should implement this method
	return true, true
}

// handleRequestVote handles an incoming vote request. It returns true if the caller
// should fall back to the follower state, false otherwise.
func (n *Node) handleRequestVote(msg RequestVoteMsg) (fallback bool) {
	// TODO: Students should implement this method
	return true
}
