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

	fallback, electionWon := n.requestVotes(curr_term)

	if fallback {
		return n.doFollower
	}

	if electionWon {
		n.Out("the current node has won the election")
		return n.doLeader
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

	

	return n.doCandidate
}

// requestVotes is called to request votes from all other nodes. It takes in a
// channel on which the result of the vote should be sent over: true for a
// successful election, false otherwise.

// helper for doCandidate
func (n *Node) requestVotes(currTerm uint64) (fallback, electionResult bool) {
    peers := n.getPeers()

    votes := 1 // We already voted for ourselves

    for _, peer := range peers {
        
        if peer.Id == n.Self.Id {
            continue
        }

        request := RequestVoteRequest{
            Term:         currTerm,
            Candidate:    n.Self,
            LastLogIndex: n.LastLogIndex(),
        }

        reply, err := peer.RequestVoteRPC(n, &request)
        if err != nil {
            continue
        }

        // A peer with a higher term means we are no longer up to date
        if reply.Term > currTerm {
            n.SetCurrentTerm(reply.Term)
            n.setVotedFor("")
            return true, false
        }

        if reply.VoteGranted {
            votes++
        }
    }

    majority := len(peers) / 2 + 1
    return false, votes >= majority
}

// handleRequestVote handles an incoming vote request. It returns true if the caller
// should fall back to the follower state, false otherwise.
func (n *Node) handleRequestVote(msg RequestVoteMsg) (fallback bool) {
	// TODO: Students should implement this method
	request := msg.request
	request_term := request.Term
	curr_term := n.GetCurrentTerm()



	if request_term < curr_term {
		msg.reply <- RequestVoteReply{
			Term:        curr_term,
			VoteGranted: false,
		}
		return false
	}

	if request_term > curr_term {
		n.SetCurrentTerm(request_term)
		n.setVotedFor("")
		return true
	}

	votedFor := n.GetVotedFor()
	if votedFor == "" || votedFor == request.Candidate.Id {
		msg.reply <- RequestVoteReply{
			Term: curr_term,
			VoteGranted: false,
		}
		return false

	}

	// this checks if a potential candidate's log is up to date
	localLastLogIndex := n.LastLogIndex()
	localLastLogTerm := n.GetLog(localLastLogIndex).TermId

	logIsUpToDate := request.LastLogTerm > localLastLogTerm || (request.LastLogTerm == localLastLogTerm && request.LastLogIndex >= localLastLogIndex)

	if !logIsUpToDate {
		msg.reply <- RequestVoteReply{
			Term: curr_term,
			VoteGranted: false,
		}
		return false
	}

	// if passes all checks set the vote
	n.setVotedFor(request.Candidate.Id)

	msg.reply <- RequestVoteReply{
		Term: curr_term,
		VoteGranted: true,
	}
	
	return false
}
