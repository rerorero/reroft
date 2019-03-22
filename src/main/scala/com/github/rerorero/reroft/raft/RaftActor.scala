package com.github.rerorero.reroft.raft

import akka.actor.{ActorRef, Cancellable, LoggingFSM, Props}
import com.github.rerorero.reroft.fsm.{ApplyAsync, Initialize}
import com.github.rerorero.reroft.log.{LogRepository, LogEntry => LogRepoEntry}
import com.github.rerorero.reroft._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Random

// RaftActor state
case class RaftState(
  // latest term server has seen (initialized to 0 on first boot, increases monotonically)
  currentTerm: Long,
  // candidateId that received vote in current term (or null if none)
  votedFor: Option[NodeID],
  leaderID: Option[NodeID],
  granted: Set[NodeID],
  // nextIndex[] for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
  nextIndex: Map[NodeID, Long],
  // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
  // this prop also used as flag which indicates whether actor is waiting logAppendEntries response or not
  matchIndex: Option[Map[NodeID, Long]],
) {
  def clearElectionState: RaftState = copy(
    votedFor = None,
    leaderID = None,
    granted = Set.empty,
    nextIndex = Map.empty,
    matchIndex = None,
  )
}

object RaftState {
  def empty = RaftState(
    currentTerm = 0L,
    votedFor = None,
    leaderID = None,
    granted = Set.empty,
    nextIndex = Map.empty,
    matchIndex = None,
  )
}

// Role is server state
sealed trait Role
case object Follower extends Role
case object Candidate extends Role
case object Leader extends Role

// event
case object ElectionTimeout
case object StartElection
case class BroadcastAppendLog(sendEmpty: Boolean)

class RaftActor(
  val stateMachine: ActorRef,
  val logRepo: LogRepository,
  val clusterNodes: Set[Node],
  val myID: NodeID,
  val minElectionTimeoutMS: Int = 150,
  val maxElectionTimeoutMS: Int = 300,
  val heartbeatIntervalMS: Int = 15,
) extends LoggingFSM[Role, RaftState] {
  implicit val executionContext: ExecutionContext = context.system.dispatcher

  private[this] val otherNodes = clusterNodes.filter(_.id == myID)
  private[this] var electionTicker: Option[Cancellable] = None
  private[this] var heartbeatTicker: Option[Cancellable] = None

  def resetElectionTicker(): Unit = {
    electionTicker.foreach(_.cancel())
    electionTicker = Some(context.system.scheduler.scheduleOnce(
      Duration(minElectionTimeoutMS + Random.nextInt(maxElectionTimeoutMS - minElectionTimeoutMS), MILLISECONDS),
      self,
      ElectionTimeout
    ))
  }

  def stopElectionTicker(): Unit = {
    electionTicker.foreach(_.cancel())
    electionTicker = None
  }

  def startHeartbeat(): Unit = if (heartbeatTicker.isEmpty) {
    heartbeatTicker = Some(context.system.scheduler.schedule(
      0 millisecond,
      Duration(heartbeatIntervalMS, MILLISECONDS),
      self,
      BroadcastAppendLog,
    ))
  }

  def stopHeartbeat(): Unit = heartbeatTicker.foreach { cancellable =>
    cancellable.cancel()
    heartbeatTicker = None
  }

  def nodeOf(id: NodeID) = clusterNodes.filter(_.id == id).headOption.getOrElse(throw new Exception(s"no such id in clusterNodes: ${id}"))

  def isMajority(n: Int) = n > (clusterNodes.size/2)

  startWith(Follower, RaftState.empty)

  when(Candidate){
    case Event(StartElection, state) =>
      resetElectionTicker()
      // broadcast vote request
      val newTerm = state.currentTerm + 1
      val req = RequestVoteRequest(newTerm, myID.toString(), logRepo.lastLogIndex(), logRepo.lastLogTerm())
      otherNodes.map(_.actor ! req)
      stay using state.clearElectionState.copy(currentTerm = newTerm)

    case Event(req: RequestVoteRequest, state) =>
      // 1. Reply false if term < currentTerm (§5.1)
      // 2. If votedFor is null or candidateId, and candidate’s log is at
      //    least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
      if (req.term < state.currentTerm) {
        log.info(s"reject ${req.candidateId} due to stale term")
        sender ! RequestVoteResponse(state.currentTerm, voteGranted = false)
        goto(Follower) using state.copy(leaderID = None)

      } else if (state.votedFor.map(_.toString() != req.candidateId).getOrElse(false)) {
        log.info(s"reject ${req.candidateId} because I've already voted ${state.votedFor}")
        sender ! RequestVoteResponse(state.currentTerm, voteGranted = false)
        stay

      } else if (req.lastLogIndex < logRepo.lastLogIndex() || req.lastLogTerm < logRepo.lastLogTerm()) {
        log.info(s"reject ${req.candidateId} dee to stale log")
        sender ! RequestVoteResponse(state.currentTerm, voteGranted = false)
        stay

      } else {
        log.info(s"vote from ${req.candidateId} has accepted")
        sender ! RequestVoteResponse(state.currentTerm, voteGranted = true)
        stay using state.copy(votedFor = Some(NodeID.of(req.candidateId)))
      }

    case Event(VoteResponse(dest, res), state) =>
      if (res.term > state.currentTerm) {
        // if new term is discovered, goto follower
        log.info(s"[Candidate] discover new term from vote response, you lose!")
        goto(Follower)

      } else if (res.term < state.currentTerm) {
        log.info(s"[Candidate] discover stale term from vote response, ignored")
        stay

      } else if (!res.voteGranted) {
        log.info(s"[Candidate] lose")
        stay

      } else {
        log.info(s"[Candidate] granted from ${dest}")
        var newState = state.copy(granted = state.granted + dest)
        if (isMajority(newState.granted.size)) {
          log.info(s"[Candidate] granted by majority, you win!")
          val lastLogIndex = logRepo.lastLogIndex()
          newState = newState.copy(
            leaderID = Some(myID),
            nextIndex = otherNodes.map(n => (n.id, lastLogIndex + 1)).toMap, // (initialized to leader last log index + 1
            matchIndex = None,
          )
          goto(Leader) using newState
        } else {
          stay using newState
        }
      }

    case Event(ElectionTimeout, state) =>
      log.info(s"[Candidate] election timeout")
      self ! StartElection
      stay

    case Event(req: AppendEntriesRequest, state) =>
      // If the leader’s term (included in its RPC) is at least
      // as large as the candidate’s current term, then the candidate
      // recognizes the leader as legitimate and returns to follower
      // state. If the term in the RPC is smaller than the candidate’s
      // current term, then the candidate rejects the RPC and continues in candidate state.
      if (req.term >= state.currentTerm) {
        log.info(s"[Candidate] discover new leader's request ${req.leaderID}")
        self ! req // rethrow to myself to append log
        goto(Follower) using state.copy(
          currentTerm = req.term,
          leaderID = Some(NodeID.of(req.leaderID))
        )

      } else {
        log.info(s"[Candidate] discover stale leader's request ${req.leaderID}")
        sender ! AppendEntriesResponse(state.currentTerm, false)
        stay
      }
  }

  when(Follower) {
    case Event(ElectionTimeout, state) =>
      log.info("[follower] timeout")
      // start election
      // increment term and transit to candidate
      goto(Candidate) using state.clearElectionState

    case Event(req: AppendEntriesRequest, state) =>
      //  If one server’s current term is smaller than the other’s, then it updates its current term to the larger value.
      //  If a candidate or leader discovers that its term is out of date, it immediately reverts to follower state.
      //  If a server receives a request with a stale term number, it rejects the request.
      // Receiver implementation:
      // 1. Reply false if term < currentTerm (§5.1)
      // 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
      // 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
      // 4. Append any new entries not already in the log
      // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

      if (req.term < state.currentTerm) {
        // (1) reject if term is stale
        log.warning(s"[follower] received stale term: received=${req.term} current=${state.currentTerm}")
        sender ! AppendEntriesResponse(state.currentTerm, false)
        stay

      } else {
        // received normal heartbeat
        resetElectionTicker()

        var newState = state.copy(leaderID = Some(NodeID.of(req.leaderID)))
        // If one server’s current term is smaller than the other’s, then it updates its current term to the larger value.
        if (req.term > state.currentTerm) {
          log.warning(s"[follower] discovered stale term: received=${req.term} current=${state.currentTerm}")
          newState = newState.copy(currentTerm = req.term)
        }

        if (req.prevLogIndex == 0) {
          // empty!
          log.info(s"[follower] empty!")
          logRepo.empty()
          stateMachine ! Initialize
          sender ! AppendEntriesResponse(newState.currentTerm, true)
          stay using newState

        } else if (!logRepo.contains(req.prevLogTerm, req.prevLogIndex)) {
          // (2) reject if log doesn't contain a matching previous entry
          log.warning(s"[follower] prevLog is inconsistent: received=(${req.prevLogTerm},${req.prevLogIndex})")
          sender ! AppendEntriesResponse(newState.currentTerm, false)
          stay using newState

        } else {
          // (3) delete the existing entry if conflicts exists
          logRepo.removeConflicted(req.prevLogTerm, req.prevLogIndex)

          // (4) Append log
          logRepo.append(req.entries.map(LogRepoEntry.fromMessage))

          // (5) commit and update commitIndex
          if (req.leaderCommit > logRepo.getCommitIndex()) {
            logRepo.commit(req.leaderCommit)
            log.info(s"[follower] new logs are committed: newIndex=${req.leaderCommit}")
          }

          // start to apply logs
          stateMachine ! ApplyAsync(req.leaderCommit)

          sender ! AppendEntriesResponse(newState.currentTerm, true)
          stay using newState
        }
      }
  }

  when(Leader) {
    case Event(BroadcastAppendLog(sendEmpty), state) =>
      if (state.matchIndex.nonEmpty) {
        log.info("[Leader] tick occurs during logAppending")
        stay

      } else {
        otherNodes.map { node =>
          val prevIndex = state.nextIndex.getOrElse(node.id, throw new Exception(s"no such key in nextIndex: ${node.id}"))
          val logs = logRepo.getLogs(prevIndex)
          val logTerm = logs.headOption.map(_.term).getOrElse(0L)
          node.actor ! AppendEntriesRequest(
            term = state.currentTerm,
            leaderID = myID.toString(),
            prevLogIndex = prevIndex,
            prevLogTerm = logTerm,
            entries = if (sendEmpty) Seq.empty else logs.map(_.toMessage),
            leaderCommit = logRepo.getCommitIndex(),
          )
        }
        // TODO: start timer for expiration of waiting response (or wait until election ticker expires?)
        log.info(s"[Leader] start waiting for append log response: ${}")
        stay using state.copy(matchIndex = Some(Map.empty))
      }

    case Event(AppendResponse(nodeID, res, requestedPrevIndex, lastIndex), state) =>
      state.matchIndex.fold (stay) { matchIndex =>
        val currentNextIndex = state.nextIndex(nodeID)

        if (res.term > state.currentTerm) {
          log.info(s"[Leader] detect new term from ${nodeID}")
          goto(Follower)

        } else if (!res.success) {
          //  If AppendEntries fails because of log inconsistency decrement nextIndex and retry (§5.3)
          val prevIndex = state.nextIndex.getOrElse(nodeID, throw new Exception(s"no such key in nextIndex: ${nodeID}")) - 1
          val logs = logRepo.getLogs(prevIndex)
          nodeOf(nodeID).actor ! AppendEntriesRequest(
            term = state.currentTerm,
            leaderID = myID.toString(),
            prevLogIndex = prevIndex,
            prevLogTerm = logs.headOption.map(_.term).getOrElse(0L),
            entries = logs.map(_.toMessage),
            leaderCommit = logRepo.getCommitIndex(),
          )

          stay using state.copy(
            nextIndex = state.nextIndex.updated(nodeID, prevIndex)
          )

        } else if(currentNextIndex != requestedPrevIndex) {
          log.info(s"[Leader] received unexpected response, ignored: current=${currentNextIndex}, requested=${requestedPrevIndex}")
          stay

        } else {
          var newState = lastIndex.map { lastLogIndex =>
            val nextState = state.copy(
              matchIndex = Some(matchIndex.updated(nodeID, lastLogIndex)),
              nextIndex = state.nextIndex.updated(nodeID, lastLogIndex),
            )

            // If there exists an N such that N > commitIndex, a majority
            // of matchIndex[i] ≥ N, and log[N].term == currentTerm:
            // set commitIndex = N (§5.3, §5.4).
            val currentCommitIndex = logRepo.getCommitIndex()
            val newCommitIndex = matchIndex.values.filter(_ > currentCommitIndex).map ( n =>
              isMajority(matchIndex.values.filter(_ >= n).size) match {
                case true => Some(n)
                case false => None
              }
            ).flatten.headOption.getOrElse(currentCommitIndex)

            if (newCommitIndex > currentCommitIndex) {
              log.info(s"[Leader] new commit index! ${currentCommitIndex} -> ${newCommitIndex}")
              logRepo.commit(newCommitIndex)
            }
            nextState

          }.getOrElse(state)

          // if received response from majority, finish waiting
          if (isMajority(matchIndex.size)) {
            log.info(s"[Leader] finish waiting for append log response: ${lastIndex}")
            newState = newState.copy(matchIndex = None)
          }

          stay using newState
        }
      }

    case Event(req: AppendEntriesRequest, state) =>
      // TODO: shoud compare index?
      if (req.term > state.currentTerm) {
        log.info(s"[Leader] discover new leader's request ${req.leaderID}")
        self ! req // rethrow to myself to append log
        goto(Follower) using state.copy(
          currentTerm = req.term,
          leaderID = Some(NodeID.of(req.leaderID))
        )
      } else {
        log.info(s"[Leader] discover stale leader's request ${req.leaderID}")
        sender ! AppendEntriesResponse(state.currentTerm, false)
        stay
      }
  }

  onTransition {
    case _ -> Follower =>
      stopHeartbeat()
      resetElectionTicker()

    case _ -> Candidate =>
      stopHeartbeat()
      self ! StartElection

    case _ -> Leader =>
      if (stateData.leaderID != Some(myID))
        throw new Exception(s"current leader id is not own ${stateData.leaderID}")
      stopElectionTicker()
      startHeartbeat()
  }

  initialize()
}

object RaftActor {
  def props(stateMachine: ActorRef, log: LogRepository, nodes: Set[Node], me: NodeID) =
    Props(new RaftActor(stateMachine, log, nodes, me))
}
