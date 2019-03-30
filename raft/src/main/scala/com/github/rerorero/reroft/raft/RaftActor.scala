package com.github.rerorero.reroft.raft

import akka.actor.{ActorRef, Cancellable, LoggingFSM, Props}
import com.github.rerorero.reroft.fsm.{Apply, ApplyResult, Initialize}
import com.github.rerorero.reroft.grpc._
import com.github.rerorero.reroft.logs.{LogRepoEntry, LogRepository}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Random

case class CommandQueEntity(command: ClientCommand, lastLogIndex: Long)

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
  commandQue: List[CommandQueEntity],
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
    commandQue = List.empty,
  )
}

// Role is server's state name
sealed trait Role
case object Follower extends Role
case object Candidate extends Role
case object Leader extends Role

// event
case object ElectionTimeout
case object StartElection
case object BroadcastAppendLog
case class ClientCommand(req: ClientCommandRequest, sender: ActorRef)
case object GetState

sealed trait ClientResponse
case class ClientSuccess(res: ClientCommandResponse) extends ClientResponse
case class ClientFailure(e: Throwable) extends ClientResponse

class RaftActor[Entry <: GeneratedMessage with Message[Entry], Computed <: GeneratedMessage with Message[Computed]](
  val stateMachine: ActorRef,
  val logRepo: LogRepository[Entry],
  val clusterNodes: Set[Node],
  val myID: NodeID,
  val minElectionTimeoutMS: Int = 150,
  val maxElectionTimeoutMS: Int = 300,
  val heartbeatIntervalMS: Int = 15,
)(implicit cmpEntry: GeneratedMessageCompanion[Entry]) extends LoggingFSM[Role, RaftState] {
  implicit val executionContext: ExecutionContext = context.system.dispatcher

  private[this] val otherNodes = clusterNodes.filter(_.id != myID)
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

  def isMajority(n: Int) = n > (clusterNodes.size/2.0)

  def flushClientCommandQueue(que: Seq[CommandQueEntity]): Unit = que.foreach { c =>
    c.command.sender ! ClientFailure(new Exception("request is out of date"))
  }

  def handleAppendLogRequest(req: AppendEntriesRequest, state: RaftState): (AppendEntriesResponse, RaftState) = {
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
      (AppendEntriesResponse(state.currentTerm, false), state)

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
        log.debug(s"[follower] empty!")
        logRepo.empty()
        stateMachine ! Initialize
        (AppendEntriesResponse(newState.currentTerm, true), newState)

      } else if (!logRepo.contains(req.prevLogTerm, req.prevLogIndex)) {
        // (2) reject if log doesn't contain a matching previous entry
        log.warning(s"[follower] prevLog is inconsistent: received=(${req.prevLogTerm},${req.prevLogIndex})")
        (AppendEntriesResponse(newState.currentTerm, false), newState)

      } else {
        // (3) delete the existing entry if conflicts exists
        logRepo.removeConflicted(req.prevLogTerm, req.prevLogIndex)

        // (4) Append log
        logRepo.append(req.entries.map(LogRepoEntry.fromMessage[Entry]))

        // (5) commit and update commitIndex
        if (req.leaderCommit > logRepo.getCommitIndex()) {
          logRepo.commit(req.leaderCommit)
          log.info(s"[follower] new logs are committed: newIndex=${req.leaderCommit}")
        }

        // start to apply logs
        stateMachine ! Apply(req.leaderCommit)

        (AppendEntriesResponse(newState.currentTerm, true), newState)
      }
    }
  }

  // 1. Reply false if term < currentTerm (§5.1)
  // 2. If votedFor is null or candidateId, and candidate’s log is at
  //    least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
  def handleVoteRequest(req: RequestVoteRequest, state: RaftState): (RequestVoteResponse, RaftState) =
    if (req.term < state.currentTerm) {
      log.info(s"[${stateName}] reject ${req.candidateId} due to stale term")
      (RequestVoteResponse(state.currentTerm, voteGranted = false), state)

    } else {
      var newState = state
      if (req.term > state.currentTerm) {
        log.info(s"[${stateName}] discover new term from ${req.candidateId} term=${req.term} current=${state.currentTerm}")
        newState = newState.clearElectionState.copy(currentTerm = req.term)
      }

      if (state.votedFor.map(_.toString() != req.candidateId).getOrElse(false)) {
        log.info(s"[${stateName}] reject ${req.candidateId} because I've already voted ${state.votedFor}")
        (RequestVoteResponse(state.currentTerm, voteGranted = false), newState)

      } else if (req.lastLogIndex < logRepo.lastLogIndex() || req.lastLogTerm < logRepo.lastLogTerm()) {
        log.info(s"[${stateName}] reject ${req.candidateId} dee to stale log")
        (RequestVoteResponse(state.currentTerm, voteGranted = false), newState)

      } else {
        log.debug(s"[${stateName}] vote from ${req.candidateId} has accepted")
        (RequestVoteResponse(state.currentTerm, voteGranted = true), newState.copy(votedFor = Some(NodeID.of(req.candidateId))))
      }
    }

  def handleStat(state: RaftState): StatCommandResponse = StatCommandResponse(
    nextIndex = state.nextIndex.map(kv => (kv._1.toString(), kv._2)),
    nodes = Seq(NodeStat(
      id = myID.toString(),
      state = stateName.toString,
      currentTerm = state.currentTerm,
      lastLogIndex = logRepo.lastLogIndex(),
      lastLogTerm = logRepo.lastLogTerm(),
      commitIndex = logRepo.getCommitIndex(),
    ))
  )

  startWith(Follower, RaftState.empty)

  //-----------------------
  //  CANDIDATE
  //-----------------------
  when(Candidate){
    // *************
    // START ELECTION
    // *************
    case Event(StartElection, state) =>
      resetElectionTicker()
      // broadcast vote request
      val newTerm = state.currentTerm + 1
      val req = RequestVoteRequest(newTerm, myID.toString(), logRepo.lastLogIndex(), logRepo.lastLogTerm())
      otherNodes.map(_.actor ! req)
      log.info(s"[Candidate] election ${newTerm} started!")
      stay using state.clearElectionState.copy(currentTerm = newTerm)

    case Event(req: RequestVoteRequest, state) =>
      val (res, newState) = handleVoteRequest(req, state)
      sender ! res
      stay using newState

    // *************
    // VOTE RESPONSE
    // *************
    case Event(VoteResponse(dest, res), state) if res.term > state.currentTerm =>
      log.info(s"[Candidate] discover new term via vote response from ${dest}, you lose!")
      goto(Follower)
    case Event(VoteResponse(dest, res), state) if res.term < state.currentTerm=>
      log.debug(s"[Candidate] discover stale term via vote response from ${dest}, ignored")
      stay
    case Event(VoteResponse(dest, res), _) if !res.voteGranted  =>
      log.info(s"[Candidate] lose by ${dest}")
      stay
    case Event(VoteResponse(dest, res), state) =>
      log.info(s"[Candidate] granted from ${dest}")
      var newState = state.copy(granted = state.granted + dest)
      if (isMajority(newState.granted.size)) {
        log.info(s"[Candidate] granted by majority, you win!")
        val lastLogIndex = logRepo.lastLogIndex()
        newState = newState.clearElectionState.copy(
          leaderID = Some(myID),
          nextIndex = otherNodes.map(n => (n.id, lastLogIndex + 1)).toMap, // (initialized to leader last log index + 1
          matchIndex = None,
        )
        goto(Leader) using newState
      } else {
        stay using newState
      }

    // *************
    // ELECTION TIMEOUT
    // *************
    case Event(ElectionTimeout, state) =>
      log.debug(s"[Candidate] election timeout")
      self ! StartElection
      stay

    // *************
    // APPEND ENTRIES REQUEST
    // *************
    // If the leader’s term (included in its RPC) is at least
    // as large as the candidate’s current term, then the candidate
    // recognizes the leader as legitimate and returns to follower
    // state. If the term in the RPC is smaller than the candidate’s
    // current term, then the candidate rejects the RPC and continues in candidate state.
    case Event(req: AppendEntriesRequest, state) if req.term >= state.currentTerm =>
      log.info(s"[Candidate] discover new leader's request ${req.leaderID}")
      val (res, newState) = handleAppendLogRequest(req, state.copy(
        currentTerm = req.term,
        leaderID = Some(NodeID.of(req.leaderID)),
        votedFor = None,
        granted = Set.empty,
      ))
      sender ! res
      goto(Follower) using newState
    case Event(req: AppendEntriesRequest, state) =>
      log.info(s"[Candidate] discover stale leader's request ${req.leaderID}")
      sender ! AppendEntriesResponse(state.currentTerm, false)
      stay

    // *************
    // CLIENT COMMAND
    // *************
    case Event(command: ClientCommand, _) =>
      log.info(s"[Candidate] receives client command, delay message")
      // TODO: handle timeout or limit the number of retries
      context.system.scheduler.scheduleOnce(minElectionTimeoutMS.millis/2, self, command)
      stay
    case Event(ApplyResult(_, _), _) =>
      stay

    // *************
    // FOR DEBUG
    // *************
    case Event(GetState, state) =>
      sender ! handleStat(state)
      stay
  }

  //-----------------------
  //  FOLLOWER
  //-----------------------
  when(Follower) {
    // *************
    // VOTE REQUEST
    // *************
    case Event(req: RequestVoteRequest, state) =>
      val (res, newState) = handleVoteRequest(req, state)
      sender ! res
      stay using newState

    // *************
    // ELECTION TIMEOUT
    // *************
    case Event(ElectionTimeout, state) =>
      log.info("[follower] timeout")
      // start election
      // increment term and transit to candidate
      goto(Candidate) using state.clearElectionState

    // *************
    // APPEND ENTRIES REQUEST
    // *************
    case Event(req: AppendEntriesRequest, state) =>
      val (res, newState) = handleAppendLogRequest(req, state)
      sender ! res
      stay using newState

    // *************
    // CLIENT COMMAND
    // *************
    case Event(command: ClientCommand, state) =>
      log.info(s"[follower] receives client command")
      state.leaderID match {
        case Some(leader) => command.sender ! ClientSuccess(ClientCommandResponse(None, leader.toString()))
        case None =>
          // TODO: handle timeout or limit the number of retries
          context.system.scheduler.scheduleOnce(heartbeatIntervalMS.millis, self, command)
      }
      stay
    case Event(ApplyResult(_, _), _) =>
      stay

    // *************
    // FOR DEBUG
    // *************
    case Event(GetState, state) =>
      sender ! handleStat(state)
      stay
  }

  //-----------------------
  //  LEADER
  //-----------------------
  when(Leader) {
    // *************
    // VOTE REQUEST
    // *************
    case Event(req: RequestVoteRequest, state) =>
      val currentTerm = state.currentTerm
      val (res, newState) = handleVoteRequest(req, state)
      sender ! res
      if (currentTerm != newState.currentTerm || newState.votedFor.nonEmpty)
        goto(Follower) using newState // detected new election or new term
      else
        stay using newState

    // *************
    // HEARTBEAT INTERVAL or START APPEND LOG
    // *************
    case Event(BroadcastAppendLog, state) if state.matchIndex.nonEmpty =>
      log.info("[Leader] tick during logAppending, delayed next tick")
      stay
    case Event(BroadcastAppendLog, state) =>
      otherNodes.map { node =>
        val prevIndex = state.nextIndex.getOrElse(node.id, throw new Exception(s"no such key in nextIndex: ${node.id}"))
        val logs = logRepo.getLogs(prevIndex)
        val logTerm = logs.headOption.map(_.term).getOrElse(0L)
        node.actor ! AppendEntriesRequest(
          term = state.currentTerm,
          leaderID = myID.toString(),
          prevLogIndex = prevIndex,
          prevLogTerm = logTerm,
          entries = logs.map(_.toMessage),
          leaderCommit = logRepo.getCommitIndex(),
        )
      }
      // TODO: start timer for expiration of waiting response (or wait until election ticker expires?)
      log.debug(s"[Leader] start waiting for append log response: ${myID}")
      stay using state.copy(matchIndex = Some(Map.empty))

    // *************
    // APPEND LOG RESPONSE
    // *************
    case Event(AppendResponse(nodeID, _, _, _), state) if !state.nextIndex.contains(nodeID) =>
      log.warning(s"[Leader] AppendResponse from unknown node: ${nodeID}")
      stay
    case Event(AppendResponse(nodeID, res, requestedPrevIndex, lastIndex), state) =>
      // lastIndex is last index of logs sent to node, so it is None when it sends empty logs (i.e. just a heartbeat)
      state.matchIndex.fold (stay) { matchIndex =>
        val currentNextIndex = state.nextIndex(nodeID)

        if (res.term > state.currentTerm) {
          log.info(s"[Leader] detect new term from ${nodeID}")
          state.commandQue.foreach(c => c.command.sender ! ClientFailure(new Exception("detected new term, request is now out of date")))
          goto(Follower) using state.copy(
            leaderID = None,
            commandQue = List.empty,
          )

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

          log.info(s"[Leader] rejected appendLogEntries by ${nodeID}, retry with index=${prevIndex}")
          stay using state.copy(
            nextIndex = state.nextIndex.updated(nodeID, prevIndex)
          )

        } else if(currentNextIndex != requestedPrevIndex) {
          log.info(s"[Leader] received unexpected response, ignored: current=${currentNextIndex}, requested=${requestedPrevIndex}")
          stay

        } else {
          var newState = lastIndex.map { lastLogIndex =>
            val newMatchIndex = matchIndex.updated(nodeID, lastLogIndex)
            val newNextIndex = state.nextIndex.updated(nodeID, lastLogIndex)

            // If there exists an N such that N > commitIndex, a majority
            // of matchIndex[i] ≥ N, and log[N].term == currentTerm:
            // set commitIndex = N (§5.3, §5.4).
            val currentCommitIndex = logRepo.getCommitIndex()
            val newCommitIndex = newMatchIndex.values.filter(_ > currentCommitIndex).map ( n =>
              isMajority(newMatchIndex.values.filter(_ >= n).size) match {
                case true => Some(n)
                case false => None
              }
            ).flatten.headOption.getOrElse(currentCommitIndex)

            if (newCommitIndex > currentCommitIndex) {
              log.info(s"[Leader] new commit index! ${currentCommitIndex} -> ${newCommitIndex}")
              logRepo.commit(newCommitIndex)
            }

            state.copy(
              matchIndex = Some(newMatchIndex),
              nextIndex = newNextIndex,
            )

          }.getOrElse(state)

          // if received response from majority, finish waiting
          if (isMajority(newState.matchIndex.fold(0)(_.size))) {
            log.debug(s"[Leader] finish waiting for append log response: ${lastIndex}")
            newState = newState.copy(matchIndex = None)
          }

          log.debug(s"[Leader] accepted appendLogEntries by ${nodeID}, requestedIndex = ${requestedPrevIndex}")
          stay using newState
        }
      }

    // *************
    // APPEND LOG REQUEST FROM OTHERS
    // *************
    case Event(req: AppendEntriesRequest, state) if req.term > state.currentTerm =>
      log.info(s"[Leader] discover new leader's request ${req.leaderID}")
      val (res, newState) = handleAppendLogRequest(req, state)
      sender ! res
      state.commandQue.foreach(c => c.command.sender ! ClientFailure(new Exception("detected new leader, request is now out of date")))
      goto(Follower) using newState.copy(
        leaderID = Some(NodeID.of(req.leaderID)),
        commandQue = List.empty,
      )
    case Event(req: AppendEntriesRequest, state) =>
      log.info(s"[Leader] discover stale leader's request ${req.leaderID}")
      sender ! AppendEntriesResponse(state.currentTerm, false)
      stay

    // *************
    // CLIENT COMMAND
    // *************
    case Event(command: ClientCommand, state) =>
      log.info(s"[Leader] receives client command")

      val lastIndex = logRepo.lastLogIndex()
      val logs = command.req.entries.zipWithIndex.map {
        case (e, i) => LogRepoEntry.fromMessage[Entry](LogEntry(state.currentTerm, lastIndex + i + 1, Some(e)))
      }
      logRepo.append(logs)

      val newLastIndex = lastIndex + logs.length
      stateMachine ! Apply(newLastIndex)
      // TODO: handle timeout
      self ! BroadcastAppendLog
      log.info(s"[Leader] waiting for logs are committed until index=${newLastIndex}")

      stay using state.copy(
        // record the command with index so that sender of command (gRPC server) can wait corresponding response of the request asynchronously
        commandQue = CommandQueEntity(command, newLastIndex) :: state.commandQue
      )
    case Event(result: ApplyResult[Computed], state) => // TODO: type erasure
      // takes out a command that apply has completed and returns the result to the caller
      val (done, notYet) = state.commandQue.partition(_.lastLogIndex <= result.index)
      done.foreach { c =>
        // this is not precise, delayed responses of request also have latest computed results
        c.command.sender ! ClientSuccess(ClientCommandResponse(Some(result.toAny)))
      }
      stay using state.copy(commandQue = notYet)

    // *************
    // FOR DEBUG
    // *************
    case Event(GetState, state) =>
      sender ! handleStat(state)
      stay
  }

  onTransition {
    case _ -> Follower =>
      stopHeartbeat()
      resetElectionTicker()

    case _ -> Candidate =>
      stopHeartbeat()
      self ! StartElection

    case _ -> Leader =>
      stopElectionTicker()
      startHeartbeat()
  }

  initialize()
}

object RaftActor {
  def props[Entry <: GeneratedMessage with Message[Entry], Computed <: GeneratedMessage with Message[Computed]](
    stateMachine: ActorRef, log: LogRepository[Entry], nodes: Set[Node], me: NodeID
  )(implicit cmpEntry: GeneratedMessageCompanion[Entry]) =
    Props(new RaftActor[Entry, Computed](stateMachine, log, nodes, me))
}
