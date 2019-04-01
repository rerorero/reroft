package com.github.rerorero.reroft.raft

import akka.actor.{ActorRef, Cancellable, LoggingFSM, Props}
import com.github.rerorero.reroft.fsm.{Apply, ApplyResult}
import com.github.rerorero.reroft.grpc._
import com.github.rerorero.reroft.logs.{LogRepoEntry, LogRepository}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Random

case class ClientCommand(req: ClientCommandRequest, sender: ActorRef)
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

  def isMajority(n: Int) = n > (clusterNodes.size / 2.0)

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
      logWarn(s"received stale term: received=${req.term} current=${state.currentTerm}")
      (AppendEntriesResponse(state.currentTerm, false), state)

    } else {
      // received normal heartbeat
      resetElectionTicker()

      var newState = state.copy(leaderID = Some(NodeID.of(req.leaderID)))
      // If one server’s current term is smaller than the other’s, then it updates its current term to the larger value.
      if (req.term > state.currentTerm) {
        logWarn(s"discovered stale term: received=${req.term} current=${state.currentTerm}")
        newState = newState.copy(currentTerm = req.term)
      }

      if (!logRepo.contains(req.prevLogTerm, req.prevLogIndex)) {
        // (2) reject if log doesn't contain a matching previous entry
        logWarn(s"prevLog is inconsistent: received=(${req.prevLogTerm},${req.prevLogIndex})")
        (AppendEntriesResponse(newState.currentTerm, false), newState)

      } else {
        // (3) delete the existing entry if conflicts exists
        logRepo.removeConflicted(req.prevLogTerm, req.prevLogIndex)

        // (4) Append log
        logRepo.append(req.entries.map(LogRepoEntry.fromMessage[Entry]))

        // (5) commit and update commitIndex
        if (req.leaderCommit > logRepo.getCommitIndex()) {
          logRepo.commit(req.leaderCommit)
          logInfo(s"new logs are committed: newIndex=${req.leaderCommit}")
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
      logInfo(s"reject ${req.candidateId} due to stale term")
      (RequestVoteResponse(state.currentTerm, voteGranted = false), state)

    } else {
      var newState = state
      if (req.term > state.currentTerm) {
        logInfo(s"discover new term from ${req.candidateId} term=${req.term} current=${state.currentTerm}")
        newState = newState.clearElectionState.copy(currentTerm = req.term)
      }

      if (state.votedFor.map(_.toString() != req.candidateId).getOrElse(false)) {
        logInfo(s"reject ${req.candidateId} because I've already voted ${state.votedFor}")
        (RequestVoteResponse(state.currentTerm, voteGranted = false), newState)

      } else if (req.lastLogIndex < logRepo.lastLogIndex() || req.lastLogTerm < logRepo.lastLogTerm()) {
        logInfo(s"reject ${req.candidateId} dee to stale log")
        (RequestVoteResponse(state.currentTerm, voteGranted = false), newState)

      } else {
        logDebug(s"vote from ${req.candidateId} has accepted")
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

  def logDebug(m: String) = log.debug(s"[${myID.toString()}](${stateName.toString}) ${m}")

  def logInfo(m: String) = log.info(s"[${myID.toString()}](${stateName.toString}) ${m}")

  def logWarn(m: String) = log.warning(s"[${myID.toString()}](${stateName.toString}) ${m}")

  def logError(m: String) = log.error(s"[${myID.toString()}](${stateName.toString}) ${m}")

  startWith(Follower, RaftState.empty)

  //-----------------------
  //  CANDIDATE
  //-----------------------
  when(Candidate) {
    // *************
    // START ELECTION
    // *************
    case Event(StartElection, state) =>
      resetElectionTicker()
      // broadcast vote request
      val newTerm = state.currentTerm + 1
      val req = RequestVoteRequest(newTerm, myID.toString(), logRepo.lastLogIndex(), logRepo.lastLogTerm())
      otherNodes.map(_.actor ! req)
      logInfo(s"election ${newTerm} started!")
      stay using state.clearElectionState.copy(currentTerm = newTerm)

    case Event(req: RequestVoteRequest, state) =>
      val (res, newState) = handleVoteRequest(req, state)
      sender ! res
      stay using newState

    // *************
    // VOTE RESPONSE
    // *************
    case Event(VoteResponse(dest, res), state) if res.term > state.currentTerm =>
      logInfo(s"discover new term via vote response from ${dest}, you lose!")
      goto(Follower)
    case Event(VoteResponse(dest, res), state) if res.term < state.currentTerm =>
      logDebug(s"discover stale term via vote response from ${dest}, ignored")
      stay
    case Event(VoteResponse(dest, res), _) if !res.voteGranted =>
      logInfo(s"lose by ${dest}")
      stay
    case Event(VoteResponse(dest, res), state) =>
      logInfo(s"granted from ${dest}")
      var newState = state.copy(granted = state.granted + dest)
      if (isMajority(newState.granted.size)) {
        logInfo(s"granted by majority, you win!")
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
      logDebug(s"election timeout")
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
      logInfo(s"discover new leader's request ${req.leaderID}")
      val (res, newState) = handleAppendLogRequest(req, state.copy(
        currentTerm = req.term,
        leaderID = Some(NodeID.of(req.leaderID)),
        votedFor = None,
        granted = Set.empty,
      ))
      sender ! res
      goto(Follower) using newState
    case Event(req: AppendEntriesRequest, state) =>
      logInfo(s"discover stale leader's request ${req.leaderID}")
      sender ! AppendEntriesResponse(state.currentTerm, false)
      stay

    // *************
    // CLIENT COMMAND
    // *************
    case Event(r: ClientCommandRequest, _) =>
      self ! ClientCommand(r, sender)
      stay
    case Event(command: ClientCommand, _) =>
      logInfo(s"receives client command, delay message")
      // TODO: handle timeout or limit the number of retries
      context.system.scheduler.scheduleOnce(minElectionTimeoutMS.millis / 2, self, command)
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
      logInfo("timeout")
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
    case Event(r: ClientCommandRequest, _) =>
      self ! ClientCommand(r, sender)
      stay
    case Event(command: ClientCommand, state) =>
      logInfo(s"receives client command")
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
      logDebug(s"tick during logAppending, delayed next tick: ${state.matchIndex}")
      stay
    case Event(BroadcastAppendLog, state) =>
      otherNodes.foreach { node =>
        // TODO: cache prevLogIndex and prevLogTerm
        val nextIndex = state.nextIndex.getOrElse(node.id, throw new Exception(s"no such key in nextIndex: ${node.id}"))
        val logs = logRepo.getLogs(nextIndex)
        val prevLogIndex = if (nextIndex > 0) nextIndex - 1 else 0L
        val prevLogTerm = if (nextIndex > 0) logRepo.getLogs(prevLogIndex, Some(prevLogIndex)).headOption.map(_.term).getOrElse(0L) else 0L
        val leaderCommitIndex = logRepo.getCommitIndex()
        node.actor ! AppendEntriesRequest(
          term = state.currentTerm,
          leaderID = myID.toString(),
          prevLogIndex = prevLogIndex,
          prevLogTerm = prevLogTerm,
          entries = logs.map(_.toMessage),
          leaderCommit = leaderCommitIndex,
        )
        logInfo(s"send appendEntriesRequest to=${node.id.toString()}, prevLog=${prevLogIndex},${prevLogTerm}, logs=${logs.length}, leaderCommit=${leaderCommitIndex}")
      }
      // TODO: start timer for expiration of waiting response (or wait until election ticker expires?)
      logInfo(s"start waiting for append log response: ${myID}")
      stay using state.copy(matchIndex = Some(Map.empty))

    // *************
    // APPEND LOG RESPONSE
    // *************
    case Event(AppendResponse(nodeID, _, _, _), state) if !state.nextIndex.contains(nodeID) =>
      logWarn(s"AppendResponse from unknown node: ${nodeID}")
      stay
    case Event(AppendResponse(nodeID, res, _, _), state) if res.term > state.currentTerm =>
      logInfo(s"detect new term from ${nodeID}")
      state.commandQue.foreach(c => c.command.sender ! ClientFailure(new Exception("detected new term, request is now out of date")))
      goto(Follower) using state.copy(
        leaderID = None,
        commandQue = List.empty,
      )
    case Event(AppendResponse(nodeID, res, _, _), state) if !res.success =>
      // TODO: cache prevLogIndex and prevLogTerm
      //  If AppendEntries fails because of log inconsistency decrement nextIndex and retry (§5.3)
      var nextIndex = state.nextIndex.getOrElse(nodeID, throw new Exception(s"no such key in nextIndex: ${nodeID}")) - 1
      if (nextIndex < 0) nextIndex = 0L
      val prevLogIndex = if (nextIndex > 0) nextIndex - 1 else 0L
      val prevLogTerm = if (nextIndex > 0) logRepo.getLogs(prevLogIndex, Some(prevLogIndex)).headOption.map(_.term).getOrElse(0L) else 0L
      val logs = logRepo.getLogs(nextIndex)
      nodeOf(nodeID).actor ! AppendEntriesRequest(
        term = state.currentTerm,
        leaderID = myID.toString(),
        prevLogIndex = prevLogIndex,
        prevLogTerm = prevLogTerm,
        entries = logs.map(_.toMessage),
        leaderCommit = logRepo.getCommitIndex(),
      )

      logInfo(s"rejected appendLogEntries by ${nodeOf(nodeID).id}, retry with index=${prevLogIndex}")
      stay using state.copy(
        nextIndex = state.nextIndex.updated(nodeID, nextIndex)
      )
    case Event(AppendResponse(nodeID, _, requestedPrevIndex, _), state) if (state.nextIndex(nodeID)-1) != requestedPrevIndex =>
      // TODO: state.nextIndex(nodeID) is not safe
      logInfo(s"received unexpected response, ignored: current=${state.nextIndex(nodeID)}, requested=${requestedPrevIndex}")
      stay
    case Event(AppendResponse(nodeID, _, requestedPrevIndex, lastIndex), state) =>
      // lastIndex is last index of logs sent to node, so it is None when it sends empty logs (i.e. just a heartbeat)
      val (matched, next) = lastIndex match {
        case Some(last) => (last, last + 1) // new next index
        case None =>
          val next = state.nextIndex.getOrElse(nodeID, throw new Exception(s"no such key in nexIndex: ${nodeID}")) // index has not updated
          (next-1, next)
      }
      val newNextIndex = state.nextIndex.updated(nodeID, next)
      val currentCommitIndex = logRepo.getCommitIndex()

      val newMatchIndex = state.matchIndex.flatMap { matchIndex =>
        // If there exists an N such that N > commitIndex, a majority
        // of matchIndex[i] ≥ N, and log[N].term == currentTerm:
        // set commitIndex = N (§5.3, §5.4).
        val matchIndexUpdated = matchIndex.updated(nodeID, matched)
        val newCommitIndex = matchIndexUpdated.values.filter(_ > currentCommitIndex).map(n =>
          isMajority(matchIndexUpdated.values.filter(_ >= n).size) match {
            case true => Some(n)
            case false => None
          }
        ).flatten.headOption.getOrElse(currentCommitIndex)

        if (newCommitIndex > currentCommitIndex) {
          logInfo(s"new commit index! ${currentCommitIndex} -> ${newCommitIndex}")
          logRepo.commit(newCommitIndex)
        }

        // if received response from majority, finish waiting
        if (isMajority(matchIndexUpdated.size)) {
          logInfo(s"finish waiting for append log response: ${lastIndex}")
          None
        } else {
          Some(matchIndexUpdated)
        }
      }

      val newState = state.copy(
        matchIndex = newMatchIndex,
        nextIndex = newNextIndex,
      )
      logInfo(s"accepted appendLogEntries by ${nodeID}, requestedIndex = ${requestedPrevIndex}, match=${newMatchIndex}, next=${newNextIndex}, last=${lastIndex}")
      stay using newState

    // *************
    // APPEND LOG REQUEST FROM OTHERS
    // *************
    case Event(req: AppendEntriesRequest, state) if req.term > state.currentTerm =>
      logInfo(s"discover new leader's request ${req.leaderID}")
      val (res, newState) = handleAppendLogRequest(req, state)
      sender ! res
      state.commandQue.foreach(c => c.command.sender ! ClientFailure(new Exception("detected new leader, request is now out of date")))
      goto(Follower) using newState.copy(
        leaderID = Some(NodeID.of(req.leaderID)),
        commandQue = List.empty,
      )
    case Event(req: AppendEntriesRequest, state) =>
      logInfo(s"discover stale leader's request ${req.leaderID}")
      sender ! AppendEntriesResponse(state.currentTerm, false)
      stay

    // *************
    // CLIENT COMMAND
    // *************
    case Event(r: ClientCommandRequest, _) =>
      self ! ClientCommand(r, sender)
      stay
    case Event(command: ClientCommand, state) =>
      logInfo(s"receives client command")

      val lastIndex = logRepo.lastLogIndex()
      val logs = command.req.entries.zipWithIndex.map {
        case (e, i) => LogRepoEntry.fromMessage[Entry](LogEntry(state.currentTerm, lastIndex + i + 1, Some(e)))
      }
      logRepo.append(logs)

      val newLastIndex = lastIndex + logs.length
      stateMachine ! Apply(newLastIndex)
      // TODO: handle timeout
      self ! BroadcastAppendLog
      logInfo(s"waiting for logs are committed until index=${newLastIndex}")

      stay using state.copy(
        // record the command with index so that sender of command (gRPC server) can wait corresponding response of the request asynchronously
        commandQue = CommandQueEntity(command, newLastIndex) :: state.commandQue
      )
    case Event(result: ApplyResult[Computed], state) => // TODO: unsafe due to type erasure
      // takes out a command that apply has completed and returns the result to the caller
      val (done, notYet) = state.commandQue.partition(_.lastLogIndex <= result.index)
      done.foreach { c =>
        // this is not precise, delayed responses of request also have latest computed results
        c.command.sender ! ClientSuccess(ClientCommandResponse(Some(result.toAny)))
      }
      logInfo(s"receive applyResult: notYet=${notYet}")
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
