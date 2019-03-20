package com.github.rerorero.reroft.raft

import akka.actor.{ActorRef, Cancellable, LoggingFSM, Props}
import com.github.rerorero.reroft.fsm.Apply
import com.github.rerorero.reroft.log.{LogEntry, LogRepository}
import com.github.rerorero.reroft.{AppendEntriesRequest, AppendEntriesResponse}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Random

// Role is server state
sealed trait Role
case object Initialize extends Role
case object Follower extends Role
case object Candidate extends Role
case object Leader extends Role

sealed trait Message
case object ElectionTimeout extends Message
case class Configure(nodes: Set[Node], me: NodeID) extends Message
case class AppendEntries(msg: AppendEntriesRequest) extends Message

sealed trait Response
case class AppendEntriesRes(msg: AppendEntriesResponse) extends Response

class RaftActor(
  val stateMachine: ActorRef,
  val logRepo: LogRepository,
  val minElectionTimeoutMS: Int = 250,
  val maxElectionTimeoutMS: Int = 500,
) extends LoggingFSM[Role, RaftState] {
  implicit val executionContext: ExecutionContext = context.system.dispatcher

  protected[this] var electionTicker: Option[Cancellable] = None

  def resetElectionTicker(): Unit = {
    electionTicker.foreach(_.cancel())
    electionTicker = Some(context.system.scheduler.scheduleOnce(
      Duration(minElectionTimeoutMS + Random.nextInt(maxElectionTimeoutMS - minElectionTimeoutMS), MILLISECONDS),
      self,
      ElectionTimeout
    ))
  }

  // TODO: term should be persistent
  startWith(Initialize, RaftState.empty)

  when(Initialize) {
    case Event(Configure(nodes, me), state) =>
      log.info(s"raft started: state=${state}")
      goto(Candidate) using state.copy(clusterNodes = nodes, myID = me)
  }

  when(Candidate){
    case _ => ???
  }

  when(Follower) {
    case Event(ElectionTimeout, state) =>
      log.info("[follower] timeout")
      goto(Follower) using state

    case Event(AppendEntries(req), state) =>
      // TODO: reset timeout timer

      // Heartbeat
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
        sender ! AppendEntriesRes(AppendEntriesResponse(state.currentTerm, false))
        stay using state

      } else if (!logRepo.contains(req.prevLogTerm, req.prevLogIndex)) {
        // (2) reject if log doesn't contain a mattching previous entry
        log.warning(s"[follower] prevLog is inconsistent: received=(${req.prevLogTerm},${req.prevLogIndex})")
        sender ! AppendEntriesRes(AppendEntriesResponse(state.currentTerm, false))
        stay using state

      } else {
        // received normal heartbeat
        resetElectionTicker()

        var newState = state
        // If one server’s current term is smaller than the other’s, then it updates its current term to the larger value.
        if (req.term > state.currentTerm) {
          log.warning(s"[follower] discovered stale term: received=${req.term} current=${state.currentTerm}")
          newState = state.copy(currentTerm = req.term)
        }

        // (3) delete the existing entry if conflicts exists
        logRepo.removeConflicted(req.prevLogTerm, req.prevLogTerm)

        // (4) Append log
        logRepo.append(req.entries.map(LogEntry.fromMessage))

        // (5) commit and update commitIndex
        if (req.leaderCommit > logRepo.getCommitIndex()) {
          logRepo.commit(req.leaderCommit)
          log.info(s"[follower] new logs are committed: newIndex=${req.leaderCommit}")
        }

        // ask to apply logs
        stateMachine ! Apply(req.leaderCommit)

        stay using newState
      }
  }
}

object RaftActor {
  def props(stateMachine: ActorRef, log: LogRepository) = Props(new RaftActor(stateMachine, log))

}
