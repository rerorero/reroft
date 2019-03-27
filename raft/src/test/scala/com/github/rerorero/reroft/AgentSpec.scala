package com.github.rerorero.reroft

import java.net.InetSocketAddress

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.UseHttp2.Always
import akka.http.scaladsl.{Http, HttpConnectionContext}
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.github.rerorero.reroft.grpc._
import com.github.rerorero.reroft.grpc.test.{TestComputed, TestEntry}
import com.github.rerorero.reroft.logs.LogRepository
import com.github.rerorero.reroft.raft.RaftConfig
import com.github.rerorero.reroft.test.TestUtil
import com.google.common.net.HostAndPort
import com.google.protobuf.any.Any
import com.typesafe.config.ConfigFactory
import io.grpc.{Status, StatusRuntimeException}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import scalapb.GeneratedMessageCompanion

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class AgentSpec
  extends TestKit(ActorSystem("test",
    ConfigFactory.parseString("akka.http.server.preview.enable-http2 = on")
      .withFallback(ConfigFactory.defaultApplication())))
    with ImplicitSender
    with Matchers
    with WordSpecLike
    with BeforeAndAfterAll
    with ScalaFutures
    with TestUtil {
  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
    shutdown()
  }

  implicit val ec: ExecutionContext = system.dispatcher
  implicit val mat: Materializer = ActorMaterializer()

  val fsmProbe = TestProbe()

  val node1 = new MockServer
  val node2 = new MockServer

  class MockServer extends RaftService {
    val mocked = mock(classOf[RaftService])

    override def appendEntries(in: AppendEntriesRequest): Future[AppendEntriesResponse] = mocked.appendEntries(in)

    override def requestVote(in: RequestVoteRequest): Future[RequestVoteResponse] = mocked.requestVote(in)

    override def clientCommand(in: ClientCommandRequest): Future[ClientCommandResponse] = {
      mocked.clientCommand(in)
    }

    def run() = Http().bindAndHandleAsync(
      RaftServiceHandler(this),
      interface = "127.0.0.1",
      port = unusedPorts(1).head,
      connectionContext = HttpConnectionContext(http2 = Always)
    )
  }

  class MockedAgent(
    val config: RaftConfig,
    val logRepository: LogRepository[TestEntry] = mock(classOf[LogRepository[TestEntry]]),
    val stateMachine: ActorRef = fsmProbe.ref,
  ) extends Agent[TestEntry, TestComputed] {
    override implicit val cmpEntry: GeneratedMessageCompanion[TestEntry] = TestEntry
    override implicit val cmpComputed: GeneratedMessageCompanion[TestComputed] = TestComputed
  }

  implicit class RichAddr(addr: InetSocketAddress) {
    def hostPort: HostAndPort = HostAndPort.fromString(s"127.0.0.1:${addr.getPort}")
  }

  "Agent" should {
    "redirect client command to leader" in {
      val expected = sample[TestComputed]
      def fut() = for {
        server1 <- node1.run()
        server2 <- node2.run()
        sut = {
          val agent = new MockedAgent(config = RaftConfig(Set(server1.localAddress, server2.localAddress).map(_.hostPort), HostAndPort.fromString("localhost:9999")))
          when(node1.mocked.clientCommand(any[ClientCommandRequest])).thenReturn(
            Future.successful(ClientCommandResponse(None, server2.localAddress.hostPort.toString))
          )
          when(node2.mocked.clientCommand(any[ClientCommandRequest])).thenReturn(Future.successful(ClientCommandResponse(Some(Any.pack(expected)))))
          agent
        }
        actual <- sut.runClientCommand(sample[Seq[TestEntry]])
      } yield  {
        server1.unbind()
        server2.unbind()
        actual
      }

      (1 to 10).foreach { _ =>
        assert(Await.result(fut(), Duration.Inf) === expected)
      }
    }
  }
}
