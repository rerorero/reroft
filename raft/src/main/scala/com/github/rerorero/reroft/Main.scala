package com.github.rerorero.reroft

//import akka.actor.ActorSystem
//import com.github.rerorero.reroft.grpc.{Server, ServerConfig}
//import com.google.common.net.HostAndPort
//import com.typesafe.config.ConfigFactory
//
//object Main {
//  def main(args: Array[String]): Unit = {
//    val conf = ConfigFactory.parseString("akka.http.server.preview.enable-http2 = on")
//        .withFallback(ConfigFactory.defaultApplication())
//    val system = ActorSystem("reroft", conf)
//
//    // props
//    val nodes = sys.props.get("raft.nodes").map(_.split(",").map(HostAndPort.fromString).toSet).get
//    val me = sys.props.get("raft.me").map(HostAndPort.fromString).get
//
//    new Server(system, ServerConfig(nodes, me)).run(me.getPort)
//  }
//}

