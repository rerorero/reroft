import scalapb.compiler.Version.{grpcJavaVersion, scalapbVersion, protobufVersion}

version := "0.1.0"

name := "reroft"

scalaVersion := "2.12.8"

scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked",
  "-feature",
  "-Xlint",
  "-language:postfixOps",
  "-language:implicitConversions",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-Yno-adapted-args",
  "-Ywarn-unused-import"
)

resolvers += Resolver.sonatypeRepo("public")


libraryDependencies ++= Seq(
  "io.grpc" % "grpc-netty" % grpcJavaVersion,
  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapbVersion,
  "org.scalatest" % "scalatest_2.12" % "3.0.5" % "test",
  "org.mockito" % "mockito-core" % "2.25.1" % "test"
)

mainClass in assembly := Some("com.github.reroft.Main")

assemblyJarName in assembly := { s"${name.value}-${version.value}.jar" }

parallelExecution in Test := false

test in assembly := {}

lazy val showVersion = taskKey[Unit]("Show version")
showVersion := {
  println("reroft version: " + version.value)
}

// configure protobuf
PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)

PB.protoSources in Compile += (baseDirectory in LocalRootProject).value / "proto"
