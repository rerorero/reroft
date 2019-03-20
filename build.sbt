
version := "0.1.0"

name := "reroft"

scalaVersion := "2.12.8"

scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked",
  "-feature",
  "-Xlint",
  //"-Ywarn-unused-import",
  "-language:postfixOps",
  "-language:implicitConversions",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-Yno-adapted-args",
)

resolvers += Resolver.sonatypeRepo("public")

enablePlugins(AkkaGrpcPlugin)

libraryDependencies ++= Seq(
  "org.scalacheck" %% "scalacheck" % "1.14.0" % Test,
  "com.typesafe.akka" %% "akka-testkit" % "2.5.20" % Test,
  "org.scalatest" % "scalatest_2.12" % "3.0.5" % Test,
  "org.mockito" % "mockito-core" % "2.25.1" % Test,
)

mainClass in assembly := Some("com.github.rerorero.reroft.Main")

assemblyJarName in assembly := { s"${name.value}-${version.value}.jar" }

parallelExecution in Test := false

test in assembly := {}

lazy val showVersion = taskKey[Unit]("Show version")
showVersion := {
  println("reroft version: " + version.value)
}

