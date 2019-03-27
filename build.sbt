
lazy val commonSettings = Seq(
  scalaVersion := "2.12.8",
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
  ),
  resolvers += Resolver.sonatypeRepo("public"),
  parallelExecution in Test := false,
  libraryDependencies ++= Seq(
    "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
    "org.scalacheck" %% "scalacheck" % "1.14.0" % Test,
    "com.typesafe.akka" %% "akka-testkit" % "2.5.20" % Test,
    "org.scalatest" % "scalatest_2.12" % "3.0.5" % Test,
    "org.mockito" % "mockito-core" % "2.25.1" % Test,
  ),
)

lazy val raft = (project in file("raft"))
  .settings(
    commonSettings,
    name := "raft",
    version := "0.1.0",
  )
  .enablePlugins(AkkaGrpcPlugin)

lazy val appCalc = (project in file("app_calc"))
  .settings(
    commonSettings,
    name := "calc",
    version := "0.1.0",
    mainClass in assembly := Some("com.github.rerorero.reroft.calc.Main"),
    assemblyJarName in assembly := { s"${name.value}-${version.value}.jar" },
    test in assembly := {},
  )
  .dependsOn(raft)

lazy val root = (project in file("."))
  .aggregate(raft, appCalc)
