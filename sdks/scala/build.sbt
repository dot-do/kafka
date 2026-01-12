ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "3.3.1"
ThisBuild / organization := "do.kafka"

lazy val root = (project in file("."))
  .settings(
    name := "kafka-sdk",
    libraryDependencies ++= Seq(
      // JSON processing
      "io.circe" %% "circe-core" % "0.14.6",
      "io.circe" %% "circe-generic" % "0.14.6",
      "io.circe" %% "circe-parser" % "0.14.6",

      // HTTP client
      "com.softwaremill.sttp.client3" %% "core" % "3.9.1",
      "com.softwaremill.sttp.client3" %% "circe" % "3.9.1",

      // FS2 Streams
      "co.fs2" %% "fs2-core" % "3.9.3",

      // Cats Effect
      "org.typelevel" %% "cats-effect" % "3.5.2",

      // Logging
      "org.slf4j" % "slf4j-api" % "2.0.9",

      // Testing
      "org.scalatest" %% "scalatest" % "3.2.17" % Test,
      "org.scalatestplus" %% "mockito-4-11" % "3.2.17.0" % Test,
      "org.typelevel" %% "cats-effect-testing-scalatest" % "1.5.0" % Test
    ),
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xfatal-warnings"
    )
  )

// Publishing configuration
publishMavenStyle := true
licenses := Seq("MIT" -> url("https://opensource.org/licenses/MIT"))
homepage := Some(url("https://kafka.do"))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/dotdo/kafka"),
    "scm:git@github.com:dotdo/kafka.git"
  )
)
developers := List(
  Developer(
    id = "dotdo",
    name = "do.kafka Team",
    email = "support@kafka.do",
    url = url("https://kafka.do")
  )
)
