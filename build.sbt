ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "test",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.1" % Provided,
      "org.apache.spark" %% "spark-sql" % "3.5.1" % Provided,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.1" % Provided,
    ),
    Compile / PB.targets := Seq(
      scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
    )
  )

Compile / run := Defaults
  .runTask(
    Compile / fullClasspath,
    Compile / run / mainClass,
    Compile / run / runner
  ).evaluated