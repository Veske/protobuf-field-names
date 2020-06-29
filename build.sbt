name := "protobuf-field-names"
version := "0.1"
scalaVersion := "2.12.11"

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-deprecation", // Emit warning and location for usages of deprecated APIs.
  "-encoding", "utf-8", // Specify character encoding used by source files.
  "-explaintypes", // Explain type errors in more detail.
  "-feature", // Emit warning and location for usages of features that should be imported explicitly.
  "-unchecked", // Enable additional warnings where generated code depends on assumptions.
  "-Xcheckinit", // Wrap field accessors to throw an exception on uninitialized access.
  "-Xfatal-warnings", // Fail the compilation if there are any warnings.
  "-Xlint:_" // Turn on all Xlint options
)

libraryDependencies in ThisBuild ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.6",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.6",
  "com.thesamet.scalapb" %% "sparksql-scalapb" % "0.10.4",
  "com.thesamet.scalapb.common-protos" %% "proto-google-common-protos-scalapb_0.10" % "1.18.0-0" % "protobuf",
  "com.thesamet.scalapb.common-protos" %% "proto-google-common-protos-scalapb_0.10" % "1.18.0-0",
  "com.holdenkarau" %% "spark-testing-base" % "2.4.5_0.14.0" % Test,
  "org.scalatest" %% "scalatest" % "3.1.2" % Test
)

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value / "scalapb"
)
PB.protocVersion := "-v3.11.4"
