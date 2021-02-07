name := "LearningSpark"

version := "0.1"

scalaVersion := "2.12.10"

idePackagePrefix := Some("com.guzelcihad.spark")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql"  % "3.0.0"
)