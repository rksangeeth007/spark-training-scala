name := "spark-training"

version := "0.1"

//scalaVersion := "2.12.4"
scalaVersion := "2.13.12"

//val sparkVersion = "2.4.4"
val sparkVersion = "3.5.1"

resolvers ++= Seq(
  "apache-snapshots" at "https://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.0.8" % "test"
)