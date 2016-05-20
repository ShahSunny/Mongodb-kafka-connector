name := "Mongodb-kafka-connector"
organization := "org.sunnyshahmca"
version := "0.1"
scalaVersion := "2.11.8"

libraryDependencies ++= Seq (
    "org.scalacheck" %% "scalacheck" % "1.13.0" % "test",
    "org.specs2" %% "specs2-scalacheck" % "3.7.1" % "test",
    "org.specs2" %% "specs2-core" % "3.7" % "test",
    "org.scalamock" %% "scalamock-specs2-support" % "3.2.2" % "test"
)

//libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"
libraryDependencies += "org.apache.avro" % "avro" % "1.8.0"
libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "1.1.1"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7"
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.21"

scalacOptions in Test ++= Seq("-Yrangepos")
