name := "Mongodb-kafka-connector"
organization := "org.sunnyshahmca"
version := "0.1"
scalaVersion := "2.11.8"

libraryDependencies ++= Seq (
    "org.scalacheck" %% "scalacheck" % "1.13.0" ,
    "org.specs2" %% "specs2-scalacheck" % "3.7.1" ,
    "org.specs2" %% "specs2-core" % "3.7" 
)
libraryDependencies += "org.apache.avro" % "avro" % "1.8.0"
libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "1.1.1"

scalacOptions in Test ++= Seq("-Yrangepos")
