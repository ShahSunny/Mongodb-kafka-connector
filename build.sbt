name := "Mongodb-kafka-connector"
organization := "org.sunnyshah"
version := "0.1"
scalaVersion := "2.11.8"
resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"
libraryDependencies ++= Seq (
    "org.scalacheck" %% "scalacheck" % "1.13.1" % "test",
    "org.specs2" %% "specs2-scalacheck" % "3.8.2" % "test",
    "org.specs2" %% "specs2-core" % "3.8.2" % "test",
    "org.specs2" %% "specs2-mock" % "3.8.2" % "test",
    "com.whisk" %% "docker-testkit-specs2" % "0.8.1" % "test"
)

//libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"
libraryDependencies += "org.apache.avro" % "avro" % "1.8.0"
libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "1.1.1"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7"
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.21"

scalacOptions += "-deprecation"
scalacOptions in Test ++= Seq("-Yrangepos")

