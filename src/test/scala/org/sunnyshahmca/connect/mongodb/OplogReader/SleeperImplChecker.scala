import org.sunnyshahmca.connect.mongodb.oplogReader

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise, blocking, Await}
import scala.util._
import org.specs2._
import org.scalacheck.{Gen,Properties,Prop}
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen.{choose, frequency,listOf, alphaStr, numChar}
import org.slf4j.{Logger,LoggerFactory}
import org.sunnyshahmca.connect.mongodb.common._
import org.mongodb.scala._

import scala.concurrent.ExecutionContext.Implicits.global

object SleepImplChecker extends mutable.Specification 
  with org.specs2.ScalaCheck with org.scalamock.specs2.IsolatedMockFactory
{
  val logger = LoggerFactory.getLogger(this.getClass);
  "SleeperImpl" should { 
    "Call before and after callbacks" >>  {
      val mockedBeforeCallback = mockFunction[Unit]
      val mockedAfterCallback = mockFunction[Unit]
      inSequence {
        mockedBeforeCallback expects ()
        mockedAfterCallback expects ()
      }
      val sleeper = new oplogReader.SleeperImpl
      val sleepDurationMs:Long = 1
      val requestId = 2
      val result = sleeper.sleep(sleepDurationMs, requestId, mockedBeforeCallback, mockedAfterCallback)
      Await.result(result, 1000 millis)
      result.value.get.get must_== requestId
    }
  }
}