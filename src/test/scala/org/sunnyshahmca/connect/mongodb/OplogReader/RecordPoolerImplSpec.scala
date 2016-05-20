import org.sunnyshahmca.connect.mongodb.oplogReader
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

object RecordPoolerImplSpec extends mutable.Specification 
  with org.specs2.ScalaCheck with org.scalamock.specs2.IsolatedMockFactory
{
  val logger = LoggerFactory.getLogger(this.getClass)
  "RecordPoolerImpl" should { 
    "request oplogMaster.onSubscriptionOver records at the subscription time" >> {
      val oplogMaster = mock[oplogReader.OplogObserverMaster]
      val subscription = mock[Subscription]
      val recordsToRequest = 10:Int
      val oplogObserver = new oplogReader.OplogObserver(oplogMaster)
      inSequence {
        (oplogMaster.onSubscriptionOver _).expects().returning(recordsToRequest)
        (subscription.request _).expects(recordsToRequest)
      }
      oplogObserver.onSubscribe(subscription)
      true
    }
  }
}