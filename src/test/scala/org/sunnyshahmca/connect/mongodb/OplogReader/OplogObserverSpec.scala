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


object OplogObserverTest extends mutable.Specification 
  with org.specs2.ScalaCheck with org.scalamock.specs2.IsolatedMockFactory
{
  val logger = LoggerFactory.getLogger(this.getClass)
  "OplogObserver" should { 
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

    "request records when it receives all the requested records" >> {
      val oplogMaster = mock[oplogReader.OplogObserverMaster]
      val subscription = mock[Subscription]
      val recordsToRequest = 2:Int
      val recordsToRequestSecondTime = 3:Int
      val oplogObserver = new oplogReader.OplogObserver(oplogMaster)
      val firstDocument = Document("id" -> 1)
      val secondDocument = Document("id" -> 2)
      inSequence {
        (oplogMaster.onSubscriptionOver _).expects().returning(recordsToRequest)
        (subscription.request _).expects(recordsToRequest)
        (oplogMaster.onNextDoc _).expects(firstDocument)
        (oplogMaster.onNextDoc _).expects(secondDocument)
        (oplogMaster.onSubscriptionOver _).expects().returning(recordsToRequestSecondTime)
        (subscription.request _).expects(recordsToRequestSecondTime)
      }
      oplogObserver.onSubscribe(subscription)
      oplogObserver.onNext(firstDocument)
      oplogObserver.onNext(secondDocument)
      true
    }

    "call unsubscribe when disable gets called " >> {
      val oplogMaster = mock[oplogReader.OplogObserverMaster]
      val subscription = mock[Subscription]
      val recordsToRequest = 2:Int
      val oplogObserver = new oplogReader.OplogObserver(oplogMaster)
      inSequence {
        (oplogMaster.onSubscriptionOver _).expects().once.returning(recordsToRequest)
        (subscription.request _).expects(recordsToRequest)
        (subscription.unsubscribe _).expects().once
      }
      oplogObserver.onSubscribe(subscription)
      oplogObserver.disable
      true
    }

    "call master's onError when it's own onError gets called" >> {
      val oplogMaster = mock[oplogReader.OplogObserverMaster]
      val oplogObserver = new oplogReader.OplogObserver(oplogMaster)
      val e = new NullPointerException()
      (oplogMaster.onError _).expects(e).once
      oplogObserver.onError(e)
      true
    }

    "not call any of the subscriber or oplogObserverMaster's methods when disabled" >> {
      val oplogMaster = mock[oplogReader.OplogObserverMaster]
      val subscription = mock[Subscription]
      val recordsToRequest = 2:Int
      val oplogObserver = new oplogReader.OplogObserver(oplogMaster)
      val e = new NullPointerException()
      val doc = Document("id" -> 1)
      (oplogMaster.onNextDoc _).expects(*).never
      (oplogMaster.onSubscriptionOver _).expects().once.returning(recordsToRequest)
      (oplogMaster.onError _).expects(*).never
      (subscription.request _).expects(recordsToRequest).once
      (subscription.unsubscribe _).expects().once
      
      oplogObserver.onSubscribe(subscription)
      oplogObserver.disable
      oplogObserver.onNext(doc)
      oplogObserver.onError(e)
      true
    }
  }
}