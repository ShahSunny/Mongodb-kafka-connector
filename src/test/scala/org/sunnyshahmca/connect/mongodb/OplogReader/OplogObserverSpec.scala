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
import org.mockito.Matchers
import scala.concurrent.ExecutionContext.Implicits.global


object OplogObserverTest extends mutable.Specification 
  with org.specs2.mock.Mockito
{
  val logger = LoggerFactory.getLogger(this.getClass)
  "OplogObserver" should { 
    "request oplogMaster.onSubscriptionOver records at the subscription time" >> {
      val oplogMaster = mock[oplogReader.OplogObserverMaster]
      val recordsToRequest = 10:Int
      oplogMaster.onSubscriptionOver returns recordsToRequest
      val subscription = mock[Subscription]
      
      val oplogObserver = new oplogReader.OplogObserver(oplogMaster)
      oplogObserver.onSubscribe(subscription)
      
      there was one(oplogMaster).onSubscriptionOver andThen
        one(subscription).request(recordsToRequest)
    }

    "request records when it receives all the requested records" >> {
      val oplogMaster = mock[oplogReader.OplogObserverMaster]
      val subscription = mock[Subscription]
      val recordsToRequest = 2:Int
      val recordsToRequestSecondTime = 3:Int
      oplogMaster.onSubscriptionOver returns recordsToRequest thenReturns recordsToRequestSecondTime

      val oplogObserver = new oplogReader.OplogObserver(oplogMaster)
      val firstDocument = Document("id" -> 1)
      val secondDocument = Document("id" -> 2)
      val thirdDocument = Document("id" -> 3)
      //Keeping this commented to rewrite in mockito, Current mockito code does 
      //not check inSequence(ordering of function calls)
      // inSequence {
      //   (oplogMaster.onSubscriptionOver _).expects().returning(recordsToRequest)
      //   (subscription.request _).expects(recordsToRequest)
      //   (oplogMaster.onNextDoc _).expects(firstDocument)
      //   (oplogMaster.onNextDoc _).expects(secondDocument)
      //   (oplogMaster.onSubscriptionOver _).expects().returning(recordsToRequestSecondTime)
      //   (subscription.request _).expects(recordsToRequestSecondTime)
      // }

      oplogObserver.onSubscribe(subscription)
      oplogObserver.onNext(firstDocument)
      oplogObserver.onNext(secondDocument)

      there were two(oplogMaster).onSubscriptionOver 

      there was one(subscription).request(recordsToRequest) andThen
                one(subscription).request(recordsToRequestSecondTime)

      there was one(oplogMaster).onNextDoc(firstDocument)   andThen
                one(oplogMaster).onNextDoc(secondDocument)
    }

    "call unsubscribe when disable gets called " >> {
      val oplogMaster = mock[oplogReader.OplogObserverMaster]
      val subscription = mock[Subscription]
      val recordsToRequest = 2:Int
      oplogMaster.onSubscriptionOver returns recordsToRequest
      val oplogObserver = new oplogReader.OplogObserver(oplogMaster)
      
      oplogObserver.onSubscribe(subscription)
      oplogObserver.disable
      there was one(subscription).unsubscribe
    }

    "call master's onError when it's own onError gets called" >> {
      val oplogMaster = mock[oplogReader.OplogObserverMaster]
      val oplogObserver = new oplogReader.OplogObserver(oplogMaster)
      val e = new NullPointerException()
      oplogObserver.onError(e)
      there was one(oplogMaster).onError(e)
    }

    "not call any of the subscriber or oplogObserverMaster's methods when disabled" >> {
      val oplogMaster = mock[oplogReader.OplogObserverMaster]
      val subscription = mock[Subscription]
      val recordsToRequest = 2:Int
      oplogMaster.onSubscriptionOver returns recordsToRequest
      val oplogObserver = new oplogReader.OplogObserver(oplogMaster)
      val e = new NullPointerException()
      val doc = Document("id" -> 1)
      
      oplogObserver.onSubscribe(subscription)
      oplogObserver.disable
      oplogObserver.onNext(doc)
      oplogObserver.onError(e)

      there was no(oplogMaster).onNextDoc(Matchers.any(classOf[Document]))
      there was one(oplogMaster).onSubscriptionOver
      there was no(oplogMaster).onError(Matchers.any(classOf[Throwable]))
      there was one(subscription).request(recordsToRequest)
      there was one(subscription).unsubscribe
    }
  }
}