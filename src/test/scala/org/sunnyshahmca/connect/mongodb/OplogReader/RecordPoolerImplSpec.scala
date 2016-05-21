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
    "return incomplete future when no records are available when request() is invoked" >> { true }
    "return future with completed first record future" + 
      "when first record is available when request() is invoked" >> { true }
    "return future with completed all records future when" + 
      "maxRecords records are already available when request() is invoked" >> { true }
    "Complete first record future when it receives first record" >> { true }
    "Complete first record and all records future when it receives all the records" >> { true }
    "Complete first record and all records futures with error" + 
      "when it has no records and getAllAvailableRecords is invoked" >> { true }
    "Complete all records future when it has only one record" + 
      "and getAllAvailableRecords is invoked" >> { true }
    "Complete all records future when it has only one record" + 
      "and getAllAvailableRecords is invoked" >> { true }
    "requestRecords from observer when getAllAvailableRecords is invoked" + 
      "and queueSize = maxRecords" >> { true }
    
    "Invoke OplogObserverCreator::recreateObserverIfNeeded when request() is invoked " >> { 
      //Check that the lastOplogRecordTimestamp and lastRecordReceivedTime is accurate
      true 
    }
    
    "Invoke OplogObserverCreator::recreateObserver when onError() is invoked " >> { 
      //Check that the lastOplogRecordTimestamp and lastRecordReceivedTime is accurate
      true 
    }
  }
}