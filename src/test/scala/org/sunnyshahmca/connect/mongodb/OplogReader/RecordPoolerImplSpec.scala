import org.sunnyshahmca.connect.mongodb.oplogReader
import oplogReader.{OplogObserverCreator,OplogRequester,RecordPoolerImpl}
import oplogReader.{Milliseconds,RequestId,AllRecordsFuture}
import oplogReader.{FirstRecordFuture,RequestIDWithFirstRecordFuture}
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
import org.bson.{BsonValue,BsonString, BsonObjectId, BsonTimestamp}
import scala.reflect.runtime.universe._
import scala.concurrent.ExecutionContext.Implicits.global

object RecordPoolerImplSpec extends mutable.Specification 
   with org.specs2.mock.Mockito 
{
  val logger = LoggerFactory.getLogger(this.getClass)
  "RecordPoolerImpl should" >> { 
    "return incomplete future when no records are available and request() is invoked" >> { 
      val lastOplogRecordTimeStamp:Option[BsonValue] = None
      val maxRecordsPerRequest = 2:Int
      val oplogRequester = mock[OplogRequester]
      val oplogObserverCreator = mock[OplogObserverCreator].defaultReturn(oplogRequester)
      var currentTime = 0:Long
      implicit def currentTimeMillis():Long = { currentTime }
      
      val recordPooler = new RecordPoolerImpl(lastOplogRecordTimeStamp, 
                              maxRecordsPerRequest,oplogObserverCreator)
      val result = recordPooler.request match {
        case Left(r:RequestId) => {
          throw new IllegalStateException("Received all records completed future " + r); 
          false
        }
        case Right((_, f:FirstRecordFuture @unchecked)) => { 
          if(f.isCompleted) { throw new IllegalStateException("Received completed future " + f) }
          true
        }
      }
      result must_== true
    }

    ("return future with completed first record future when" +
      "first record is available when request() is invoked") >> { 
      val lastOplogRecordTimeStamp:Option[BsonValue] = None
      val maxRecordsPerRequest = 2:Int
      val oplogRequester = mock[OplogRequester]
      val oplogObserverCreator = mock[OplogObserverCreator].defaultReturn(oplogRequester)
      var currentTime = 0:Long
      implicit def currentTimeMillis():Long = { currentTime }
      
      val recordPooler = new RecordPoolerImpl(lastOplogRecordTimeStamp, 
                              maxRecordsPerRequest,oplogObserverCreator)
      val ts = new BsonTimestamp(1,1)
      recordPooler.onNextDoc(Document("id" -> 1, "op" -> "i","ts" -> ts))
      recordPooler.request match {
        case Left(r:RequestId) => {
          throw new IllegalStateException("Received all records completed future " + r); 
          false
        }
        case Right((_, f:FirstRecordFuture @unchecked)) => { 
          if(!f.isCompleted) { throw new IllegalStateException("Received completed future ") }
          if(f.value.get.get._2.isCompleted) { throw new IllegalStateException("All records completed future ") }
          true
        }
      }
    }
    ("return future with completed all records future when maxRecords" +
     "records are already available when request() is invoked") >> { 
      val lastOplogRecordTimeStamp:Option[BsonValue] = None
      val maxRecordsPerRequest = 2:Int
      val oplogRequester = mock[OplogRequester]
      val oplogObserverCreator = mock[OplogObserverCreator].defaultReturn(oplogRequester)
      var currentTime = 0:Long
      implicit def currentTimeMillis():Long = { currentTime }
      
      val recordPooler = new RecordPoolerImpl(lastOplogRecordTimeStamp, 
                              maxRecordsPerRequest,oplogObserverCreator)
      val ts1 = new BsonTimestamp(1,1)
      recordPooler.onNextDoc(Document("id" -> 1, "op" -> "i","ts" -> ts1))
      val ts2 = new BsonTimestamp(2,1)
      recordPooler.onNextDoc(Document("id" -> 2, "op" -> "i","ts" -> ts2))

      recordPooler.request match {
        case Left(r:RequestId) => { true }
        case Right(_) => { 
          throw  new IllegalStateException("Received completed future"); false
        }
      }
    }
    
    "Complete first record future when it receives first record" >> { 
      val lastOplogRecordTimeStamp:Option[BsonValue] = None
      val maxRecordsPerRequest = 2:Int
      val oplogRequester = mock[OplogRequester]
      val oplogObserverCreator = mock[OplogObserverCreator].defaultReturn(oplogRequester)
      var currentTime = 0:Long
      implicit def currentTimeMillis():Long = { currentTime }
      
      val recordPooler = new RecordPoolerImpl(lastOplogRecordTimeStamp, 
                              maxRecordsPerRequest,oplogObserverCreator)
      val ts1 = new BsonTimestamp(1,1)
      recordPooler.onNextDoc(Document("id" -> 1, "op" -> "i","ts" -> ts1))

      recordPooler.request match {
        case Left(r:RequestId) => { new IllegalStateException("Received left, all records available"); false }
        case Right((_, f:FirstRecordFuture @unchecked)) => { 
          Await.ready(f, Duration(1, SECONDS))
          if(f.value.get.get._2.isCompleted) { throw new IllegalStateException("All records completed future") }
          recordPooler.getAllAvailableRecords.size == 1
        }
      }
    }

    "Complete first record and all records future when it receives all the records" >> { 
      val lastOplogRecordTimeStamp:Option[BsonValue] = None
      val maxRecordsPerRequest = 2:Int
      val oplogRequester = mock[OplogRequester]
      val oplogObserverCreator = mock[OplogObserverCreator].defaultReturn(oplogRequester)
      var currentTime = 0:Long
      implicit def currentTimeMillis():Long = { currentTime }
      
      val recordPooler = new RecordPoolerImpl(lastOplogRecordTimeStamp, 
                              maxRecordsPerRequest,oplogObserverCreator)
      val ts1 = new BsonTimestamp(1,1)
      val ts2 = new BsonTimestamp(2,1)

      recordPooler.request match {
        case Left(r:RequestId) => { new IllegalStateException("Received left, all records available"); false }
        case Right((_, f:FirstRecordFuture @unchecked)) => { 
          recordPooler.onNextDoc(Document("id" -> 1, "op" -> "i","ts" -> ts1))
          recordPooler.onNextDoc(Document("id" -> 2, "op" -> "i","ts" -> ts2))
          Await.ready(f, Duration(1, SECONDS))
          Await.ready(f.value.get.get._2, Duration(1, SECONDS))
          recordPooler.getAllAvailableRecords.size == 2
        }
      }
    }
    ("Complete first record and all records futures with success" + 
      "when it has no records and getAllAvailableRecords is invoked") >>  { 
      val lastOplogRecordTimeStamp:Option[BsonValue] = None
      val maxRecordsPerRequest = 2:Int
      val oplogRequester = mock[OplogRequester]
      val oplogObserverCreator = mock[OplogObserverCreator].defaultReturn(oplogRequester)
      var currentTime = 0:Long
      implicit def currentTimeMillis():Long = { currentTime }
      
      val recordPooler = new RecordPoolerImpl(lastOplogRecordTimeStamp, 
                              maxRecordsPerRequest,oplogObserverCreator)
      recordPooler.request match {
        case Left(r:RequestId) => { new IllegalStateException("Received left, all records available"); false }
        case Right((_, f:FirstRecordFuture @unchecked)) => { 
          val availableRecords = recordPooler.getAllAvailableRecords 
          Await.ready(f, Duration(1, SECONDS))
          val hasFirstRecordFutureFailed = f.value match {
            case Some(Failure(_)) => true
            case _ => false
          }
          (availableRecords.size == 0 && hasFirstRecordFutureFailed) 
        }
      }
    }
    
    ("Complete all records future with failure when it has only one record" + 
      "and getAllAvailableRecords is invoked") >>  { 
      val lastOplogRecordTimeStamp:Option[BsonValue] = None
      val maxRecordsPerRequest = 2:Int
      val oplogRequester = mock[OplogRequester]
      val oplogObserverCreator = mock[OplogObserverCreator].defaultReturn(oplogRequester)
      var currentTime = 0:Long
      implicit def currentTimeMillis():Long = { currentTime }
      
      val recordPooler = new RecordPoolerImpl(lastOplogRecordTimeStamp, 
                              maxRecordsPerRequest,oplogObserverCreator)
      recordPooler.request match {
        case Left(r:RequestId) => { new IllegalStateException("Received left, all records available"); false }
        case Right((_, f:FirstRecordFuture @unchecked)) => { 
          val ts1 = new BsonTimestamp(1,1)
          recordPooler.onNextDoc(Document("id" -> 1, "op" -> "i","ts" -> ts1))
          val availableRecords = recordPooler.getAllAvailableRecords 
          Await.ready(f, Duration(1, SECONDS))
          val hasAllRecordsRecordsFutureFailed = f.value match {
            case Some(Success((_,allRecordsFuture:Future[RequestId]))) => {
              Await.ready(allRecordsFuture, Duration(1, SECONDS))
              allRecordsFuture.value match {
                case Some(Failure(_)) => true
                case _ => false
              }
            }
            case _ => false
          }
          (availableRecords.size == 1 && hasAllRecordsRecordsFutureFailed) 
        }
      }
    }
    
    ("call OplogObserverCreator::recreateIfNeeded when getAllAvailableRecords is invoked, " + 
      "It should call requestRecords from observer when queueSize == maxRecords") >> { 
      val lastOplogRecordTimeStamp:Option[BsonValue] = None
      val maxRecordsPerRequest = 2:Int
      val oplogRequester = mock[OplogRequester]

      val oplogObserverCreator = mock[OplogObserverCreator].defaultReturn(oplogRequester)
      var currentTime = 1:Long
      implicit def currentTimeMillis():Long = { currentTime }
      
      val recordPooler = new RecordPoolerImpl(lastOplogRecordTimeStamp, 
                              maxRecordsPerRequest,oplogObserverCreator)
      val ts1 = new BsonTimestamp(1,1)
      val ts2 = new BsonTimestamp(2,1)
      val ts3 = new BsonTimestamp(3,1) 
      recordPooler.onNextDoc(Document("id" -> 1, "op" -> "i","ts" -> ts1))
      currentTime = 2
      recordPooler.onNextDoc(Document("id" -> 2, "op" -> "n","ts" -> ts2))
      recordPooler.request
      recordPooler.onNextDoc(Document("id" -> 3, "op" -> "i","ts" -> ts3))
      recordPooler.getAllAvailableRecords
      
      there was one(oplogObserverCreator).recreateIfNeeded(currentTime, 
          Some(ts2),recordPooler,oplogRequester)
      there was one(oplogRequester).requestRecords(maxRecordsPerRequest)
      
    }
    
    ("Invoke OplogObserverCreator::recreateObserver" 
      + "when onError is invoked and futures still successfully completed with new oplogRequester") >> { 
      //Check that the lastOplogRecordTimestamp and lastRecordReceivedTime is accurate
      val lastOplogRecordTimeStamp:Option[BsonValue] = None
      val maxRecordsPerRequest = 2:Int
      val oplogRequester = mock[OplogRequester]

      val oplogObserverCreator = mock[OplogObserverCreator].defaultReturn(oplogRequester)
      var currentTime = 1:Long
      implicit def currentTimeMillis():Long = { currentTime }
      
      val recordPooler = new RecordPoolerImpl(lastOplogRecordTimeStamp, 
                              maxRecordsPerRequest,oplogObserverCreator)
      val ts1 = new BsonTimestamp(1,1)
      val ts2 = new BsonTimestamp(2,1)
      val ts3 = new BsonTimestamp(3,1) 
      recordPooler.onNextDoc(Document("id" -> 1, "op" -> "i","ts" -> ts1))
      currentTime = 2
      recordPooler.onNextDoc(Document("id" -> 2, "op" -> "n","ts" -> ts2))
      val result = recordPooler.request match {
        case Left(r:RequestId) => { new IllegalStateException("Received left, all records available"); false }
        case Right((_, f:FirstRecordFuture @unchecked)) => { 
          recordPooler.onError(new Throwable("Test"))
          recordPooler.onNextDoc(Document("id" -> 3, "op" -> "i","ts" -> ts3))
          Await.ready(f, Duration(1, SECONDS))
          Await.ready(f.value.get.get._2, Duration(1, SECONDS))
          recordPooler.getAllAvailableRecords.size == 2
        }
      }
      
      there was one(oplogObserverCreator).recreate(Some(ts2),recordPooler,oplogRequester)
      result must_== true
    }
  }
}