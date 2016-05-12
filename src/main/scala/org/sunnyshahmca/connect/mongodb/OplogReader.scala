package org.sunnyshahmca.connect.mongodb
package object OplogReader {
  import org.bson.BsonTimestamp
  import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
  import scala.util._
  import scala.collection.immutable
  import org.mongodb.scala._
  import scala.concurrent.duration._
  import scala.concurrent.{Future, Promise}
  import org.bson.{BsonValue,BsonString, BsonObjectId, BsonTimestamp}
  import com.mongodb.CursorType
  import Common._
  
  type RequestId = Int
  type AllRecordsFuture = Future[Int]
  type FirstRecordFuture = Future[(Long, AllRecordsFuture)]
  type RequestIDWithFirstRecordFuture = (RequestId,FirstRecordFuture)

  trait OplogObserverMaster {
    def onNextDoc(d:Document):Unit 
    def onSubscriptionOver:Int 
    def onError(t:Throwable):Unit 
  }

  trait OplogRequester {
    def requestRecords(rc:Int):Boolean
    def disable:Boolean
  }

  trait RecordPooler {
    def request: Either[RequestId, RequestIDWithFirstRecordFuture]
    def getAllAvailableRecords():Seq[(Long,Document)]
  }

  /* This function returns a future which need to complete in below cases
          - Successful completion
              - When we get the maxRecords
              - When we maxWeightAllowed is passed without any record
              - When the maxWaitForSubsequentRecords has passed since the timeOfFirstRecordReceived
  This code deals with the four futures. 
          - maxWeightAllowedTimeout future
          - firstRecordReceived future
          - allRecordsAvailable future
          - firstRecordStalenessTimeout future */
  def oplogDataWatcher(recordPooler:RecordPooler, maxWaitAllowed:Duration, maxWaitForSubsequentRecords:Duration)  
        (implicit ec:ExecutionContext) : Future[Seq[(Long,Document)]] = {

      val requestFuture = recordPooler.request match {
        case Right((requestId:Int, f:FirstRecordFuture)) => { 
          println("oplogDataWatcher:: requestId = " + requestId + "first record future received")
          val allRecordsReceivedOrFirstRecordTimeOutFuture:Future[Int] = f.flatMap {
            case (firstRecordReceivedMs:Long, allRecordsReceived:Future[Int]) => {
              val firstRecordFreshnessTimeout:Long =  (firstRecordReceivedMs + maxWaitForSubsequentRecords.toMillis) - System.currentTimeMillis()
              def firstRecordTimeOutFuture = if(firstRecordFreshnessTimeout > 0) {
                println("oplogDataWatcher:: requestId = " + requestId + " " + firstRecordFreshnessTimeout + " going into the timeout!"); 
                Future{ blocking { 
                  try{Thread.sleep(firstRecordFreshnessTimeout)} catch { case _:Throwable => {} } 
                  println("oplogDataWatcher:: requestId = " + requestId + " Timeout = " + firstRecordFreshnessTimeout)
                  requestId
                } }
              } else { 
                println("oplogDataWatcher:: first record is already timedout requestId = " + requestId + firstRecordFreshnessTimeout); 
                Future.successful(requestId)
              }
              allRecordsReceived.onSuccess{ case _ => println("oplogDataWatcher:: allRecordsReceived onSuccess requestId = " + requestId) }
              val firstRace = successRace(allRecordsReceived, firstRecordTimeOutFuture)
              firstRace.onComplete{ case _ => println("firstRace won  requestId = " + requestId) }
              firstRace
            }
          }
          def maxWaitTimeoutFuture = Future { blocking { try{Thread.sleep(maxWaitAllowed.toMillis)} catch { case _:Throwable => {} } 
            println("maxWaitTimeoutFuture done requestId = " + requestId  + maxWaitAllowed.toMillis);
            requestId
          } }
          val secondRace:Future[Int] = successRace(allRecordsReceivedOrFirstRecordTimeOutFuture, maxWaitTimeoutFuture)
          secondRace.onComplete{ case _ => println("secondRace won requestId = " + requestId) }
          secondRace
        }

        case Left(requestId:Int) => { 
          println("all the records are already available requestId = " + requestId); 
          Future.successful(requestId)
        }
      }

      requestFuture.map { (requestId) => {
        println("request finished requestId = " + requestId)
        recordPooler.getAllAvailableRecords
      }}
  }


  class OplogObserver(m:OplogObserverMaster) extends Observer[Document] with OplogRequester{
    var m_subscription:Subscription = null
    var receivedRecordsCounter = 0
    var requestedRecords = -1
    var isEnabled = true
    
    override def onNext(doc: Document): Unit = {
      if(isEnabled && requestedRecords > 0) {
        receivedRecordsCounter = receivedRecordsCounter + 1
        println("OplogObserver:: onNext Result [" + receivedRecordsCounter+"/"+requestedRecords+"]: " + doc.toString)
        m.onNextDoc(doc)
        if(receivedRecordsCounter == requestedRecords) {
          receivedRecordsCounter = 0
          val recordsTorequest = m.onSubscriptionOver
          println("OplogObserver::onNext = " + recordsTorequest) 
          if(isEnabled) requestRecords(recordsTorequest)
        }
      } else if (isEnabled && requestedRecords == 0) {
        System.err.println("OplogObserver::onNext requestedRecords is ZERO, Still received record, Skipping it! " + doc.toString)  
      } else {
        println("OplogObserver::onNext Disabled, Skiiping the record " + doc.toString)
      }    
    }

    override def onError(e: Throwable): Unit = {
      if(isEnabled){ 
        println("OplogObserver::onError error received " + e) 
        m.onError(e)
      } else {
        println("OplogObserver::onError Disabled, Error received " + e) 
      }
    }

    override def onSubscribe(subscription: Subscription): Unit = {
      m_subscription = subscription
      println("OplogObserver::onSubscribe requesting " + requestedRecords + " records ")
      requestRecords(requestedRecords)
    }

    def requestRecords(rc:Int):Boolean = {
      println("OplogObserver::requestRecords rc = " + rc)
      val requestCount = if(rc < 0) m.onSubscriptionOver else rc
      println("OplogObserver::requestRecords requestCount = " + requestCount)
      if(receivedRecordsCounter != 0) {
        System.err.println("Error: OplogObserver::requestRecords requestRecords with receivedRecordsCounter = " + receivedRecordsCounter)
      }
      receivedRecordsCounter = 0
      requestedRecords = requestCount
      if(requestedRecords > 0 ) {
        m_subscription.request(requestCount)
        true
      } else {
        println("OplogObserver::requestRecords did not request, recordCount <=0 " + requestCount)
        false
      }
    }

    override def onComplete(): Unit = { println("onComplete received, Not doing anything!")  }
    def disable:Boolean = { 
      val copyIsEnabled = isEnabled; 
      println("disabling the observer, unsubscribed!");  
      isEnabled = false; 
      m_subscription.unsubscribe 
      copyIsEnabled
    }
  }

  case class RecordPoolerException(msg:String)  extends Exception

  //This class's responsibility is to maintain the cursor to the cluster
  //If cursor gets an error or disconnected then this class need to recreate the cursor
  //It also keeps track of the maxTimeStamp so that at the time of reconnection, 
  //there won't be duplication of records 
  //It need to pull the data from the cluster and put it into the threadSafeQueue
  case class ObserverRestartTimeout(d:Duration)

  class RecordPoolerImpl(lastOplogRecordTimeStamp:Option[BsonValue], maxRecords:Int)
  (implicit oplogObservableFactory:(Option[BsonValue], OplogObserverMaster, MongoClient) => OplogRequester,
   observerRestartTimeout:ObserverRestartTimeout,
   mongoClient:MongoClient) 
  extends OplogObserverMaster with RecordPooler {
    val noOp                    = new BsonString("n")

    var curOplogRecordTimeStamp = lastOplogRecordTimeStamp
    var queueRecords            = createBlankQueue
    var m_oplogObserver         = createObserver
    var firstRecordPromise      = Promise[(Long, Future[RequestId])]
    var allRecordsPromise       = Promise[RequestId]
    var lastRecordReceivedAt    = System.currentTimeMillis

    var requestId:Int = 0

    def createBlankQueue = immutable.Queue[(Long, Document)]()
    def request: Either[RequestId, RequestIDWithFirstRecordFuture] = {
      requestId = requestId + 1
      if(queueRecords.size == maxRecords) {
          println("RecordPooler::request queueRecords.size == maxRecords")
          Left(requestId)
      } else {
        this.synchronized {
          println("RecordPooler::request queueRecords.size != maxRecords")
          resetPromises()
          if(queueRecords.size >= 1) {
            println("RecordPooler::request queueRecords.size >= 1 Calling success of firstRecord Promise")
            sendSuccessForFirstRecordPromise 
          }
          recreateObserverIfNeeded
          Right((requestId,firstRecordPromise.future))
        }
      }
    }

    def sendSuccessForFirstRecordPromise =  {
      val firstRecordPromiseResult = (queueRecords.front._1, allRecordsPromise.future)
      println("RecordPooler::sendSuccessForFirstRecordPromise firstRecordPromiseResult = " + firstRecordPromiseResult)
      firstRecordPromise.trySuccess(firstRecordPromiseResult)
    }

    def sendSuccessForAllRecordsPromise = {
      println("RecordPooler::sendSuccessForAllRecordsPromise ")
      allRecordsPromise.trySuccess(requestId)
    }

    def recreateObserverIfNeeded = {
      val currentTime = System.currentTimeMillis
      if(currentTime - lastRecordReceivedAt > observerRestartTimeout.d.toMillis) {
        println("RecordPooler::recreateObserverIfNeeded observer stale since  " + (currentTime - lastRecordReceivedAt) + " Timeout = " + observerRestartTimeout.d.toMillis )
        recreateObserver
      } else {
        println("RecordPooler::recreateObserverIfNeeded observer is active  " + (currentTime - lastRecordReceivedAt) + " Timeout = " + observerRestartTimeout.d.toMillis )
      }
    }

    def resetPromises() = {
      println("RecordPooler::resetPromises" )
      completePromises()
      firstRecordPromise  = Promise[(Long, Future[Int])]
      allRecordsPromise   = Promise[Int]
    }

    def completePromises() = {
      println("RecordPooler::completePromises" )
      val e = new RecordPoolerException("getAllAvailableRecords got called, terminating the lingering current promises")
      firstRecordPromise.tryFailure(e)
      allRecordsPromise.tryFailure(e)
    }

    def getAllAvailableRecords():Seq[(Long,Document)] = {
      println("RecordPooler::getAllAvailableRecords" )
      this.synchronized {
        val backupQueueRecords = queueRecords
        queueRecords = createBlankQueue
        completePromises
        if(backupQueueRecords.size == maxRecords) {
          println("RecordPooler::getAllAvailableRecords backupQueueRecords.size == maxRecords, requestRecords" )
          requestRecords
        }
        backupQueueRecords
      }
    }

    def createObserver = {
      println("RecordPooler::createObserver")
      oplogObservableFactory(curOplogRecordTimeStamp, this, mongoClient)
    }

    def recreateObserver = { 
      println("RecordPooler::recreateObserver")
      m_oplogObserver.disable; m_oplogObserver = createObserver 
    }
    def onError(e:Throwable) = { println("RecordPooler::onError" + e); this.synchronized { recreateObserver } }
    def requestRecords = { println("RecordPooler::requestRecords"); m_oplogObserver.requestRecords(onSubscriptionOver) }

    def onNextDoc(doc:Document) = {
      if (queueRecords.size == maxRecords) {
        System.err.println("ERROR: RecordPooler::onNextDoc queueRecords.size == maxRecords, Rejecting the record " + doc)
      } else {
        this.synchronized {
          curOplogRecordTimeStamp = Some(doc("ts"))
          lastRecordReceivedAt = System.currentTimeMillis()
          if(doc("op") != noOp) {
            queueRecords = queueRecords :+ (lastRecordReceivedAt, doc)
            println("RecordPooler::onNextDoc doc is not noOp, Queue size = " + queueRecords.size)
            if (queueRecords.size == maxRecords) {
              println("RecordPooler::onNextDoc queueRecords.size == maxRecords, sendSuccessForAllRecordsPromise ")
              sendSuccessForAllRecordsPromise
            } else if (queueRecords.size == 1) {
              println("RecordPooler::onNextDoc queueRecords.size == 1, sendSuccessForFirstRecordPromise ")
              sendSuccessForFirstRecordPromise
            }
          } else {
              println("RecordPooler::onNextDoc doc is noOp, Queue size = " + queueRecords.size)
          }
        }//synchronized end
      }
    }

    def onSubscriptionOver = { 
      val recordsToRequest = maxRecords - queueRecords.size;  
      println("RecordPooler::onSubscriptionOver recordsToRequest = " + recordsToRequest)
      recordsToRequest
    }
  }
  

  def mongodbOplogObservableFactory(lastRecord:Option[BsonValue], observerMaster:OplogObserverMaster, mongoClient:MongoClient): OplogRequester = {
    val database: MongoDatabase = mongoClient.getDatabase("local")
    val collection: MongoCollection[Document] = database.getCollection("oplog.rs") 
    val filterDocument = Document("op" -> Document("$ne" -> "n"))
    val unFilteredCollectionRecords = collection.find(filterDocument).cursorType(CursorType.TailableAwait)
    val collectRecords = lastRecord match {
      case Some(lr:BsonValue) => {
        println("actualOplogObservableFactory lastRecord = " + lr)
        unFilteredCollectionRecords.filter(Document("ts" -> Document("$gt" -> lr))) 
      }
      case None => {
        println("actualOplogObservableFactory No lastRecord = " )
        unFilteredCollectionRecords
      } 
    }
    println("actualOplogObservableFactory observerMaster's remaining are " + observerMaster.onSubscriptionOver) 
    val oplogObserver = new OplogObserver(observerMaster)
    collectRecords.subscribe(oplogObserver)
    oplogObserver
  }

  object Implicits {
    implicit val observerRestartTimeout = ObserverRestartTimeout(Duration(600, SECONDS))
    implicit def actualOplogObservableFactory:(Option[BsonValue], OplogObserverMaster, MongoClient) => OplogRequester = mongodbOplogObservableFactory
  }//End of object Implicits
}
