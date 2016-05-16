package org.sunnyshahmca.connect.mongodb
package object oplogReader {
  import org.bson.BsonTimestamp
  import scala.concurrent.{ExecutionContext, Future, Promise, blocking, Await}
  import scala.util._
  import scala.collection.immutable
  import org.mongodb.scala._
  import scala.concurrent.duration._
  import scala.concurrent.{Future, Promise}
  import org.bson.{BsonValue,BsonString, BsonObjectId, BsonTimestamp}
  import com.mongodb.CursorType
  import common._
  import org.slf4j.{Logger,LoggerFactory}
  
  val logger = LoggerFactory.getLogger(this.getClass);  

  type RequestId = Int
  type Milliseconds = Long
  type AllRecordsFuture = Future[RequestId]
  type FirstRecordFuture = Future[(Milliseconds, AllRecordsFuture)]
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

  class SleeperImpl extends Sleeper {
    def sleep[T](msSleep:Long, value:T, beforeSleepTrigger:()=>Unit, afterSleepTrigger:()=>Unit)
    (implicit ec:ExecutionContext):Future[T] = {
      Future{ 
        beforeSleepTrigger()
        blocking { 
          try{ Thread.sleep(msSleep)} catch { case _:Throwable => {} } 
          afterSleepTrigger()
          value
        } 
      }
    }
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
        (implicit ec:ExecutionContext, sl:Sleeper) : Future[Seq[(Milliseconds,Document)]] = {

    val requestFuture = recordPooler.request match {
      case Right((requestId:Int, f:FirstRecordFuture)) => { 
        logger.info("oplogDataWatcher:: requestId = {} first record future received", requestId )
        val allRecordsReceivedOrFirstRecordTimeOutFuture:Future[Int] = f.flatMap {
          case (firstRecordReceivedMs:Milliseconds, allRecordsReceived:Future[Int]) => {
            def firstRecordTimeOutFuture:Future[Int] = if(maxWaitForSubsequentRecords.toMillis > 0) {
              sl.sleep(maxWaitForSubsequentRecords.toMillis, requestId, () => {
                  logger.debug("oplogDataWatcher:: requestId = {} going into the timeout for {}",requestId, maxWaitForSubsequentRecords.toMillis);
                },() => {
                  logger.trace("oplogDataWatcher:: firstRecordTimeOutFuture timed out")
              })
            } else { 
              logger.trace("oplogDataWatcher:: first record is already timedout requestId = {} Timeout = {}", requestId, maxWaitForSubsequentRecords.toMillis); 
              Future.successful(requestId)
            }
            allRecordsReceived.onSuccess{ case _ => logger.trace("oplogDataWatcher:: allRecordsReceived onSuccess requestId = {}", requestId) }
            val firstRace = successRace(allRecordsReceived, firstRecordTimeOutFuture)
            firstRace.onComplete{ case _ => logger.info("firstRace won  requestId = {}", requestId) }
            firstRace
          }
        }
        def maxWaitTimeoutFuture = sl.sleep(maxWaitAllowed.toMillis, requestId, ()=>{}, ()=>{
            logger.trace("maxWaitTimeoutFuture done requestId = {} maxWaitAllowed = {}", requestId, maxWaitAllowed.toMillis);
        })
        val secondRace:Future[Int] = successRace(allRecordsReceivedOrFirstRecordTimeOutFuture, maxWaitTimeoutFuture)
        secondRace.onComplete{ case _ => logger.info("secondRace won requestId = {}", requestId) }
        secondRace
      }

      case Left(requestId:Int) => { 
        logger.info("all the records are already available requestId = {}", requestId); 
        Future.successful(requestId)
      }
    }

    requestFuture.map { (requestId) => {
      logger.info("request finished requestId = {}", requestId)
      recordPooler.getAllAvailableRecords
    }}
  }


  class OplogObserver(m:OplogObserverMaster) extends Observer[Document] with OplogRequester{
    var m_subscription:Subscription = null
    var receivedRecordsCounter = 0
    var requestedRecords = -1
    var isEnabled = true
    val logger = LoggerFactory.getLogger(this.getClass) 

    override def onNext(doc: Document): Unit = {
      if(isEnabled && requestedRecords > 0) {
        receivedRecordsCounter = receivedRecordsCounter + 1
        logger.debug("onNext Result [ {} / {} ]",receivedRecordsCounter,requestedRecords)
        m.onNextDoc(doc)
        if(receivedRecordsCounter == requestedRecords) {
          receivedRecordsCounter = 0
          val recordsTorequest = m.onSubscriptionOver
          logger.trace("onNext = {}", recordsTorequest) 
          if(isEnabled) requestRecords(recordsTorequest)
        }
      } else if (isEnabled && requestedRecords == 0) {
        logger.error("onNext requestedRecords is ZERO, Still received record, Skipping it! {}" + doc.toString)  
      } else {
        logger.error("onNext Disabled, Skiiping the record {}" + doc.toString)
      }    
    }

    override def onError(e: Throwable): Unit = {
      if(isEnabled){ 
        logger.error("onError error received {}", e) 
        m.onError(e)
      } else {
        logger.error("onError Disabled, Error received {}", e) 
      }
    }

    override def onSubscribe(subscription: Subscription): Unit = {
      m_subscription = subscription
      logger.trace("onSubscribe requesting {} records", requestedRecords )
      requestRecords(requestedRecords)
    }

    def requestRecords(rc:Int):Boolean = {
      logger.trace("requestRecords rc = {}",rc)
      val requestCount = if(rc < 0) m.onSubscriptionOver else rc
      logger.debug("requestRecords requestCount = {}",requestCount)
      if(receivedRecordsCounter != 0) {
        logger.error("Error: requestRecords requestRecords with receivedRecordsCounter = {}",receivedRecordsCounter)
      }
      receivedRecordsCounter = 0
      requestedRecords = requestCount
      if(requestedRecords > 0 ) {
        m_subscription.request(requestCount)
        true
      } else {
        logger.debug("requestRecords did not request, recordCount <=0 {}", requestCount)
        false
      }
    }

    override def onComplete(): Unit = { println("onComplete received, Not doing anything!")  }
    def disable:Boolean = { 
      val copyIsEnabled = isEnabled; 
      logger.debug("disabling the observer, unsubscribed!");  
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
    val logger = LoggerFactory.getLogger(this.getClass) 

    def createBlankQueue = immutable.Queue[(Long, Document)]()
    def request: Either[RequestId, RequestIDWithFirstRecordFuture] = {
      requestId = requestId + 1
      if(queueRecords.size == maxRecords) {
          logger.debug("RecordPooler::request queueRecords.size == maxRecords")
          Left(requestId)
      } else {
        this.synchronized {
          logger.debug("RecordPooler::request queueRecords.size != maxRecords")
          resetPromises()
          if(queueRecords.size >= 1) {
            logger.debug("RecordPooler::request queueRecords.size >= 1 Calling success of firstRecord Promise")
            sendSuccessForFirstRecordPromise 
          }
          recreateObserverIfNeeded
          Right((requestId,firstRecordPromise.future))
        }
      }
    }

    def sendSuccessForFirstRecordPromise =  {
      val firstRecordPromiseResult = (queueRecords.front._1, allRecordsPromise.future)
      logger.trace("RecordPooler::sendSuccessForFirstRecordPromise firstRecordPromiseResult = {}", firstRecordPromiseResult)
      firstRecordPromise.trySuccess(firstRecordPromiseResult)
    }

    def sendSuccessForAllRecordsPromise = {
      logger.trace("RecordPooler::sendSuccessForAllRecordsPromise ")
      allRecordsPromise.trySuccess(requestId)
    }

    def recreateObserverIfNeeded = {
      val currentTime = System.currentTimeMillis
      if(currentTime - lastRecordReceivedAt > observerRestartTimeout.d.toMillis) {
        logger.warn("RecordPooler::recreateObserverIfNeeded observer stale since  {}  Timeout = {}", (currentTime - lastRecordReceivedAt), observerRestartTimeout.d.toMillis )
        recreateObserver
      } else {
        logger.trace("RecordPooler::recreateObserverIfNeeded observer is active  " + (currentTime - lastRecordReceivedAt) + " Timeout = " + observerRestartTimeout.d.toMillis )
      }
    }

    def resetPromises() = {
      logger.trace("RecordPooler::resetPromises" )
      completePromises()
      firstRecordPromise  = Promise[(Milliseconds, Future[RequestId])]
      allRecordsPromise   = Promise[RequestId]
    }

    def completePromises() = {
      logger.trace("RecordPooler::completePromises" )
      val e = new RecordPoolerException("getAllAvailableRecords got called, terminating the lingering current promises")
      firstRecordPromise.tryFailure(e)
      allRecordsPromise.tryFailure(e)
    }

    def getAllAvailableRecords():Seq[(Long,Document)] = {
      logger.debug("RecordPooler::getAllAvailableRecords" )
      this.synchronized {
        val backupQueueRecords = queueRecords
        queueRecords = createBlankQueue
        completePromises
        if(backupQueueRecords.size == maxRecords) {
          logger.debug("RecordPooler::getAllAvailableRecords backupQueueRecords.size == maxRecords, requestRecords" )
          requestRecords
        }
        backupQueueRecords
      }
    }

    def createObserver = {
      logger.trace("RecordPooler::createObserver")
      oplogObservableFactory(curOplogRecordTimeStamp, this, mongoClient)
    }

    def recreateObserver = { 
      logger.trace("RecordPooler::recreateObserver")
      m_oplogObserver.disable; m_oplogObserver = createObserver 
    }
    def onError(e:Throwable) = { logger.error("RecordPooler::onError {}", e); this.synchronized { recreateObserver } }
    def requestRecords = { logger.trace("RecordPooler::requestRecords"); m_oplogObserver.requestRecords(onSubscriptionOver) }

    def onNextDoc(doc:Document) = {
      if (queueRecords.size == maxRecords) {
        logger.error("RecordPooler::onNextDoc queueRecords.size == maxRecords, Rejecting the record {}", doc)
      } else {
        this.synchronized {
          curOplogRecordTimeStamp = Some(doc("ts"))
          lastRecordReceivedAt = System.currentTimeMillis()
          if(doc("op") != noOp) {
            queueRecords = queueRecords :+ (lastRecordReceivedAt, doc)
            logger.debug("RecordPooler::onNextDoc doc is not noOp, Queue size = {}", queueRecords.size)
            if (queueRecords.size == maxRecords) {
              logger.info("RecordPooler::onNextDoc queueRecords.size == maxRecords, sendSuccessForAllRecordsPromise ")
              sendSuccessForAllRecordsPromise
            } else if (queueRecords.size == 1) {
              logger.info("RecordPooler::onNextDoc queueRecords.size == 1, sendSuccessForFirstRecordPromise ")
              sendSuccessForFirstRecordPromise
            }
          } else {
              logger.trace("RecordPooler::onNextDoc doc is noOp, Queue size = {}", queueRecords.size)
          }
        }//synchronized end
      }
    }

    def onSubscriptionOver = { 
      val recordsToRequest = maxRecords - queueRecords.size;
      logger.trace("RecordPooler::onSubscriptionOver recordsToRequest = {}", recordsToRequest)
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
        logger.info("actualOplogObservableFactory lastRecord = {}", lr)
        unFilteredCollectionRecords.filter(Document("ts" -> Document("$gt" -> lr))) 
      }
      case None => {
        logger.error("actualOplogObservableFactory No lastRecord = " )
        unFilteredCollectionRecords
      } 
    }
    logger.trace("actualOplogObservableFactory observerMaster's remaining are ", observerMaster.onSubscriptionOver) 
    val oplogObserver = new OplogObserver(observerMaster)
    collectRecords.subscribe(oplogObserver)
    oplogObserver
  }

  object Implicits {
    implicit val observerRestartTimeout = ObserverRestartTimeout(Duration(600, SECONDS))
    implicit def actualOplogObservableFactory:(Option[BsonValue], OplogObserverMaster, MongoClient) => OplogRequester = mongodbOplogObservableFactory
  }//End of object Implicits

  def testOplogDataWatcher(maxWaitAllowed:Long, maxWaitForSubsequentRecords:Long, firstRecordDelay:Long, secondRecordDelay:Long):Boolean = {
    import ExecutionContext.Implicits.global

    implicit object dummySleeper extends Sleeper {
      var listPromises = List[(Promise[Int], Long, Int)]()
      def sleep[T](msSleep:Long, value:T, beforeSleepTrigger:()=>Unit = ()=>{}, afterSleepTrigger:()=>Unit = ()=>{})
      (implicit ec:ExecutionContext):Future[T] = {
        this.synchronized {
          val f = Promise[T]
          val newPromise = (f.asInstanceOf[Promise[Int]],msSleep,value.asInstanceOf[Int])
          logger.info("dummySleeper::sleep newPromise = {}", newPromise)
          listPromises = newPromise :: listPromises
          f.future
        }
      }

      def done():Unit = {
        this.synchronized {
          listPromises = listPromises.sortWith( (a,b) => {(a._2 < b._2 || ((a._2 == b._2) && (a._3 < b._3)))})
          val first = listPromises.head
          logger.info("testOplogDataWatcher::done {}", listPromises)
          listPromises = listPromises.takeRight(listPromises.size - 1)
          first._1.success(first._3)
        }
      }
    }

    object recordPooler extends RecordPooler {
      var promiseFirst = Promise[(Milliseconds, AllRecordsFuture)]
      var secondRecordFuture:Future[RequestId] = null

      def request: Either[RequestId, RequestIDWithFirstRecordFuture] = {
        val firstRecordFuture = dummySleeper.sleep(firstRecordDelay,1:Int)
        secondRecordFuture = dummySleeper.sleep(firstRecordDelay + secondRecordDelay,2:Int)
        secondRecordFuture.onSuccess{case _ => {logger.info("secondRecordFuture's onSuccess got called")}}
        promiseFirst.completeWith(firstRecordFuture.map( (requestId) => {
          Future{ blocking{ Thread.sleep(1); dummySleeper.done() } }
          (requestId, secondRecordFuture)
        }))
        return Right(3,promiseFirst.future)
      }

      def getAllAvailableRecords():Seq[(Milliseconds,Document)] = {
        if(promiseFirst.isCompleted && secondRecordFuture.isCompleted) 
          List((1,Document("id" -> 1)),(2, Document("id" -> 2)))
        else if ( promiseFirst.isCompleted )
          List((1,Document("id" -> 1)))
        else 
          List[(Milliseconds,Document)]()
      }
    }
    
    val maxDelayAfterFirstRecord = maxWaitAllowed - firstRecordDelay
    val expectedCount = if(firstRecordDelay > maxWaitAllowed ) {
      0
    } else if ( secondRecordDelay > maxDelayAfterFirstRecord ||  secondRecordDelay > maxWaitForSubsequentRecords ) {
      1
    } else {
      2
    }

    val result = oplogDataWatcher(recordPooler, Duration(maxWaitAllowed, MILLISECONDS), Duration(maxWaitForSubsequentRecords, MILLISECONDS))
    logger.info("Calling Done, Expected count = {}", expectedCount)
    dummySleeper.done()
    Await.result(result, 100 millis)
    logger.info("After Done {}", result)
    val isSuccess = result.value.get match {
      case Success(l) => { 
        if(l.size == expectedCount)  { 
          logger.info("l.size {} == expectedCount {}", l.size, expectedCount); true
        } else { 
          logger.info("l.size {} != expectedCount {}", l.size, expectedCount); false
        }
      }
      case _ => logger.info("Failure"); false
    }
    isSuccess
  }
}
