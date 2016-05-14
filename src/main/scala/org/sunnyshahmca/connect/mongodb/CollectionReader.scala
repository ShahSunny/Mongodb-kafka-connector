package org.sunnyshahmca.connect.mongodb

package object CollectionReader {
  import org.mongodb.scala.{MongoClient,MongoDatabase,MongoCollection,Document,FindObservable}
  import org.mongodb.scala.bson.{BsonObjectId,BsonValue}
  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._
  import scala.concurrent.Future
  import Common._
  import org.slf4j.{Logger,LoggerFactory}

  val logger = LoggerFactory.getLogger(this.getClass);

  case class MaxServerCursorTimeOut(d:Duration)
  case class MaxNoOfRecordsToExtract(v:Int)
  case class NoMaxValueFound(m:String = "No Max Value Found, Either Collection is not there or it is empty.") extends Exception(m) 

  def extractNewMin(records:Seq[Document], maxRecordsRequested:Int):
                (Seq[Document], Option[BsonValue]) = {
    if(records.size < maxRecordsRequested) {
      logger.debug("CollectionReader::extractNewMin records.size < maxRecordsRequested, No Min found")
      //This means we have reached the last record.
      (records, None)
    } else {
      logger.debug("CollectionReader::extractNewMin start next fetch from {}", records.last.get("_id"))
      (records.take(records.size - 1), records.last.get("_id"))
    }
  }

  def addMinMaxCap(collectRecords:FindObservable[Document], 
                startValuefor_Id:Option[BsonValue], maxValuefor_Id:BsonValue) = {
    val maxValueDoc = Document( "$max" -> Document("_id" -> maxValuefor_Id), "$hint" -> Document("_id" -> 1 ))
    val collectionWithMaxCap = collectRecords.modifiers(maxValueDoc)
    startValuefor_Id match {
      case Some(v) => {
        val minCap = Document("$min" -> Document("_id" -> v ))
        logger.debug("CollectionReader::addMinMaxCap Added min cap {}", minCap)
        collectionWithMaxCap.modifiers(minCap)
      } 
      case None => {
        logger.debug("CollectionReader::addMinMaxCap No min cap to add")
        collectionWithMaxCap
      }
    }
  }

  def extractRecords(collection:MongoCollection[Document], 
                    maxValuefor_Id:BsonValue,
                    startValuefor_Id:Option[BsonValue])
                    (implicit maxRecords:MaxNoOfRecordsToExtract, timeout:MaxServerCursorTimeOut):
                    Future[(Seq[Document],Option[BsonValue])] = {
    
    val maxRecordsRequired = maxRecords.v + 1
    val collectRecords = collection.find().
                          limit(maxRecordsRequired).
                          sort(Document("_id" -> 1)).
                          maxTime(timeout.d)
    val collectionRecordsWithMin = addMinMaxCap(collectRecords, startValuefor_Id, maxValuefor_Id)
    collectRecords.toFuture.map(extractNewMin(_,maxRecordsRequired)).flatMap( _ match {
        case (records, None) => {
          logger.trace("CollectionReader::extractRecords Now trying to fetch the max record ( last record ) : {}", maxValuefor_Id)
          collection.find(Document("_id" -> maxValuefor_Id)).maxTime(timeout.d).toFuture.map {
            case maxValueRecord:Seq[Document] if maxValueRecord.size > 0 => {
              logger.info("CollectionReader::extractRecords Found the last record! : ", maxValueRecord)
              (records ++ maxValueRecord, None)
            }
            case _ => {
              logger.warn("CollectionReader::extractRecords Looks like the max record is not available with the server! : {}", maxValuefor_Id)
              (records, None)
            }
          }
        }
        case (r) => Future.successful(r)
      }
    )
  }

  def findMaxIDValue(collection:MongoCollection[Document]):Future[Option[BsonValue]] = {
    collection.find().sort(Document("_id" -> -1)).first.toFuture.map{ (records) => 
      if(records.size > 0) {
        val maxId = records.last.get("_id")
        logger.info("CollectionReader::findMaxIDValue found the max _id {}", maxId)
        maxId
      }
      else {
        logger.error("CollectionReader::findMaxIDValue couldn't find the max _id ")
        None
      }
    }
  }

  def readRecords(collection:MongoCollection[Document], maxValue:BsonValue, startValuefor_Id:Option[BsonValue], recordCount:Int = 0)
              (implicit maxRetriesAllowed:MaxRetriesAllowed, delayBetweenRetries:DelayBetweenRetries,
                maxRecords:MaxNoOfRecordsToExtract, timeout:MaxServerCursorTimeOut):
              Future[(Seq[Document],Option[BsonValue])] = {
    
    var newRecordCount = recordCount
    OpRetrier(() => extractRecords(collection,maxValue,startValuefor_Id)).flatMap { 
      case (records,Some(newMinValue)) => {
        for(record <- records) { 
          logger.info("{} ==> {}", newRecordCount, record); 
          newRecordCount = newRecordCount + 1 
        } 
        readRecords(collection, maxValue, Some(newMinValue), newRecordCount)

      }
      case (records,None) => { 
        for(record <- records) { 
          logger.info("{} ==> {}",newRecordCount,record); 
          newRecordCount = newRecordCount + 1 
        } 
        Future.successful(Nil, None)
      }
    }
  }

  def printAllRecords(mongoClient: MongoClient,dbName:String, colllectionName:String ) = {
    val database: MongoDatabase = mongoClient.getDatabase(dbName)
    val collection: MongoCollection[Document] = database.getCollection(colllectionName)  
    implicit val timeout = MaxServerCursorTimeOut(Duration(1,MINUTES))
    implicit val maxRecords = MaxNoOfRecordsToExtract(32)
    import Common.OpRetrierImplicits._

    val finalResult = findMaxIDValue(collection).flatMap{ 
      case Some(maxValue) => readRecords(collection, maxValue, None)
      case None => Future.failed(throw new NoMaxValueFound)
    }
    finalResult
  }
}