package org.sunnyshahmca.connect.mongodb

package object collectionReader {
  import org.mongodb.scala.{MongoClient,MongoDatabase,MongoCollection,Document,FindObservable}
  import org.mongodb.scala.bson.{BsonObjectId,BsonValue}
  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._
  import scala.concurrent.Future
  import common._
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

  def addMinMaxCap(collectionRecords:FindObservable[Document], 
                startValuefor_Id:Option[BsonValue], maxValuefor_Id:BsonValue) = {
   startValuefor_Id match {
      case Some(v) => {
        val minMaxCap = Document("$min" -> Document("_id" -> v ),"$max" -> Document("_id" -> maxValuefor_Id), "$hint" -> Document("_id" -> 1 ))
        logger.debug("CollectionReader::addMinMaxCap Added min cap {}", minMaxCap)
        collectionRecords.modifiers(minMaxCap)
      } 
      case None => {
        logger.debug("CollectionReader::addMinMaxCap No min cap to add")
        val maxValueDoc = Document( "$max" -> Document("_id" -> maxValuefor_Id), "$hint" -> Document("_id" -> 1 ))
        collectionRecords.modifiers(maxValueDoc)
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
            (records ++ maxValueRecord, None)
          }
          case _ => {
            logger.warn("CollectionReader::extractRecords Looks like the max record is not available with the server! : {}", maxValuefor_Id)
            (records, None)
          }
        }
      }
      case (r) => Future.successful(r)
    })
  }

  def findMaxIDValue(collection:MongoCollection[Document]):Future[Option[BsonValue]] = {
    collection.find().sort(Document("_id" -> -1)).first.toFuture.map{ (records) => 
      if(records.size > 0) {
        val maxId = records.last.get("_id")
        logger.info("CollectionReader::findMaxIDValue found the max _id {}", maxId)
        maxId
      } else {
        logger.error("CollectionReader::findMaxIDValue couldn't find the max _id ")
        None
      }
    }
  }
}

