package org.sunnyshahmca.connect.mongodb

package object databaseReader {
  import org.mongodb.scala.{MongoClient,MongoDatabase,MongoCollection,Document,FindObservable}
  import scala.concurrent.{ExecutionContext, Future}
  import scala.concurrent.ExecutionContext.Implicits.global
  import common._
  import common.OpRetrierImplicits._

  def getAvailableCollections(mongoClient:MongoClient, database:MongoDatabase)
    (implicit maxRetriesAllowed:MaxRetriesAllowed, delayBetweenRetries:DelayBetweenRetries)
    :Future[Seq[String]] = {
    val collectionNames = OpRetrier(() => database.listCollectionNames().toFuture)(maxRetriesAllowed,delayBetweenRetries)
    collectionNames.map{ _.filter { ! _.startsWith("system.") } }
  }

  def availableDatabases(mongoClient:MongoClient)
  (implicit maxRetriesAllowed:MaxRetriesAllowed, delayBetweenRetries:DelayBetweenRetries):Future[Seq[String]]
   = { OpRetrier(() => mongoClient.listDatabaseNames().toFuture)(maxRetriesAllowed,delayBetweenRetries) }
   
}