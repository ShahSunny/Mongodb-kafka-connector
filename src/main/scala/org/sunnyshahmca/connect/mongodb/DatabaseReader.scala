package org.sunnyshahmca.connect.mongodb

package object DatabaseReader {
  import org.mongodb.scala.{MongoClient,MongoDatabase,MongoCollection,Document,FindObservable}
  import scala.concurrent.{ExecutionContext, Future}
  import scala.concurrent.ExecutionContext.Implicits.global
  import Common._
  import Common.OpRetrierImplicits._

  def getAvailableCollections(mongoClient:MongoClient, database:MongoDatabase)
    (implicit maxRetriesAllowed:MaxRetriesAllowed, delayBetweenRetries:DelayBetweenRetries)
    :Future[Seq[String]] = {
    val collectionNames = OpRetrier(() => database.listCollectionNames().toFuture)
    collectionNames.map{ _.filter { ! _.startsWith("system.") } }
  }

  def availableDatabases(mongoClient:MongoClient)
  (implicit maxRetriesAllowed:MaxRetriesAllowed, delayBetweenRetries:DelayBetweenRetries):Future[Seq[String]]
   = { OpRetrier(() => mongoClient.listDatabaseNames().toFuture) }
   
}