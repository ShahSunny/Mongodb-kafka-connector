
import org.slf4j.{Logger,LoggerFactory}
import com.whisk.docker.{DockerKit,DockerContainer,DockerReadyChecker}
import com.whisk.docker.specs2.DockerTestKit
import org.specs2._
import org.specs2.specification.core.Env
import scala.concurrent._
import com.github.dockerjava.core.DockerClientConfig
import com.github.dockerjava.netty.DockerCmdExecFactoryImpl
import ExecutionContext.Implicits.global
import scala.util.{Try, Success, Failure} 
import scala.concurrent.duration._
import org.mongodb.scala._
import org.sunnyshahmca.connect.mongodb._
import org.mongodb.scala.bson.{BsonObjectId,BsonValue,BsonInt32,BsonArray,BsonDocument}
import org.sunnyshahmca.connect.mongodb.collectionReader._
import org.sunnyshahmca.connect.mongodb.common._

trait MongoService extends DockerKit {

  val DefaultMongodbPort = 27017
  val logger = LoggerFactory.getLogger(this.getClass)
  val mongodbContainer = DockerContainer("mongo:"+mongodbVersion)
    .withPorts(DefaultMongodbPort -> None)
    .withReadyChecker(DockerReadyChecker.LogLineContains("waiting for connections on port"))
    .withCommand("mongod", "--nojournal", "--smallfiles", "--syncdelay", "0")
  
  def isMongoContainerReady = super.isContainerReady(mongodbContainer) 
  def mongodbVersion = "3.2.6"
  
  abstract override def dockerContainers: List[DockerContainer] = mongodbContainer :: super.dockerContainers
  def getPortsWithWait = { 
    val ports = getContainerState(mongodbContainer).getPorts()
    Await.ready(ports,Duration(60, SECONDS))
    ports.value
  }
  def getIPAddress():Future[String] = {
    getContainerState(mongodbContainer).id.flatMap { (id) =>
        Future{ docker.client.inspectContainerCmd(id).exec() }
    }.map {
        _.getNetworkSettings.getNetworks().get("bridge").getIpAddress()
    }
  }

  def getMongodbPort() : Option[Int] = {
    getPortsWithWait match {
      case Some(m) => { logger.trace("New port is  " + m.get(27017)); Some(m.get(27017)) }
      case None => { logger.error("Ports is Failure, Container start failed!"); None }
    }
  }
}

trait MongoOplogService extends DockerKit {

  val DefaultMongodbPort = 27017
  val logger = LoggerFactory.getLogger(this.getClass)
  val replicaSetName = "r1"
  val m1 = DockerContainer("mongo:"+mongodbVersion)
    .withPorts(DefaultMongodbPort -> None)
    .withEnv("NAME=m1")
    .withReadyChecker(DockerReadyChecker.LogLineContains("waiting for connections on port"))
    .withCommand("mongod", "--replSet", replicaSetName, "--noprealloc", "--nojournal", "--smallfiles", "--syncdelay", "0")

  val m2 = DockerContainer("mongo:"+mongodbVersion)
    .withPorts(DefaultMongodbPort -> None)
    .withEnv("NAME=m2")
    .withReadyChecker(DockerReadyChecker.LogLineContains("waiting for connections on port"))
    .withCommand("mongod", "--replSet", replicaSetName, "--noprealloc", "--nojournal", "--smallfiles", "--syncdelay", "0")

  val m3 = DockerContainer("mongo:"+mongodbVersion)
    .withEnv("NAME=m3")
    .withPorts(DefaultMongodbPort -> None)
    .withReadyChecker(DockerReadyChecker.LogLineContains("waiting for connections on port"))
    .withCommand("mongod", "--replSet", replicaSetName, "--noprealloc", "--nojournal", "--smallfiles", "--syncdelay", "0")

  def isMongoContainerReady(dc:DockerContainer)= super.isContainerReady(dc) 
  def mongodbVersion = "3.2.6"
  
  abstract override def dockerContainers: List[DockerContainer] = m1 :: m2 :: m3 :: super.dockerContainers
  def getPortsWithWait(dc:DockerContainer) = { 
    val ports = getContainerState(dc).getPorts()
    Await.ready(ports,Duration(60, SECONDS))
    ports.value
  }
  
  def getIPAddress(dc:DockerContainer):String = {
    val ipFuture = getContainerState(dc).id.flatMap { (id) =>
        Future{ docker.client.inspectContainerCmd(id).exec() }
    }.map {
        _.getNetworkSettings.getIpAddress()
    }
    Await.ready(ipFuture,Duration(60,SECONDS))
    ipFuture.value.get.get
  }

  def getMongodbPort(dc:DockerContainer) : Option[Int] = {
    getPortsWithWait(dc) match {
      case Some(m) => { logger.trace("New port is  " + m.get(27017)); Some(m.get(27017)) }
      case None => { logger.error("Ports is Failure, Container start failed!"); None }
    }
  }
  
  def initiateCluster() = {
    val cfg = Document(
      "_id" -> replicaSetName,
      "members" -> BsonArray(
        BsonDocument("_id" -> 0, "host" -> (getIPAddress(m1)+":27017")),
        BsonDocument("_id" -> 1, "host" -> (getIPAddress(m2)+":27017")),
        BsonDocument("_id" -> 2, "host" -> (getIPAddress(m3)+":27017"))
      )
    )
    logger.trace("CFG = {}",cfg)
    val mongoClient = MongoClient("mongodb://"+getIPAddress(m1)+":27017")
    val database: MongoDatabase = mongoClient.getDatabase("admin")
    Await.ready(database.runCommand(Document("replSetInitiate" -> cfg)).toFuture, Duration(60, SECONDS))
    mongoClient.close()
  }

}



class MongodbServiceSpecOplogReader(env: Env) extends mutable.Specification
    with DockerTestKit
    with MongoOplogService {
  implicit val ee = env.executionEnv
  "Read oplog" >> {
    initiateCluster()
//    val ipAddress = getIPAddress()
//    Await.ready(ipAddress,Duration(60, SECONDS))
//		logger.info("ipAddress = {}",ipAddress.value.get.get)
    true must_== true
  }
}  


case class CollectionName(d:String)
case class DatabaseName(d:String)

  
object CollectionReaderHelper {
	
   def readRecords(collection:MongoCollection[Document], maxValue:BsonValue, startValuefor_Id:Option[BsonValue])
              (implicit maxRetriesAllowed:MaxRetriesAllowed, delayBetweenRetries:DelayBetweenRetries,
                maxRecords:MaxNoOfRecordsToExtract, timeout:MaxServerCursorTimeOut): Future[Seq[Document]] = {
    
    OpRetrier(() => extractRecords(collection,maxValue,startValuefor_Id)).map { 
			case (records,Some(newMinValue)) => {
				throw new IllegalStateException("More records than expected") 
				Seq.empty[Document] 
			}
      case (records,None) => records 
    }
  }
 
	def readAllTheRecords(mongoClient:MongoClient, maxRecordsI:Int = 1032)
	  (implicit colName:CollectionName, dbName:DatabaseName):Seq[Document] = {
		val database: MongoDatabase = mongoClient.getDatabase(dbName.d)
		val collection: MongoCollection[Document] = database.getCollection(colName.d)  
		implicit val timeout = MaxServerCursorTimeOut(Duration(1,MINUTES))
		implicit val maxRecords = MaxNoOfRecordsToExtract(maxRecordsI)
		import common.OpRetrierImplicits._

		val allRecords = findMaxIDValue(collection).flatMap{ 
			case Some(maxValue) => readRecords(collection, maxValue, None)
			case None => Future { Seq.empty[Document] }
		}
		Await.ready(allRecords,Duration(60, SECONDS))
		allRecords.value.get.get
  }
 
  def readRecordsInSteps(collection:MongoCollection[Document], maxValue:BsonValue, startValuefor_Id:Option[BsonValue])
              (implicit maxRetriesAllowed:MaxRetriesAllowed, delayBetweenRetries:DelayBetweenRetries,
                maxRecords:MaxNoOfRecordsToExtract, timeout:MaxServerCursorTimeOut): Future[Seq[Document]] = {
    
    OpRetrier(() => extractRecords(collection,maxValue,startValuefor_Id)).map { 
			case (records,Some(newMinValue)) => {
			  val additionalRecords = readRecordsInSteps(collection, maxValue,Some(newMinValue))
        Await.ready(additionalRecords, Duration(60,SECONDS))
        records ++ additionalRecords.value.get.get
			}
      case (records,None) => records 
    }
  }


  def readAllRecordsInSteps(mongoClient:MongoClient, steps:Int, deleteFrom:Option[Document] = None)
	  (implicit colName:CollectionName, dbName:DatabaseName):Seq[Document] = {
		val database: MongoDatabase = mongoClient.getDatabase(dbName.d)
		val collection: MongoCollection[Document] = database.getCollection(colName.d)  
		implicit val timeout = MaxServerCursorTimeOut(Duration(1,MINUTES))
		implicit val maxRecords = MaxNoOfRecordsToExtract(steps)
		import common.OpRetrierImplicits._

		val allRecords = findMaxIDValue(collection).flatMap{ 
			case Some(maxValue) => {
        deleteFrom match {
          case Some(d) => {
            Await.ready(collection.deleteMany(d).toFuture, Duration(60,SECONDS))
          }
          case _ => {}
        }
        readRecordsInSteps(collection, maxValue, None)
      }
      case None => Future { Seq.empty[Document] }
		}
		Await.ready(allRecords,Duration(60, SECONDS))
		allRecords.value.get.get
  }
}

object MongoDBServiceHelper {
	
  val doc: Document = Document( "name" -> "MongoDB", "type" -> "database",
                                   "count" -> 1, "info" -> Document("x" -> 203, "y" -> 102))
  
	def insertOneRecord(mongoClient:MongoClient)
		(implicit colName:CollectionName, dbName:DatabaseName):Future[Seq[Long]] = {
    val database: MongoDatabase = mongoClient.getDatabase(dbName.d)
    val collection: MongoCollection[Document] = database.getCollection(colName.d)
    Await.ready(collection.insertOne(doc).toFuture(),Duration(60, SECONDS))
    collection.count().toFuture
  }
  
  def insertManyRecords(mongoClient:MongoClient, recordCount:Int) 
 		(implicit colName:CollectionName, dbName:DatabaseName):Future[Seq[Long]]  = {
    val database: MongoDatabase = mongoClient.getDatabase(dbName.d)
    val collection: MongoCollection[Document] = database.getCollection(colName.d)
    for(  x <- 1 to recordCount) {
        Await.ready(collection.insertOne(Document("no"->x)).toFuture(),Duration(60, SECONDS))
    }
    collection.count().toFuture
  }
}

class MongodbServiceSpec(env: Env) extends mutable.Specification
    with DockerTestKit
    with MongoService {

  implicit val ee = env.executionEnv
  
  "the mongodb container should have one record" >> {
    logger.info("Port = {}",getMongodbPort())
    val mongoClient = MongoClient("mongodb://localhost:" + getMongodbPort().get)
		implicit val c = CollectionName("test")
		implicit val d = DatabaseName("mydb") 
    MongoDBServiceHelper.insertOneRecord(mongoClient) 
	  val records = CollectionReaderHelper.readAllTheRecords(mongoClient).map( _ - "_id")
		records  must containTheSameElementsAs(Seq(MongoDBServiceHelper.doc))
  }
  
  "the mongodb should have 1 recors when retrived in the steps of 1" >> {
    logger.info("Port = {}",getMongodbPort())
    val mongoClient = MongoClient("mongodb://localhost:" + getMongodbPort().get)
		implicit val c = CollectionName("test")
		implicit val d = DatabaseName("mydb") 
    val maxValue = 1
    //MongoDBServiceHelper.insertManyRecords(mongoClient,maxValue)
    val stepSize = 1
	  val records = CollectionReaderHelper.readAllRecordsInSteps(mongoClient,stepSize).map( _ - "_id")
		records  must containTheSameElementsAs(Seq(MongoDBServiceHelper.doc))
  }

}

class MongodbServiceSpecEmptyRecords(env: Env) extends mutable.Specification
    with DockerTestKit
    with MongoService {
  implicit val ee = env.executionEnv
  "the mongodb should not have any record" >> {
    logger.info("Port = {}",getMongodbPort())
    val mongoClient = MongoClient("mongodb://localhost:" + getMongodbPort().get)
		implicit val c = CollectionName("test")
		implicit val d = DatabaseName("mydb") 
    //MongoDBServiceHelper.insertOneRecord(mongoClient) 
	  val records = CollectionReaderHelper.readAllTheRecords(mongoClient).map( _ - "_id")
		records must be empty
  }
}

class MongodbServiceSpecManyRecords(env: Env) extends mutable.Specification
    with DockerTestKit
    with MongoService {
  implicit val ee = env.executionEnv
  "the mongodb should have 1024 recors when retrived in the steps of 100" >> {
    logger.info("Port = {}",getMongodbPort())
//    val ipAddress = getIPAddress()
//    Await.ready(ipAddress,Duration(60, SECONDS))
//		logger.info("ipAddress = {}",ipAddress.value.get.get)
    val mongoClient = MongoClient("mongodb://localhost:" + getMongodbPort().get)
		implicit val c = CollectionName("test")
		implicit val d = DatabaseName("mydb") 
    val maxValue = 1024
    MongoDBServiceHelper.insertManyRecords(mongoClient,maxValue)
    val stepSize = 10
	  val records = CollectionReaderHelper.readAllRecordsInSteps(mongoClient,stepSize).map( _("no").asInstanceOf[BsonInt32].intValue())
    (records must containTheSameElementsAs(1 to maxValue))
    (records must beSorted)
    (records must have size(maxValue))
  }
  
  "the mongodb should have 1024 recors when retrived in the steps of 512" >> {
    logger.info("Port = {}",getMongodbPort())
    val mongoClient = MongoClient("mongodb://localhost:" + getMongodbPort().get)
		implicit val c = CollectionName("test")
		implicit val d = DatabaseName("mydb") 
    val maxValue = 1024
    //MongoDBServiceHelper.insertManyRecords(mongoClient,maxValue)
    val stepSize = 512
	  val records = CollectionReaderHelper.readAllRecordsInSteps(mongoClient,stepSize).map( _("no").asInstanceOf[BsonInt32].intValue())
    (records must containTheSameElementsAs(1 to maxValue))
    (records must beSorted)
    (records must have size(maxValue))
  }
  
  "the mongodb should have 900 recors when we delete >900 records while reading" >> {
    logger.info("Port = {}",getMongodbPort())

    val mongoClient = MongoClient("mongodb://localhost:" + getMongodbPort().get)
		implicit val c = CollectionName("test")
		implicit val d = DatabaseName("mydb") 
    val maxValue = 900
    //MongoDBServiceHelper.insertManyRecords(mongoClient,maxValue)
    val stepSize = 512
    val records = CollectionReaderHelper.readAllRecordsInSteps(mongoClient,stepSize,Some(Document("no" -> Document("$gt" -> maxValue)))).
                    map( _("no").asInstanceOf[BsonInt32].intValue())

    (records must containTheSameElementsAs(1 to maxValue))
    (records must beSorted)
    (records must have size(maxValue))
  }

}

