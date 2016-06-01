
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
import org.mongodb.scala.bson.{BsonObjectId,BsonValue}
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
  
  def getMongodbPort() : Option[Int] = {
    getPortsWithWait match {
      case Some(m) => { logger.trace("New port is  " + m.get(27017)); Some(m.get(27017)) }
      case None => { logger.error("Ports is Failure, Container start failed!"); None }
    }
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
			case None => Future.failed(throw new NoMaxValueFound)
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
}

class MongodbServiceSpec(env: Env) extends mutable.Specification
    with org.specs2.specification.BeforeAfterEach
    with MongoService {

//  val logger = LoggerFactory.getLogger(this.getClass)
  implicit val ee = env.executionEnv

  override def mongodbVersion = "3.2.6"
  override def before() = {
    logger.trace("BeforeEach")
    startAllOrFail()
  }

  override def after() {
    logger.error("AfterEach")
    stopAllQuietly()
  }
  
  "the mongodb container should be ready" >> {
    logger.info("Port = {}",getMongodbPort())
    val mongoClient = MongoClient("mongodb://localhost:" + getMongodbPort().get)
		implicit val c = CollectionName("test")
		implicit val d = DatabaseName("mydb") 
    MongoDBServiceHelper.insertOneRecord(mongoClient) 
	  val records = CollectionReaderHelper.readAllTheRecords(mongoClient).map( _ - "_id")
		records  must containTheSameElementsAs(Seq(MongoDBServiceHelper.doc))
  }

}

