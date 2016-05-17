package org.sunnyshahmca.connect.mongodb.OplogReader

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

object OplogDataWatcherChecker extends mutable.Specification with org.specs2.ScalaCheck
{
  val numberGen = frequency(
    (1, Gen.choose(0,65536)),
    (1, 0),
    (1, 1)
  )
  val maxWaitGen = frequency(
    (1, Gen.choose(1,65536)),
    (1, 1)
  )
  val logger = LoggerFactory.getLogger(this.getClass);  

  "Validate CheckOplogDataWatcher for not all data beforehand with reference impllimentation" >> 
  Prop.forAll(maxWaitGen,maxWaitGen,numberGen,numberGen) {
      (maxWaitAllowed:Int, maxWaitForSubsequentRecords:Int, firstRecordDelay:Int, secondRecordDelay:Int) => {
        val maxDelayAfterFirstRecord = maxWaitAllowed - firstRecordDelay
        val expectedCount = if(firstRecordDelay > maxWaitAllowed ) 0  
          else if ( secondRecordDelay > maxDelayAfterFirstRecord ||  secondRecordDelay > maxWaitForSubsequentRecords ) 1
          else 2

        val result = testOplogDataWatcher(maxWaitAllowed, maxWaitForSubsequentRecords, firstRecordDelay, secondRecordDelay)
        result must_== expectedCount
      }
  }

  "Validate CheckOplogDataWatcher for all data available beforehand" >>  {
      val result = testOplogDataWatcher(maxWaitGen.sample.get, maxWaitGen.sample.get, 
                          numberGen.sample.get, numberGen.sample.get,true)
      result must_== 2
  }

  def testOplogDataWatcher(maxWaitAllowed:Long, maxWaitForSubsequentRecords:Long, firstRecordDelay:Long, secondRecordDelay:Long, 
    isAllDataAvailable:Boolean = false):Int = {

    implicit object dummySleeper extends Sleeper {
      var listPromises = List[(Promise[Int], Long, Int)]()
      var isDoneOnce = false
      def sleep[T](msSleep:Long, value:T, beforeSleepTrigger:()=>Unit = ()=>{}, afterSleepTrigger:()=>Unit = ()=>{})
      (implicit ec:ExecutionContext):Future[T] = {
        this.synchronized {
          val f = Promise[T]
          val newPromise = (f.asInstanceOf[Promise[Int]],if(!isDoneOnce) msSleep else msSleep + firstRecordDelay,value.asInstanceOf[Int])
          logger.info("dummySleeper::sleep newPromise = {}", newPromise)
          listPromises = newPromise :: listPromises
          f.future
        }
      }

      def done():Unit = {
        this.synchronized {
          isDoneOnce = true
          listPromises = listPromises.sortWith( (a,b) => {(a._2 < b._2 || ((a._2 == b._2) && (a._3 < b._3)))})
          val first = listPromises.head
          logger.info("testOplogDataWatcher::done {}", listPromises)
          listPromises = listPromises.takeRight(listPromises.size - 1)
          first._1.success(first._3)
        }
      }
    }

    object recordPooler extends oplogReader.RecordPooler {
      var promiseFirst = Promise[(oplogReader.Milliseconds, oplogReader.AllRecordsFuture)]
      var secondRecordFuture:Future[oplogReader.RequestId] = null

      def request: Either[oplogReader.RequestId, oplogReader.RequestIDWithFirstRecordFuture] = {
        if(isAllDataAvailable) {
          Left(3)
        } else {
          val firstRecordFuture = dummySleeper.sleep(firstRecordDelay,1:Int)
          secondRecordFuture = dummySleeper.sleep(firstRecordDelay + secondRecordDelay,2:Int)
          secondRecordFuture.onSuccess{case _ => {logger.info("secondRecordFuture's onSuccess got called")}}
          promiseFirst.completeWith(firstRecordFuture.map( (requestId) => {
            Future{ blocking{ Thread.sleep(1); dummySleeper.done() } }
            (requestId, secondRecordFuture)
            }))
          return Right(3,promiseFirst.future)
        }
      }

      def getAllAvailableRecords():Seq[(oplogReader.Milliseconds,Document)] = {
        if((promiseFirst.isCompleted && secondRecordFuture.isCompleted) || isAllDataAvailable)
        List((1,Document("id" -> 1)),(2, Document("id" -> 2)))
        else if ( promiseFirst.isCompleted )
        List((1,Document("id" -> 1)))
        else 
        List[(oplogReader.Milliseconds,Document)]()
      }
    }

    val result = oplogReader.oplogDataWatcher(recordPooler, Duration(maxWaitAllowed, MILLISECONDS), Duration(maxWaitForSubsequentRecords, MILLISECONDS))
    if(!isAllDataAvailable) { dummySleeper.done() }
    Await.result(result, 100 millis)
    logger.info("After Done {}", result)
    result.value.get match {
      case Success(l) =>  l.size 
      case _ =>  -1
    }
  }
}