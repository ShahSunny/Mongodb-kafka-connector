package org.sunnyshahmca.connect.mongodb
package object Common {
  import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
  import scala.concurrent.duration._
  import ExecutionContext.Implicits.global
  import scala.util.{Try, Success, Failure}
  import org.slf4j.{Logger,LoggerFactory}
  val logger = LoggerFactory.getLogger(this.getClass);  

  def successRace[T](f: Future[T], g: => Future[T]): Future[T] = {
    val p = Promise[T]()
    p.tryCompleteWith(f)
    if(!f.isCompleted) { p.tryCompleteWith(g) }
    p.future
  }

  case class MaxRetriesAllowed(r:Int)
  case class DelayBetweenRetries(d:Duration)

  object OpRetrierImplicits  {
    implicit val m = MaxRetriesAllowed(10)
    implicit val d = DelayBetweenRetries(Duration(1, SECONDS))
  }
  
  def OpRetrier[T]( op: () => Future[T], retriesMade:Int = 0)
                    (implicit maxRetriesAllowed:MaxRetriesAllowed, delayBetweenRetries:DelayBetweenRetries):Future[T] = {

    val promise = Promise[T]
    if(retriesMade < 0 ) {
      promise.failure(new IllegalArgumentException("retriesMade =" + retriesMade + ", retriesMade can't be < 0 "))
    } else if (maxRetriesAllowed.r < retriesMade) {
      promise.failure(new IllegalArgumentException("maxRetriesAllowed = " + maxRetriesAllowed.r + " retriesMade = " + retriesMade + ", max_retries can't be < retriesMade"))
    } else {
      val resultF = if( retriesMade == 0 || delayBetweenRetries.d.toMillis <= 0 ) op()
                    else Future { blocking { Try{Thread.sleep(delayBetweenRetries.d.toMillis)} } }.flatMap((t) => op())
      resultF.onSuccess { case result => promise.success(result) } 
      resultF.onFailure { 
        case e:Throwable => 
          val nretriesMade = retriesMade + 1
          if(nretriesMade >= maxRetriesAllowed.r) {
            logger.info("That's it! Already made {} retries", nretriesMade)
            promise.failure(e)
          } else {
            logger.info("Might get one more chance, retries made = {}", nretriesMade)
            promise.completeWith(OpRetrier(op, retriesMade+1)(maxRetriesAllowed, delayBetweenRetries))
          }
      }
    }
    promise.future
  }
}