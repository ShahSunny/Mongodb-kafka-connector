package org.sunnyshahmca.connect.mongodb
package object Common {
  import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
  import ExecutionContext.Implicits.global
  import scala.util.{Try, Success, Failure}

  def successRace[T](f: Future[T], g: => Future[T]): Future[T] = {
    val p = Promise[T]()
    p.tryCompleteWith(f)
    if(!f.isCompleted) { p.tryCompleteWith(g) }
    p.future
  }

  case class MaxRetriesAllowed(r:Int)
  case class DelayBetweenRetries(d:Int)
  
  object OpRetrierImplicits  {
    implicit val m = MaxRetriesAllowed(10)
    implicit val d = DelayBetweenRetries(1000)
  }
  
  def OpRetrier[T]( op: () => Future[T], retriesMade:Int = 0)
                    (implicit maxRetriesAllowed:MaxRetriesAllowed, delayBetweenRetries:DelayBetweenRetries):Future[T] = {

    val promise = Promise[T]
    if(retriesMade < 0 ) {
      promise.failure(new IllegalArgumentException("retriesMade =" + retriesMade + ", retriesMade can't be < 0 "))
    } else if (maxRetriesAllowed.r < retriesMade) {
      promise.failure(new IllegalArgumentException("maxRetriesAllowed = " + maxRetriesAllowed.r + " retriesMade = " + retriesMade + ", max_retries can't be < retriesMade"))
    } else {
      val resultF = if( retriesMade == 0 || delayBetweenRetries.d <= 0 ) op()
                    else Future { blocking { Try{Thread.sleep(delayBetweenRetries.d)} } }.flatMap((t) => op())
      resultF.onSuccess { case result => promise.success(result) } 
      resultF.onFailure { 
        case e:Throwable => 
          val nretriesMade = retriesMade + 1
          if(nretriesMade >= maxRetriesAllowed.r) {
            println("That's it! Already made " + nretriesMade + " retries.")
            promise.failure(e)
          } else {
            println("Might get one more chance, retries made = " + nretriesMade)
            promise.completeWith(OpRetrier(op, retriesMade+1))
          }
      }
    }
    promise.future
  }

}