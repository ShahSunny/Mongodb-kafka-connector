package org.sunnyshahmca.connect.mongodb
package object Common {
  import scala.concurrent.{Future, Promise}
  def successRace[T](f: Future[T], g: => Future[T]): Future[T] = {
    val p = Promise[T]()
    p.tryCompleteWith(f)
    if(!f.isCompleted)
    p.tryCompleteWith(g)
    p.future
  }
}