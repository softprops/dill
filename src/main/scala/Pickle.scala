package dill

import org.python.core.{ PyObject, PyList, PyInteger, PyLong, PyFloat }
import org.python.modules.cPickle
import java.util.concurrent.TimeUnit
import java.util.Date

// https://github.com/jmxtrans/embedded-jmxtrans/blob/master/src/main/java/org/jmxtrans/embedded/output/GraphitePickleWriter.java#L141-L172

trait Pickles[T] {
  def pickle(p: T): PyObject
}

object Pickles {
  implicit def iterablePickles[T: Pickles] = new Pickles[Iterable[T]] {
    def pickle(p: Iterable[T]) = {
      val xs = new PyList()
      p.foreach(Pickle.apply(_))
      xs
    }
  }
  implicit object IntPickles extends Pickles[Int] {
    def pickle(p: Int) = new PyInteger(p)
  }
  implicit object LongPickles extends Pickles[Long] {
    def pickle(p: Long) = new PyLong(p)
  }
  implicit object FloatPickles extends Pickles[Float] {
    def pickle(p: Float) = new PyFloat(p)
  }
  implicit object DoublePickles extends Pickles[Double] {
    def pickle(p: Double) = new PyFloat(p)
  }
  implicit object DatePickles extends Pickles[Date] {
    def pickle(p: Date) = new PyLong(TimeUnit.SECONDS.convert(p.getTime(), TimeUnit.MILLISECONDS))
  }
}

object Pickle {
  def apply[T: Pickles](p: T) = implicitly[Pickles[T]].pickle(p)
}
