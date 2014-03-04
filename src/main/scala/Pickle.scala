package dill

import org.python.core.{ PyDictionary, PyObject, PyList, PyInteger, PyLong, PyFloat, PyString, PyTuple }
import org.python.modules.cPickle
import java.util.concurrent.TimeUnit
import java.util.Date
import java.io.OutputStream
import java.nio.ByteBuffer

trait Pickles[T] {
  def pickle(p: T): PyObject
}

object Pickles {
  implicit def traversableOncePickles[T: Pickles] = new Pickles[TraversableOnce[T]] {
    def pickle(p: TraversableOnce[T]) = {
      val xs = new PyList()
      p.foreach(Pickle(_))
      xs
    }
  }
  implicit def tuple2Pickles[A: Pickles, B: Pickles]= new Pickles[(A,B)] {
    def pickle(p: (A, B)) = new PyTuple(Pickle(p._1), Pickle(p._2))
  }
  implicit def mapPickles[A: Pickles, B: Pickles] = new Pickles[Map[A, B]] {
    def pickle(p: Map[A, B]) = {
      val dict = new PyDictionary()
      p.foreach {
        case (k, v) => dict.put(Pickle(k), Pickle(v))
      }
      dict
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
  implicit object StringPickles extends Pickles[String] {
    def pickle(p: String) = new PyString(p)
  }
  implicit object DatePickles extends Pickles[Date] {
    def pickle(p: Date) = new PyLong(TimeUnit.SECONDS.convert(p.getTime(), TimeUnit.MILLISECONDS))
  }
}

object Pickle {
  def apply[T: Pickles](p: T) = implicitly[Pickles[T]].pickle(p)
  def dumps[T: Pickles, O](p: T)(out: OutputStream) = {
    val payload = cPickle.dumps(apply(p))
    val header  = ByteBuffer.allocate(4).putInt(payload.__len__()).array()
    out.write(header)
    out.write(payload.toBytes())
    out.flush()
  }
}
