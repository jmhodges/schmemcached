package com.twitter.twemcached.unit

import org.specs.Specification
import com.twitter.twemcached.Interpreter
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.twemcached.protocol._
import scala.collection.mutable
import com.twitter.twemcached.util.ChannelBufferUtils._
import com.twitter.twemcached.util.AtomicMap

class InterpreterSpec extends Specification {
  "Interpreter" should {
    val map = mutable.Map[ChannelBuffer, ChannelBuffer]()
    val interpreter = new Interpreter(new AtomicMap(Seq(map)))

    "set & get" in {
      val key   = "foo"
      val value = "bar"
      interpreter(Set(key, value))
      interpreter(Get(Seq(key))) mustEqual Values(Seq(Value(key, value)))
    }
  }
}