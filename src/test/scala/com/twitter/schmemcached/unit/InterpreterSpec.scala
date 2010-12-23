package com.twitter.schmemcached.unit

import org.specs.Specification
import com.twitter.schmemcached.Interpreter
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.schmemcached.protocol._
import scala.collection.mutable
import com.twitter.schmemcached.util.ChannelBufferUtils._
import com.twitter.schmemcached.util.AtomicMap

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