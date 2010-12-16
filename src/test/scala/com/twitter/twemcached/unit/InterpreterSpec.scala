package com.twitter.twemcached.unit

import org.specs.Specification
import com.twitter.util.MapMaker
import com.twitter.twemcached.Interpreter
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import com.twitter.twemcached.protocol._

class InterpreterSpec extends Specification {
  "Interpreter" should {
    val map = MapMaker[String, ChannelBuffer] { _ => }
    val interpreter = new Interpreter(map)

    "set & get" in {
      val bar = ChannelBuffers.wrappedBuffer("bar".getBytes)
      interpreter(Set("foo", bar))
      interpreter(Get(Seq("foo"))) mustEqual Values(Seq(Value("foo", bar)))
    }
  }
}