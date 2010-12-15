package com.twitter.twemcached.protocol

import org.specs.Specification
import com.twitter.util.MapMaker
import com.twitter.twemcached.Interpreter
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

class InterpreterSpec extends Specification {
  "Interpreter" should {
    val map = MapMaker[String, ChannelBuffer](_.softValues)
    val interpreter = new Interpreter(map)

    "set & get" in {
      interpreter(Set("foo", ChannelBuffers.wrappedBuffer("bar".getBytes)))
      interpreter(Get(Seq("foo"))) mustEqual
        ChannelBuffers.wrappedBuffer("VALUE foo 0 3\r\nbar\r\nEND\r\n".getBytes)
    }
  }
}