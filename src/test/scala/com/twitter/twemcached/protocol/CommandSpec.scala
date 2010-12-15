package com.twitter.twemcached.protocol

import org.specs.Specification
import org.jboss.netty.buffer.ChannelBuffers

class CommandSpec extends Specification {
  "Command" should {
    "parse storage commands" in {
      val buffer = ChannelBuffers.wrappedBuffer("bar".getBytes)
      Command.parse(Seq("add", "foo", "0", "0", "3"), buffer) mustEqual Add("foo", buffer)
    }
  }
}