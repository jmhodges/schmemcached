package com.twitter.twemcached.protocol

import org.specs.Specification
import org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer

class CommandSpec extends Specification {
  "Command" should {
    "tokenize" in {
      Command.tokenize(wrappedBuffer("set my_key 0 2592000 1".getBytes)) mustEqual
        Seq("set", "my_key", "0", "2592000", "1")
    }

    "needsData" in {
      Command.needsData(Seq("set", "my_key", "0", "2592000", "1")) mustEqual
        Some(1)
    }

    "parse storage commands" in {
      val buffer = wrappedBuffer("bar".getBytes)
      Command.parse(Seq("add", "foo", "0", "0", "3"), buffer) mustEqual Add("foo", buffer)
    }
  }
}