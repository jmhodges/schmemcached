package com.twitter.twemcached.protocol.text

import org.specs.Specification
import org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer
import com.twitter.twemcached.protocol.{ParseResponse, Add}

class ParseSpec extends Specification {
  "AbstractParser" should {
    "tokenize" in {
      new Parser().tokenize(wrappedBuffer("set my_key 0 2592000 1".getBytes)) mustEqual
        Seq("set", "my_key", "0", "2592000", "1")
    }
  }

  "ParseCommand" should {
    "needsData" in {
      ParseCommand.needsData(Seq("set", "my_key", "0", "2592000", "1")) mustEqual
        Some(1)
    }

    "parse storage commands" in {
      val buffer = wrappedBuffer("bar".getBytes)
      ParseCommand(Seq("add", "foo", "0", "0", "3"), buffer) mustEqual Add("foo", buffer)
    }
  }

  "ParseResponse" should {
    "needsData" in {
      ParseResponse.needsData(Seq("VALUE", "key", "0", "1")) mustEqual
        Some(1)
    }
  }
}