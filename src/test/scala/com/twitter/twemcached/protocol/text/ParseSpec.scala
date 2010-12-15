package com.twitter.twemcached.protocol.text

import org.specs.Specification
import org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer
import com.twitter.twemcached.protocol.Add

class ParseSpec extends Specification {
  "Parse" should {
    "tokenize" in {
      Parse.tokenize(wrappedBuffer("set my_key 0 2592000 1".getBytes)) mustEqual
        Seq("set", "my_key", "0", "2592000", "1")
    }

    "needsData" in {
      Parse.needsData(Seq("set", "my_key", "0", "2592000", "1")) mustEqual
        Some(1)
    }

    "parse storage commands" in {
      val buffer = wrappedBuffer("bar".getBytes)
      Parse(Seq("add", "foo", "0", "0", "3"), buffer) mustEqual Add("foo", buffer)
    }
  }
}