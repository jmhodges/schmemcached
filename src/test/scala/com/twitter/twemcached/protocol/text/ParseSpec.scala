package com.twitter.twemcached.protocol.text

import org.specs.Specification
import org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer
import com.twitter.twemcached.protocol._

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
      ParseCommand(Seq("add",     "foo", "0", "0", "3"), buffer) mustEqual Add("foo", buffer)
      ParseCommand(Seq("set",     "foo", "0", "0", "3"), buffer) mustEqual Set("foo", buffer)
      ParseCommand(Seq("replace", "foo", "0", "0", "3"), buffer) mustEqual Replace("foo", buffer)
      ParseCommand(Seq("append",  "foo", "0", "0", "3"), buffer) mustEqual Append("foo", buffer)
      ParseCommand(Seq("prepend", "foo", "0", "0", "3"), buffer) mustEqual Prepend("foo", buffer)
    }
  }

  "ParseResponse" should {
    "needsData" in {
      ParseResponse.needsData(Seq("VALUE", "key", "0", "1")) mustEqual
        Some(1)
    }

    "parse simple responses" in {
      ParseResponse(Seq("STORED"))     mustEqual Stored()
      ParseResponse(Seq("NOT_STORED")) mustEqual NotStored()
      ParseResponse(Seq("DELETED"))    mustEqual Deleted()
    }

    "parse values" in {
      val one = wrappedBuffer("1".getBytes)
      val two = wrappedBuffer("2".getBytes)
      val values = Seq(
        (Seq("foo", "0", "1"), one),
        (Seq("bar", "0", "1"), two))

      ParseResponse.parseValues(values) mustEqual
        Values(Seq(
          Value("foo", one),
          Value("bar", two)))
    }
  }
}