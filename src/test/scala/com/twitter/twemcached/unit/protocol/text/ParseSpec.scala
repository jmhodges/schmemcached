package com.twitter.twemcached.unit.protocol.text

import org.specs.Specification
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.twemcached.protocol._
import com.twitter.twemcached.protocol.ParseResponse.ValueLine
import com.twitter.twemcached.protocol.text.{ParseCommand, Parser}
import com.twitter.twemcached.util.ChannelBufferUtils._
import org.jboss.netty.util.CharsetUtil

class ParserSpec extends Specification {
  "Parser" should {
    "tokenize" in {
      Parser.tokenize("set my_key 0 2592000 1").toList.map(_.toString(CharsetUtil.UTF_8)) mustEqual
        Seq("set", "my_key", "0", "2592000", "1")
    }
  }

  "ParseCommand" should {
    "needsData" in {
      ParseCommand.needsData(Seq("set", "my_key", "0", "2592000", "1")) mustEqual
        Some(1)
    }

    "parse storage commands" in {
      val buffer = "bar"
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

    "isEnd" in {
      ParseResponse.isEnd(Seq("END")) mustBe true
    }

    "parse simple responses" in {
      ParseResponse(Seq("STORED"))     mustEqual Stored()
      ParseResponse(Seq("NOT_STORED")) mustEqual NotStored()
      ParseResponse(Seq("DELETED"))    mustEqual Deleted()
    }

    "parse values" in {
      val one = "1"
      val two = "2"
      val values = Seq(
        ValueLine(Seq("VALUE", "foo", "0", "1"), one),
        ValueLine(Seq("VALUE", "bar", "0", "1"), two))

      ParseResponse.parseValues(values) mustEqual
        Values(Seq(
          Value("foo", one),
          Value("bar", two)))
    }
  }
}