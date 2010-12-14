package com.twitter.twemcached.protocol

import org.specs.Specification

class CommandSpec extends Specification {
  "Command" should {
    "parse storage commands" in {
      Command("add foo 0 0 bar")     mustEqual Add("foo", "bar")
      Command("set foo 0 0 bar")     mustEqual Set("foo", "bar")
      Command("replace foo 0 0 bar") mustEqual Replace("foo", "bar")
      Command("append foo 0 0 bar")  mustEqual Append("foo", "bar")
      Command("prepend foo 0 0 bar") mustEqual Prepend("foo", "bar")
    }

    "parse retrieval commands" in {
      Command("get foo bar baz") mustEqual Get(Seq("foo", "bar", "baz"))
    }

    "parse the delete commands" in {
      Command("delete foo 123") mustEqual Delete("foo")
    }

    "parse arithmetical commands" in {
      Command("incr foo 123") mustEqual Incr("foo", 123)
      Command("decr foo 123") mustEqual Decr("foo", 123)
    }
  }
}