package com.twitter.twemcached.protocol

import org.specs.Specification

class CommandSpec extends Specification {
  "Command" should {
    "parse storage commands" in {
      Command("add")(Seq("foo", "0", "0", "3", "bar")) mustEqual Add("foo", "bar")
    }
  }
}