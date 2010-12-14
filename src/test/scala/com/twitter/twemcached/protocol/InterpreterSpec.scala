package com.twitter.twemcached.protocol

import org.specs.Specification
import com.twitter.util.MapMaker
import com.twitter.twemcached.Interpreter

class InterpreterSpec extends Specification {
  "Interpreter" should {
    val map = MapMaker[String, String](_.softValues)
    val interpreter = new Interpreter(map)

    "set & get" in {
      interpreter(Set("foo", "bar"))
      interpreter(Get(Seq("foo"))) mustEqual "VALUE foo 0 3\r\nbar\r\nEND\r\n"
    }
  }
}