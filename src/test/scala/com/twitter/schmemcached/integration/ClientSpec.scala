package com.twitter.schmemcached.integration

import org.specs.Specification
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.schmemcached.protocol._
import com.twitter.schmemcached.protocol.text.Memcached
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.schmemcached.Client
import org.jboss.netty.util.CharsetUtil

object ClientSpec extends Specification {
  /**
   * Note: This test needs a real Memcached server running on 11211 to work!!
   */
  "ConnectedClient" should {
    "work" in {
      val service = ClientBuilder().hosts("localhost:11211").codec(Memcached).buildService[Command, Response]()
      val client = Client(service)

      client.delete("foo")()

      "set & get" in {
        client.get("foo")() mustEqual None
        client.set("foo", "bar")()
        client.get("foo")().get.toString(CharsetUtil.UTF_8) mustEqual "bar"
      }

      "gets" in {
        client.set("foo", "bar")()
        client.set("baz", "boing")()
        val result = client.get("foo", "baz", "notthere")()
          .map { case (key, value) => (key, value.toString(CharsetUtil.UTF_8)) }
        result mustEqual Map(
          "foo" -> "bar",
          "baz" -> "boing"
        )
      }

      "append & prepend" in {
        client.set("foo", "bar")()
        client.append("foo", "rab")()
        client.get("foo")().get.toString(CharsetUtil.UTF_8) mustEqual "barrab"
        client.prepend("foo", "rab")()
        client.get("foo")().get.toString(CharsetUtil.UTF_8) mustEqual "rabbarrab"
      }

      "incr & decr" in {
        client.set("foo", "")()
        client.incr("foo")()    mustEqual 1
        client.incr("foo", 2)() mustEqual 3
        client.decr("foo")()    mustEqual 2
      }
    }
  }
}