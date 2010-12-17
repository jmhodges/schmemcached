package com.twitter.twemcached.integration

import org.specs.Specification
import com.twitter.twemcached.MemcachedServer
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.twemcached.protocol._
import org.jboss.netty.buffer.ChannelBuffers
import com.twitter.util.RandomSocket
import com.twitter.twemcached.protocol.text.Memcached
import com.twitter.util.TimeConversions._
import com.twitter.finagle.service.Service
import java.net.InetSocketAddress
import com.twitter.twemcached.util.ChannelBufferUtils._

class InterpreterServiceSpec extends Specification {
  "InterpreterService" should {
    var server: MemcachedServer = null
    var client: Service[Command, Response] = null

    doBefore {
      val address = RandomSocket()
      server = new MemcachedServer(new InetSocketAddress(1234))
      server.start()
      Thread.sleep(1000)
      client = ClientBuilder()
        .hosts("localhost:" + 1234)
        .codec(Memcached)
        .buildService[Command, Response]()
    }

    doAfter {
      server.stop()
    }

    "set & get" in {
      val key   = "key"
      val value = "value"
      val result = for {
        _ <- client(Set(key, value))
        r <- client(Get(Seq(key)))
      } yield r
      result(1.second) mustEqual Values(Seq(Value(key, value)))
    }
  }
}