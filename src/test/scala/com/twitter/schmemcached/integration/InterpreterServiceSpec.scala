package com.twitter.schmemcached.integration

import org.specs.Specification
import com.twitter.schmemcached.Server
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.schmemcached.protocol._
import org.jboss.netty.buffer.ChannelBuffers
import com.twitter.util.RandomSocket
import com.twitter.schmemcached.protocol.text.Memcached
import com.twitter.util.TimeConversions._
import com.twitter.finagle.service.Service
import java.net.InetSocketAddress
import com.twitter.schmemcached.util.ChannelBufferUtils._

object InterpreterServiceSpec extends Specification {
  "InterpreterService" should {
    var server: Server = null
    var client: Service[Command, Response] = null

    doBefore {
      val address = RandomSocket()
      server = new Server(address)
      server.start()
      client = ClientBuilder()
        .hosts("localhost:" + address.getPort)
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