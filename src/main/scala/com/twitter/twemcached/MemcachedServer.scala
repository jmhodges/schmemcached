package com.twitter.twemcached

import org.jboss.netty.channel.Channel
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.util.MapMaker
import org.jboss.netty.buffer.ChannelBuffer
import protocol.text.Memcached
import java.net.SocketAddress
import java.util.logging.Logger

class MemcachedServer(address: SocketAddress) {
  private[this] val map = MapMaker[ChannelBuffer, ChannelBuffer](_.softValues)
  private[this] val interpreter = new Interpreter(map)
  private[this] val service = new InterpreterService(interpreter)

  private[this] val serverSpec =
    ServerBuilder()
      .name("twemcached")
      .codec(Memcached)
      .service(service)
      .bindTo(address)

  private[this] var channel: Option[Channel] = None

  def start() {
    channel = Some(serverSpec.build())
  }

  def stop() {
    require(channel.isDefined, "Channel is not open!")

    channel.foreach { channel =>
      channel.close()
      this.channel = None
    }
  }
}