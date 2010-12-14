package com.twitter.twemcached

import org.jboss.netty.channel.Channel
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.util.MapMaker
import com.twitter.util.StorageUnitConversions._
import java.net.InetSocketAddress

class MemcachedServer(port: Int) {
  private[this] val map = MapMaker[String, String](_.softValues)
  private[this] val interpreter = new Interpreter(map)
  private[this] val service = new InterpreterService(interpreter)

  private[this] val serverSpec =
    ServerBuilder()
      .name("twemcached")
      .codec(new MemcachedCodec(1.megabyte.inBytes.toInt))
      .service(service)
      .bindTo(new InetSocketAddress(port))

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