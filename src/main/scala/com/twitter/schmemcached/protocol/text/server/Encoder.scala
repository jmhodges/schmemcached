package com.twitter.schmemcached.protocol.text.server

import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import com.twitter.schmemcached.protocol.text.Show
import com.twitter.schmemcached.protocol.Response

object Encoder extends OneToOneEncoder {
  def encode(context: ChannelHandlerContext, channel: Channel, message: AnyRef) = {
    message match {
      case response: Response => Show(response)
    }
  }
}