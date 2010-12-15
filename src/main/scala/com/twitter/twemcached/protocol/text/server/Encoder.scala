package com.twitter.twemcached.protocol.text.server

import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import com.twitter.twemcached.protocol.text.Show
import com.twitter.twemcached.protocol.Response

object Encoder extends OneToOneEncoder {
  def encode(context: ChannelHandlerContext, channel: Channel, message: AnyRef) = {
    message match {
      case response: Response => Show(response)
    }
  }
}