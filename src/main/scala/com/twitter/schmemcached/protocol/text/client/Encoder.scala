package com.twitter.schmemcached.protocol.text.client

import com.twitter.schmemcached.protocol.text.Show
import com.twitter.schmemcached.protocol.Command
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}

object Encoder extends OneToOneEncoder {
  def encode(context: ChannelHandlerContext, channel: Channel, message: AnyRef) = {
    message match {
      case command: Command => Show(command)
    }
  }
}