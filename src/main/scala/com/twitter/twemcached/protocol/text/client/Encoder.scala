package com.twitter.twemcached.protocol.text.client

import org.jboss.netty.channel.{MessageEvent, ChannelHandlerContext, SimpleChannelDownstreamHandler}
import com.twitter.twemcached.protocol.text.Show
import com.twitter.twemcached.protocol.Response

object Encoder extends SimpleChannelDownstreamHandler {
  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) = {
    e.getMessage match {
      case response: Response => Show(response)
    }
  }
}