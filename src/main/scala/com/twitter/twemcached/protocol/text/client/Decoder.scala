package com.twitter.twemcached.protocol.text.client

import org.jboss.netty.channel._
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.util.StateMachine
import com.twitter.twemcached.protocol.text.{AbstractDecoder, ParseCommand}

class Decoder extends AbstractDecoder with StateMachine {
  case class AwaitingResponse()                extends State
  case class AwaitingEnd()                     extends State
  case class AwaitingData(tokens: Seq[String]) extends State

  private[this] var pipeline: ChannelPipeline = _

  override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    pipeline = ctx.getPipeline
    awaitResponse()
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    e.getCause.printStackTrace()
    awaitResponse()
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val data = e.getMessage.asInstanceOf[ChannelBuffer]

    state match {
      case AwaitingResponse() =>
        val tokens = ParseCommand.tokenize(data)
        val bytesNeeded = ParseCommand.needsData(tokens)
        if (bytesNeeded.isDefined) {
          awaitData(tokens, bytesNeeded.get)
        } else {
          Channels.fireMessageReceived(ctx, ParseCommand.parse(tokens))
        }
      case AwaitingData(tokens) =>
        Channels.fireMessageReceived(ctx, ParseCommand(tokens, data))
        pipeline.remove("decodeData")
        awaitResponse()
    }
  }

  private[this] def awaitData(tokens: Seq[String], bytesNeeded: Int) {
    state = AwaitingData(tokens)
    pipeline.remove("decodeLine")
    pipeline.addBefore("decoder", "decodeData",
      new DecodeData(bytesNeeded))
  }

  private[this] def awaitResponse() {
    state = AwaitingResponse()
    pipeline.addBefore("decoder", "decodeLine", DecodeLine)
  }
}