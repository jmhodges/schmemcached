package com.twitter.twemcached.protocol.text.client

import org.jboss.netty.channel._
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.util.StateMachine
import com.twitter.twemcached.protocol.text.AbstractDecoder
import com.twitter.twemcached.protocol.ParseResponse.ValueLine
import com.twitter.twemcached.protocol.{ServerError, ParseResponse}

class Decoder extends AbstractDecoder with StateMachine {
  case class AwaitingResponse()                                             extends State
  case class AwaitingResponseOrEnd(valuesSoFar: Seq[ValueLine])             extends State
  case class AwaitingData(valuesSoFar: Seq[ValueLine], tokens: Seq[String]) extends State

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
        val tokens = ParseResponse.tokenize(data)
        val bytesNeeded = ParseResponse.needsData(tokens)
        if (bytesNeeded.isDefined) {
          awaitData(Seq(), tokens, bytesNeeded.get)
        } else {
          Channels.fireMessageReceived(ctx, ParseResponse(tokens))
        }
      case AwaitingData(valuesSoFar, tokens) =>
        pipeline.remove("decodeData")
        awaitResponseOrEnd(valuesSoFar ++ Seq(ValueLine(tokens, data)))
      case AwaitingResponseOrEnd(valuesSoFar) =>
        val tokens = ParseResponse.tokenize(data)
        if (ParseResponse.isEnd(tokens)) {
          Channels.fireMessageReceived(ctx, valuesSoFar)
        } else {
          val bytesNeeded = ParseResponse.needsData(tokens)
          if (!bytesNeeded.isDefined) throw new ServerError("Invalid state transition")
          awaitData(valuesSoFar, tokens, bytesNeeded.get)
        }
    }
  }

  private[this] def awaitData(valuesSoFar: Seq[ValueLine], tokens: Seq[String], bytesNeeded: Int) {
    state = AwaitingData(valuesSoFar, tokens)
    pipeline.remove("decodeLine")
    pipeline.addBefore("decoder", "decodeData", new DecodeData(bytesNeeded))
  }

  private[this] def awaitResponse() {
    state = AwaitingResponse()
    pipeline.addBefore("decoder", "decodeLine", DecodeLine)
  }

  private[this] def awaitResponseOrEnd(valuesSoFar: Seq[ValueLine]) {
    state = AwaitingResponseOrEnd(valuesSoFar)
    pipeline.addBefore("decoder", "decodeLine", DecodeLine)
  }
}