package com.twitter.twemcached.protocol.text.client

import org.jboss.netty.channel._
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.util.StateMachine
import com.twitter.twemcached.protocol.text.AbstractDecoder
import com.twitter.twemcached.protocol.ParseResponse.ValueLine
import com.twitter.twemcached.protocol.{Values, Response, ServerError, ParseResponse}

class Decoder extends AbstractDecoder with StateMachine {
  case class AwaitingResponse()                                             extends State
  case class AwaitingResponseOrEnd(valuesSoFar: Seq[ValueLine])             extends State
  case class AwaitingData(valuesSoFar: Seq[ValueLine], tokens: Seq[String], bytesNeeded: Int) extends State

  private[this] var pipeline: ChannelPipeline = _

  override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    pipeline = ctx.getPipeline
    awaitResponse()
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    e.getCause.printStackTrace()
    awaitResponse()
  }

  def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): Response = {
    state match {
      case AwaitingResponse() =>
        val line = decodeLine(buffer)
        if (line eq null) return null

        val tokens = ParseResponse.tokenize(buffer)
        val bytesNeeded = ParseResponse.needsData(tokens)
        if (bytesNeeded.isDefined) {
          awaitData(Seq(), tokens, bytesNeeded.get)
          null
        } else {
          ParseResponse(tokens)
        }
      case AwaitingData(valuesSoFar, tokens, bytesNeeded) =>
        awaitResponseOrEnd(valuesSoFar ++ Seq(ValueLine(tokens, buffer)))
        null
      case AwaitingResponseOrEnd(valuesSoFar) =>
        val tokens = ParseResponse.tokenize(buffer)
        if (ParseResponse.isEnd(tokens)) {
          ParseResponse.parseValues(valuesSoFar)
        } else {
          val bytesNeeded = ParseResponse.needsData(tokens)
          if (!bytesNeeded.isDefined) throw new ServerError("Invalid state transition")
          awaitData(valuesSoFar, tokens, bytesNeeded.get)
          null
        }
    }
  }

  private[this] def awaitData(valuesSoFar: Seq[ValueLine], tokens: Seq[String], bytesNeeded: Int) {
    state = AwaitingData(valuesSoFar, tokens, bytesNeeded)
  }

  private[this] def awaitResponse() {
    state = AwaitingResponse()
  }

  private[this] def awaitResponseOrEnd(valuesSoFar: Seq[ValueLine]) {
    state = AwaitingResponseOrEnd(valuesSoFar)
  }
}