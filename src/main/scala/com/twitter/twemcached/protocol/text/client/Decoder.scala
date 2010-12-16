package com.twitter.twemcached.protocol.text.client

import org.jboss.netty.channel._
import com.twitter.util.StateMachine
import com.twitter.twemcached.protocol.text.AbstractDecoder
import com.twitter.twemcached.protocol.ParseResponse.ValueLine
import com.twitter.twemcached.protocol.{Values, Response, ServerError, ParseResponse}
import org.jboss.netty.util.CharsetUtil
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}

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
        if (line.isDefined) {
          val tokens = ParseResponse.tokenize(line.get)
          val bytesNeeded = ParseResponse.needsData(tokens)
          if (bytesNeeded.isDefined) {
            awaitData(Seq(), tokens, bytesNeeded.get)
          } else {
            ParseResponse(tokens)
          }
        } else needMoreData
      case AwaitingData(valuesSoFar, tokens, bytesNeeded) =>
        val data = decodeData(bytesNeeded, buffer)
        if (data.isDefined) {
          awaitResponseOrEnd(
            valuesSoFar ++
            Seq(ValueLine(tokens, ChannelBuffers.copiedBuffer(data.get))))
        } else needMoreData
      case AwaitingResponseOrEnd(valuesSoFar) =>
        val line = decodeLine(buffer)
        if (line.isDefined) {
          val tokens = ParseResponse.tokenize(line.get)
          if (ParseResponse.isEnd(tokens)) {
            ParseResponse.parseValues(valuesSoFar)
          } else {
            val bytesNeeded = ParseResponse.needsData(tokens)
            if (!bytesNeeded.isDefined) throw new ServerError("Invalid state transition")
            awaitData(valuesSoFar, tokens, bytesNeeded.get)
          }
        } else needMoreData
    }
  }

  private[this] def awaitData(valuesSoFar: Seq[ValueLine], tokens: Seq[String], bytesNeeded: Int) = {
    state = AwaitingData(valuesSoFar, tokens, bytesNeeded)
    needMoreData
  }

  private[this] def awaitResponse() {
    state = AwaitingResponse()
  }

  private[this] def awaitResponseOrEnd(valuesSoFar: Seq[ValueLine]) = {
    state = AwaitingResponseOrEnd(valuesSoFar)
    needMoreData
  }

  private[this] val needMoreData: Response = null
}