package com.twitter.twemcached.protocol.text.server

import org.jboss.netty.channel._
import com.twitter.util.StateMachine
import com.twitter.twemcached.protocol.text.{AbstractDecoder, ParseCommand}
import com.twitter.twemcached.protocol.Command
import org.jboss.netty.util.CharsetUtil
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}

class Decoder extends AbstractDecoder with StateMachine {
  case class AwaitingCommand() extends State
  case class AwaitingData(tokens: Seq[String], bytesNeeded: Int) extends State

  private[this] var pipeline: ChannelPipeline = _

  override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    pipeline = ctx.getPipeline
    awaitCommand()
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    e.getCause.printStackTrace()
    awaitCommand()
  }

  def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): Command = {
    state match {
      case AwaitingCommand() =>
        val line = decodeLine(buffer)
        if (line.isDefined) {
          val tokens = ParseCommand.tokenize(line.get)
          val bytesNeeded = ParseCommand.needsData(tokens)
          if (bytesNeeded.isDefined) {
            awaitData(tokens, bytesNeeded.get)
          } else {
            ParseCommand.parse(tokens)
          }
        } else needMoreData
      case AwaitingData(tokens, bytesNeeded) =>
        val data = decodeData(bytesNeeded, buffer)
        if (data.isDefined) {
          awaitCommand()
          ParseCommand(tokens, ChannelBuffers.copiedBuffer(data.get))
        } else needMoreData
    }
  }

  private[this] def awaitData(tokens: Seq[String], bytesNeeded: Int) = {
    state = AwaitingData(tokens, bytesNeeded)
    needMoreData
  }

  private[this] def awaitCommand() {
    state = AwaitingCommand()
  }

  private[this] val needMoreData: Command = null
}