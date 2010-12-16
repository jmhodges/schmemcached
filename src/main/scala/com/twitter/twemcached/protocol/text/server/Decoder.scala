package com.twitter.twemcached.protocol.text.server

import org.jboss.netty.channel._
import com.twitter.util.StateMachine
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.twemcached.protocol.text.{AbstractDecoder, ParseCommand}
import com.twitter.twemcached.protocol.Command

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
        if (line eq null) return null

        val tokens = ParseCommand.tokenize(buffer)
        val bytesNeeded = ParseCommand.needsData(tokens)
        if (bytesNeeded.isDefined) {
          awaitData(tokens, bytesNeeded.get)
          null
        } else {
          ParseCommand.parse(tokens)
        }
      case AwaitingData(tokens, bytesNeeded) =>
        val data = decodeData(bytesNeeded, buffer)
        if (data eq null) return null

        awaitCommand()
        ParseCommand(tokens, buffer)
    }
  }

  private[this] def awaitData(tokens: Seq[String], bytesNeeded: Int) {
    state = AwaitingData(tokens, bytesNeeded)
  }

  private[this] def awaitCommand() {
    state = AwaitingCommand()
  }
}