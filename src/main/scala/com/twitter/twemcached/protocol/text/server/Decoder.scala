package com.twitter.twemcached.protocol.text.server

import org.jboss.netty.channel._
import com.twitter.util.StateMachine
import com.twitter.twemcached.protocol.text.{AbstractDecoder, ParseCommand}
import com.twitter.twemcached.protocol.Command
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}

class Decoder extends AbstractDecoder[Command] with StateMachine {
  case class AwaitingCommand() extends State
  case class AwaitingData(tokens: Seq[String], bytesNeeded: Int) extends State

  def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): Command = {
    state match {
      case AwaitingCommand() =>
        decodeLine(buffer, ParseCommand.needsData(_)) { tokens =>
          ParseCommand(tokens)
        }
      case AwaitingData(tokens, bytesNeeded) =>
        decodeData(bytesNeeded, buffer) { data =>
          ParseCommand(tokens, data)
        }
    }
  }

  protected def awaitData(tokens: Seq[String], bytesNeeded: Int) = {
    state = AwaitingData(tokens, bytesNeeded)
    needMoreData
  }

  protected def start() = {
    state = AwaitingCommand()
  }

  protected val needMoreData: Command = null
}