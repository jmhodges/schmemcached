package com.twitter.twemcached.protocol.text.client

import org.jboss.netty.channel._
import com.twitter.util.StateMachine
import com.twitter.twemcached.protocol.ParseResponse.ValueLine
import com.twitter.twemcached.protocol.{Response, ServerError, ParseResponse}
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import com.twitter.twemcached.protocol.text.{Parser, AbstractDecoder}

class Decoder extends AbstractDecoder[Response] with StateMachine {
  case class AwaitingResponse()                                             extends State
  case class AwaitingResponseOrEnd(valuesSoFar: Seq[ValueLine])             extends State
  case class AwaitingData(valuesSoFar: Seq[ValueLine], tokens: Seq[String], bytesNeeded: Int) extends State

  def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): Response = {
    state match {
      case AwaitingResponse() =>
        decodeLine(buffer, ParseResponse.needsData(_)) { tokens =>
          ParseResponse(tokens)
        }
      case AwaitingData(valuesSoFar, tokens, bytesNeeded) =>
        decodeData(bytesNeeded, buffer) { data =>
          awaitResponseOrEnd(
            valuesSoFar ++
            Seq(ValueLine(tokens, ChannelBuffers.copiedBuffer(data))))
        }
      case AwaitingResponseOrEnd(valuesSoFar) =>
        decodeLine(buffer, ParseResponse.needsData(_)) { tokens =>
          if (ParseResponse.isEnd(tokens)) {
            ParseResponse.parseValues(valuesSoFar)
          } else needMoreData
        }
    }
  }

  protected def awaitData(tokens: Seq[String], bytesNeeded: Int) = {
    state match {
      case AwaitingResponse() =>
        awaitData(Seq(), tokens, bytesNeeded)
      case AwaitingResponseOrEnd(valuesSoFar) =>
        awaitData(valuesSoFar, tokens, bytesNeeded)
    }
  }

  private[this] def awaitData(valuesSoFar: Seq[ValueLine], tokens: Seq[String], bytesNeeded: Int) = {
    state = AwaitingData(valuesSoFar, tokens, bytesNeeded)
    null
  }

  protected def start() {
    state = AwaitingResponse()
  }

  private[this] def awaitResponseOrEnd(valuesSoFar: Seq[ValueLine]) = {
    state = AwaitingResponseOrEnd(valuesSoFar)
    null
  }

  protected val needMoreData: Response = null
}