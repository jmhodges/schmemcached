package com.twitter.twemcached.protocol

import text.Parser
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.twemcached.util.ChannelBufferUtils._

object ParseResponse extends Parser[Response] {
  import Parser.DIGITS

  case class ValueLine(tokens: Seq[ChannelBuffer], buffer: ChannelBuffer) {
    val toValue = Value(tokens(1), buffer)
  }
  private[this] val VALUE      = "VALUE": ChannelBuffer
  private[this] val STORED     = "STORED": ChannelBuffer
  private[this] val NOT_STORED = "NOT_STORED": ChannelBuffer
  private[this] val DELETED    = "DELETED": ChannelBuffer
  private[this] val END        = "END": ChannelBuffer

  def needsData(tokens: Seq[ChannelBuffer]) = {
    val responseName = tokens.head
    val args = tokens.tail
    if (responseName == VALUE) {
      validateValueResponse(args)
      Some(args(2).toInt)
    } else None
  }

  def isEnd(tokens: Seq[ChannelBuffer]) =
    (tokens.length == 1 && tokens.head == END)

  def apply(tokens: Seq[ChannelBuffer]) = {
    tokens.head match {
      case STORED     => Stored()
      case NOT_STORED => NotStored()
      case DELETED    => Deleted()
    }
  }

  def parseValues(valueLines: Seq[ValueLine]) = {
    Values(valueLines.map(_.toValue))
  }

  private[this] def validateValueResponse(args: Seq[ChannelBuffer]) = {
    if (args.length < 3) throw new ServerError("Too few arguments")
    if (args.length > 4) throw new ServerError("Too many arguments")
    if (args.length == 4 && !args(3).matches(DIGITS)) throw new ServerError("CAS must be a number")
    if (!args(2).matches(DIGITS)) throw new ServerError("Bytes must be number")

    val (key, flags, bytes) = (args(0), args(1), args(2))
    val cas =
      if (args.length == 4)
        Some(args(4).toInt)
      else
        None

    (key, flags, bytes, cas)
  }

}