package com.twitter.twemcached.protocol

import text.Parser
import org.jboss.netty.buffer.ChannelBuffer

object ParseResponse extends Parser {
  private[this] val VALUE      = "VALUE"
  private[this] val STORED     = "STORED"
  private[this] val NOT_STORED = "NOT_STORED"
  private[this] val DELETED    = "DELETED"

  def needsData(tokens: Seq[String]) = {
    val responseName = tokens.head
    val args = tokens.tail
    if (responseName == VALUE) {
      validateValueResponse(args)
      Some(args(2).toInt)
    } else None
  }

  def apply(tokens: Seq[String]): Response = {
    tokens.head match {
      case STORED     => Stored()
      case NOT_STORED => NotStored()
      case DELETED    => Deleted()
    }
  }

  def parseValues(values: Seq[(Seq[String], ChannelBuffer)]): Response = {
    val vs = values map { case (tokens, buffer) =>
      Value(tokens.head, buffer)
    }
    Values(vs)
  }

  private[this] def validateValueResponse(args: Seq[String]) = {
    if (args.length < 3) throw new ServerError("Too few arguments")
    if (args.length > 4) throw new ServerError("Too many arguments")
    if (args.length == 4 && !args(3).matches(DIGITS)) throw new ServerError("CAS must be a number")
    if (!args(2).matches(DIGITS)) throw new ServerError("Bytes must be number")

    val (key, flags, bytes) = (args(0), args(1), args(2))
    val cas = if (args.length == 4) Some(args(4).toInt) else None

    (key, flags, bytes, cas)
  }

}