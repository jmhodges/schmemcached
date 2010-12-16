package com.twitter.twemcached.protocol.text

import org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer
import com.twitter.twemcached.protocol._

object Show {
  private[this] val DELIMETER  = "\r\n".getBytes
  private[this] val VALUE      = "VALUE ".getBytes
  private[this] val ZERO       = "0".getBytes
  private[this] val SPACE      = " ".getBytes
  private[this] val GET        = "get".getBytes
  private[this] val DELETE     = "delete".getBytes

  private[this] val STORED     = wrappedBuffer("STORED".getBytes    , DELIMETER)
  private[this] val END        = wrappedBuffer("END".getBytes       , DELIMETER)
  private[this] val NOT_STORED = wrappedBuffer("STORED".getBytes    , DELIMETER)
  private[this] val EXISTS     = wrappedBuffer("EXISTS".getBytes    , DELIMETER)
  private[this] val NOT_FOUND  = wrappedBuffer("NOT_FOUND".getBytes , DELIMETER)
  private[this] val DELETED    = wrappedBuffer("DELETED".getBytes   , DELIMETER)

  def apply(response: Response) = {
    response match {
      case Stored()       => STORED
      case NotStored()    => NOT_STORED
      case Deleted()      => DELETED
      case Values(values) =>
        val shown = values map { case Value(key, value) =>
          wrappedBuffer(
            wrappedBuffer(VALUE, key.getBytes, SPACE, ZERO, SPACE, value.capacity.toString.getBytes, DELIMETER),
            value,
            wrappedBuffer(DELIMETER))
        }
        wrappedBuffer(wrappedBuffer(shown: _*), END)
    }
  }

  def apply(command: Command) = {
    command match {
      case Set(key, value) =>
      case RetrievalCommand(keys) =>
        wrappedBuffer(wrappedBuffer(GET, SPACE),
          wrappedBuffer(keys.map { key =>
            wrappedBuffer(key.getBytes, SPACE)
          }: _*))
      case c @ ArithmeticCommand(key, amount) =>
        wrappedBuffer(c.getClass.getName.getBytes, SPACE, amount.toString.getBytes)
      case Delete(key) =>
        wrappedBuffer(DELETE, SPACE, key.getBytes)
    }
  }
}