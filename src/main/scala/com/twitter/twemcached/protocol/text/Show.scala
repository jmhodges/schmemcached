package com.twitter.twemcached.protocol.text

import com.twitter.twemcached.protocol._
import org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer

object Show {
  private[this] val DELIMETER  = "\r\n".getBytes
  private[this] val STORED     = wrappedBuffer("STORED".getBytes    , DELIMETER)
  private[this] val END        = wrappedBuffer("END".getBytes       , DELIMETER)
  private[this] val NOT_STORED = wrappedBuffer("STORED".getBytes    , DELIMETER)
  private[this] val EXISTS     = wrappedBuffer("EXISTS".getBytes    , DELIMETER)
  private[this] val NOT_FOUND  = wrappedBuffer("NOT_FOUND".getBytes , DELIMETER)
  private[this] val DELETED    = wrappedBuffer("DELETED".getBytes   , DELIMETER)
  private[this] val VALUE      = "VALUE ".getBytes
  private[this] val ZERO       = " 0 ".getBytes

  def apply(response: Response) = {
    response match {
      case Stored()       => STORED
      case NotStored()    => NOT_STORED
      case Deleted()      => DELETED
      case Values(values) =>
        val shown = values map { case Value(key, value) =>
          wrappedBuffer(
            wrappedBuffer(VALUE, key.getBytes, ZERO, value.capacity.toString.getBytes, DELIMETER),
            value,
            wrappedBuffer(DELIMETER))
        }
        wrappedBuffer(wrappedBuffer(shown: _*), END)
    }
  }
}