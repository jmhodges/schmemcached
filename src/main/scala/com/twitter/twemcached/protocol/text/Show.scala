package com.twitter.twemcached.protocol.text

import org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer
import com.twitter.twemcached.protocol._
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.twemcached.util.ChannelBufferUtils._

object Show {
  private[this] val DELIMETER: ChannelBuffer = "\r\n"
  private[this] val VALUE    : ChannelBuffer = "VALUE "
  private[this] val ZERO     : ChannelBuffer = "0"
  private[this] val SPACE    : ChannelBuffer = " "
  private[this] val GET      : ChannelBuffer = "get"
  private[this] val DELETE   : ChannelBuffer = "delete"
  private[this] val INCR     : ChannelBuffer = "incr"
  private[this] val DECR     : ChannelBuffer = "decr"
  private[this] val ADD      : ChannelBuffer = "add"
  private[this] val SET      : ChannelBuffer = "set"
  private[this] val APPEND   : ChannelBuffer = "append"
  private[this] val PREPEND  : ChannelBuffer = "prepend"
  private[this] val REPLACE  : ChannelBuffer = "replace"

  private[this] val STORED     = wrappedBuffer("STORED"    , DELIMETER)
  private[this] val END        = wrappedBuffer("END"       , DELIMETER)
  private[this] val NOT_STORED = wrappedBuffer("STORED"    , DELIMETER)
  private[this] val EXISTS     = wrappedBuffer("EXISTS"    , DELIMETER)
  private[this] val NOT_FOUND  = wrappedBuffer("NOT_FOUND" , DELIMETER)
  private[this] val DELETED    = wrappedBuffer("DELETED"   , DELIMETER)

  def apply(response: Response) = {
    response match {
      case Stored()       => STORED
      case NotStored()    => NOT_STORED
      case Deleted()      => DELETED
      case Values(values) =>
        val shown = values map { case Value(key, value) =>
          wrappedBuffer(
            wrappedBuffer(VALUE, key, SPACE, ZERO, SPACE, value.capacity.toString, DELIMETER),
            value,
            wrappedBuffer(DELIMETER))
        }
        wrappedBuffer(wrappedBuffer(shown: _*), END)
    }
  }

  def apply(command: Command): ChannelBuffer = {
    command match {
      case Add(key, value) =>
        showStorageCommand(ADD, key, value)
      case Set(key, value) =>
        showStorageCommand(SET, key, value)
      case Replace(key, value) =>
        showStorageCommand(REPLACE, key, value)
      case Append(key, value) =>
        showStorageCommand(APPEND, key, value)
      case Prepend(key, value) =>
        showStorageCommand(PREPEND, key, value)
      case Get(keys) =>
        apply(Gets(keys))
      case Gets(keys) =>
        wrappedBuffer(wrappedBuffer(GET, SPACE),
          wrappedBuffer(keys.map { key =>
            wrappedBuffer(key, SPACE)
          }: _*), wrappedBuffer(DELIMETER))
      case Incr(key, amount) =>
        wrappedBuffer(INCR, SPACE, amount.toString, DELIMETER)
      case Decr(key, amount) =>
        wrappedBuffer(DECR, SPACE, amount.toString, DELIMETER)
      case Delete(key) =>
        wrappedBuffer(DELETE, SPACE, key, DELIMETER)
    }
  }

  @inline private[this] def showStorageCommand(name: ChannelBuffer, key: ChannelBuffer, value: ChannelBuffer) = {
    wrappedBuffer(
      wrappedBuffer(name, SPACE, key, SPACE, ZERO, SPACE, ZERO, SPACE, value.capacity.toString, DELIMETER),
      value, DELIMETER)
  }
}