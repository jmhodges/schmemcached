package com.twitter.twemcached.protocol.text

import org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer
import com.twitter.twemcached.protocol._
import org.jboss.netty.buffer.ChannelBuffer

object Show {
  private[this] val DELIMETER  = "\r\n".getBytes
  private[this] val VALUE      = "VALUE ".getBytes
  private[this] val ZERO       = "0".getBytes
  private[this] val SPACE      = " ".getBytes
  private[this] val GET        = "get".getBytes
  private[this] val DELETE     = "delete".getBytes
  private[this] val INCR       = "incr".getBytes
  private[this] val DECR       = "decr".getBytes
  private[this] val ADD        = "add".getBytes
  private[this] val SET        = "set".getBytes
  private[this] val APPEND     = "append".getBytes
  private[this] val PREPEND    = "prepend".getBytes
  private[this] val REPLACE    = "replace".getBytes

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
            wrappedBuffer(key.getBytes, SPACE)
          }: _*), wrappedBuffer(DELIMETER))
      case Incr(key, amount) =>
        wrappedBuffer(INCR, SPACE, amount.toString.getBytes, DELIMETER)
      case Decr(key, amount) =>
        wrappedBuffer(DECR, SPACE, amount.toString.getBytes, DELIMETER)
      case Delete(key) =>
        wrappedBuffer(DELETE, SPACE, key.getBytes, DELIMETER)
    }
  }

  @inline private[this] def showStorageCommand(name: Array[Byte], key: String, value: ChannelBuffer) = {
    wrappedBuffer(
      wrappedBuffer(name, SPACE, key.getBytes, SPACE, ZERO, SPACE, ZERO, SPACE, value.capacity.toString.getBytes, DELIMETER),
      value, wrappedBuffer(DELIMETER))
  }
}