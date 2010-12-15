package com.twitter.twemcached

import protocol._
import scala.collection.mutable
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer
import org.jboss.netty.util.CharsetUtil

class Interpreter(data: mutable.Map[String, ChannelBuffer]) {
  private[this] val DIGITS     = "^\\d+$"
  private[this] val DELIMETER  = "\r\n".getBytes
  private[this] val END        = wrappedBuffer("END".getBytes       , DELIMETER)
  private[this] val STORED     = wrappedBuffer("STORED".getBytes    , DELIMETER)
  private[this] val NOT_STORED = wrappedBuffer("STORED".getBytes    , DELIMETER)
  private[this] val EXISTS     = wrappedBuffer("EXISTS".getBytes    , DELIMETER)
  private[this] val NOT_FOUND  = wrappedBuffer("NOT_FOUND".getBytes , DELIMETER)
  private[this] val DELETED    = wrappedBuffer("DELETED".getBytes   , DELIMETER)

  private[this] case class Value(key: String, value: ChannelBuffer) {
    val VALUE = "VALUE ".getBytes
    val ZERO = " 0 ".getBytes

    def toMessage = wrappedBuffer(
      wrappedBuffer(VALUE, key.getBytes, ZERO, value.capacity.toString.getBytes, DELIMETER),
      value,
      wrappedBuffer(DELIMETER))
  }
  private[this] case class Values(values: Seq[Value]) {
    def toMessage = wrappedBuffer(wrappedBuffer(values.map(_.toMessage): _*), END)
  }

  def apply(command: Command): ChannelBuffer = {
    command match {
      case Set(key, value)      =>
        data(key) = value
        STORED
      case Add(key, value)      =>
        synchronized {
          val existing = data.get(key)
          if (existing.isDefined)
            NOT_STORED
          else {
            data(key) = value
            STORED
          }
        }
      case Replace(key, value)  =>
        synchronized {
          val existing = data.get(key)
          if (existing.isDefined) {
            data(key) = value
            STORED
          } else {
            NOT_STORED
          }
        }
      case Append(key, value)   =>
        synchronized {
          val existing = data.get(key)
          if (existing.isDefined) {
            data(key) = wrappedBuffer(value, existing.get)
            STORED
          } else {
            NOT_STORED
          }
        }
      case Prepend(key, value)  =>
        synchronized {
          val existing = data.get(key)
          if (existing.isDefined) {
            data(key) = wrappedBuffer(existing.get, value)
            STORED
          } else {
            NOT_STORED
          }
        }
      case Get(keys)            =>
        Values(
          keys flatMap { key =>
            data.get(key) map(Value(key, _))
          }
        ).toMessage
      case Delete(key)  =>
        if (data.remove(key).isDefined)
          DELETED
        else
          NOT_STORED
      case Incr(key, value)     =>
        synchronized {
          val existing = data.get(key)
          if (existing.isDefined) {
            data(key) = {
              val existingString = existing.get.toString(CharsetUtil.US_ASCII)
              if (existingString.matches(DIGITS))
                wrappedBuffer((existingString.toInt + value).toString.getBytes)
              else
                wrappedBuffer(value.toString.getBytes)
            }
            STORED
          } else {
            NOT_STORED
          }
        }
      case Decr(key, value)     =>
        apply(Incr(key, -value))
    }
  }
}