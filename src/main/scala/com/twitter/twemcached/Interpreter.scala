package com.twitter.twemcached

import protocol._
import scala.collection.mutable
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer
import org.jboss.netty.util.CharsetUtil

class Interpreter(data: mutable.Map[String, ChannelBuffer], concurrencyLevel: Int = 16) {
  private[this] val DIGITS     = "^\\d+$"
  class Lock
  private[this] val stripes    = {
    val array = new Array[AnyRef](concurrencyLevel)
    (0 until concurrencyLevel) foreach { i =>
      array(i) = new Lock
    }
    array
  }

  def apply(command: Command): Response = {
    command match {
      case Set(key, value)      =>
        data(key) = value
        Stored()
      case Add(key, value)      =>
        stripe(key).synchronized {
          val existing = data.get(key)
          if (existing.isDefined)
            NotStored()
          else {
            data(key) = value
            Stored()
          }
        }
      case Replace(key, value)  =>
        stripe(key).synchronized {
          val existing = data.get(key)
          if (existing.isDefined) {
            data(key) = value
            Stored()
          } else {
            NotStored()
          }
        }
      case Append(key, value)   =>
        stripe(key).synchronized {
          val existing = data.get(key)
          if (existing.isDefined) {
            data(key) = wrappedBuffer(value, existing.get)
            Stored()
          } else {
            NotStored()
          }
        }
      case Prepend(key, value)  =>
        stripe(key).synchronized {
          val existing = data.get(key)
          if (existing.isDefined) {
            data(key) = wrappedBuffer(existing.get, value)
            Stored()
          } else {
            NotStored()
          }
        }
      case Get(keys)            =>
        Values(
          keys flatMap { key =>
            data.get(key) map(Value(key, _))
          }
        )
      case Delete(key)  =>
        if (data.remove(key).isDefined)
          Deleted()
        else
          NotStored()
      case Incr(key, value)     =>
        stripe(key).synchronized {
          val existing = data.get(key)
          if (existing.isDefined) {
            data(key) = {
              val existingString = existing.get.toString(CharsetUtil.US_ASCII)
              if (existingString.matches(DIGITS))
                wrappedBuffer((existingString.toInt + value).toString.getBytes)
              else
                wrappedBuffer(value.toString.getBytes)
            }
            Stored()
          } else {
            NotStored()
          }
        }
      case Decr(key, value)     =>
        apply(Incr(key, -value))
    }
  }

  @inline private[this] def stripe(key: String) = stripes(key.hashCode % stripes.length)
}