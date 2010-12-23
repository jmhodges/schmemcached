package com.twitter.twemcached

import protocol._
import scala.collection.mutable
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer
import org.jboss.netty.util.CharsetUtil
import text.Parser
import com.twitter.twemcached.util.ChannelBufferUtils._
import com.twitter.util.SynchronizedLruMap
import util.AtomicMap

/**
 * Evalutes a given Memcached operation and returns the result.
 */
class Interpreter(map: AtomicMap[ChannelBuffer, ChannelBuffer]) {
  import Parser.DIGITS

  def apply(command: Command): Response = {
    command match {
      case Set(key, value) =>
        map.lock(key) { data =>
          data(key) = value
          Stored
        }
      case Add(key, value) =>
        map.lock(key) { data =>
          val existing = data.get(key)
          if (existing.isDefined)
            NotStored
          else {
            data(key) = value
            Stored
          }
        }
      case Replace(key, value) =>
        map.lock(key) { data =>
          val existing = data.get(key)
          if (existing.isDefined) {
            data(key) = value
            Stored
          } else {
            NotStored
          }
        }
      case Append(key, value) =>
        map.lock(key) { data =>
          val existing = data.get(key)
          if (existing.isDefined) {
            data(key) = wrappedBuffer(value, existing.get)
            Stored
          } else {
            NotStored
          }
        }
      case Prepend(key, value) =>
        map.lock(key) { data =>
          val existing = data.get(key)
          if (existing.isDefined) {
            data(key) = wrappedBuffer(existing.get, value)
            Stored
          } else {
            NotStored
          }
        }
      case Get(keys) =>
        Values(
          keys flatMap { key =>
            map.lock(key) { data =>
              data.get(key) map(Value(key, _))
            }
          }
        )
      case Delete(key) =>
        map.lock(key) { data =>
          if (data.remove(key).isDefined)
            Deleted
          else
            NotFound
        }
      case Incr(key, delta) =>
        map.lock(key) { data =>
          val existing = data.get(key)
          if (existing.isDefined) {
            val existingString = existing.get.toString(CharsetUtil.US_ASCII)
            if (!existingString.isEmpty && !existingString.matches(DIGITS))
              throw new ClientError("cannot increment or decrement non-numeric value")

            val existingValue =
              if (existingString.isEmpty) 0
              else existingString.toInt

            val result = existingValue + delta
            data(key) = result.toString

            Number(result)
          } else {
            NotFound
          }
        }
      case Decr(key, value) =>
        map.lock(key) { data =>
          apply(Incr(key, -value))
        }
    }
  }
}