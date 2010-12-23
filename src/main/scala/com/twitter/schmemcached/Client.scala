package com.twitter.schmemcached

import com.twitter.finagle.service
import protocol._
import com.twitter.schmemcached.util.ChannelBufferUtils._
import com.twitter.util.Future
import org.jboss.netty.util.CharsetUtil
import org.jboss.netty.buffer.ChannelBuffer

object Client {
  def apply(services: Seq[service.Client[Command, Response]]): Client = {
    new PartitionedClient(services.map(apply(_)), _.hashCode)
  }

  def apply(raw: service.Client[Command, Response]): Client = {
    new ConnectedClient(raw)
  }
}

trait Client {
  def get(key: String):                    Future[Option[ChannelBuffer]]
  def get(keys: String*):                  Future[Map[String, ChannelBuffer]]
  def set(key: String, value: String):     Future[Response]
  def add(key: String, value: String):     Future[Response]
  def append(key: String, value: String):  Future[Response]
  def prepend(key: String, value: String): Future[Response]
  def delete(key: String):                 Future[Response]
  def incr(key: String):                   Future[Int]
  def incr(key: String, delta: Int):       Future[Int]
  def decr(key: String):                   Future[Int]
  def decr(key: String, delta: Int):       Future[Int]
}

protected class ConnectedClient(underlying: service.Client[Command, Response]) extends Client {
  def get(key: String) = {
    underlying(Get(Seq(key))) map {
      case Values(values) =>
        if (values.size > 0) Some(values.head.value)
        else None
    }
  }

  def get(keys: String*) = {
    underlying(Get(keys)) map {
      case Values(values) =>
        val tuples = values.map {
          case Value(key, value) =>
            (key.toString(CharsetUtil.UTF_8), value)
        }
        Map(tuples: _*)
    }
  }

  def set(key: String, value: String) = underlying(Set(key, value))
  def add(key: String, value: String) = underlying(Add(key, value))
  def append(key: String, value: String) = underlying(Append(key, value))
  def prepend(key: String, value: String) = underlying(Prepend(key, value))
  def delete(key: String) = underlying(Delete(key))
  def incr(key: String): Future[Int] = incr(key, 1)
  def incr(key: String, delta: Int): Future[Int] = {
    underlying(Incr(key, delta)) map {
      case Number(value) =>
        value
    }
  }

  def decr(key: String): Future[Int] = decr(key, 1)

  def decr(key: String, delta: Int): Future[Int] = {
    underlying(Decr(key, delta)) map {
      case Number(value) =>
        value
    }
  }
}

protected class PartitionedClient(clients: Seq[Client], hash: String => Long) extends Client {
  def get(key: String)                    = idx(key).get(key)
  def get(keys: String*)                  = {
    val keysGroupedByClient = keys.groupBy(idx(_))

    val mapOfMaps = keysGroupedByClient.map { case (client, keys) =>
      client.get(keys: _*)
    }

    mapOfMaps.reduceLeft { (result, nextMap) =>
      for {
        result <- result
        nextMap <- nextMap
      } yield {
        result ++ nextMap
      }
    }
  }

  def set(key: String, value: String)     = idx(key).set(key, value)
  def add(key: String, value: String)     = idx(key).add(key, value)
  def append(key: String, value: String)  = idx(key).append(key, value)
  def prepend(key: String, value: String) = idx(key).prepend(key, value)
  def delete(key: String)                 = idx(key).delete(key)
  def incr(key: String)                   = idx(key).incr(key)
  def incr(key: String, delta: Int)       = idx(key).incr(key, delta)
  def decr(key: String)                   = idx(key).decr(key)
  def decr(key: String, delta: Int)       = idx(key).decr(key, delta)

  private[this] def idx(key: String) = {
    clients((hash(key) % clients.size).toInt)
  }
}