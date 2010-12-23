package com.twitter.schmemcached

import com.twitter.finagle.service
import protocol._
import com.twitter.schmemcached.util.ChannelBufferUtils._
import com.twitter.util.Future
import org.jboss.netty.util.CharsetUtil

class Client(underlying: service.Client[Command, Response]) {
  def get(key: String) = {
    underlying(Get(Seq(key))) map { case Values(values) =>
      if (values.size > 0) Some(values.head.value)
      else None
    }
  }

  def get(keys: String*) = {
    underlying(Get(keys)) map { case Values(values) =>
      val tuples = values.map { case Value(key, value) =>
        (key.toString(CharsetUtil.UTF_8), value)
      }
      Map(tuples: _*)
    }
  }

  def set(key: String, value: String)             = underlying(Set(key, value))
  def add(key: String, value: String)             = underlying(Add(key, value))
  def append(key: String, value: String)          = underlying(Append(key, value))
  def prepend(key: String, value: String)         = underlying(Prepend(key, value))
  def delete(key: String)                         = underlying(Delete(key))

  def incr(key: String): Future[Int]              = incr(key, 1)
  def incr(key: String, delta: Int): Future[Int]  = {
    underlying(Incr(key, delta)) map { case Number(value) =>
      value
    }
  }

  def decr(key: String): Future[Int]              = decr(key, 1)
  def decr(key: String, delta: Int): Future[Int]  = {
    underlying(Decr(key, delta)) map { case Number(value) =>
      value
    }
  }
}