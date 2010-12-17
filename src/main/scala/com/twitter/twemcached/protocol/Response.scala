package com.twitter.twemcached.protocol

import org.jboss.netty.buffer.ChannelBuffer

sealed abstract class Response
case class Stored()                   extends Response
case class NotStored()                extends Response
case class Deleted()                  extends Response

case class Values(values: Seq[Value]) extends Response

case class Value(key: ChannelBuffer, value: ChannelBuffer)
