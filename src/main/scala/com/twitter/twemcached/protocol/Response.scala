package com.twitter.twemcached.protocol

import org.jboss.netty.buffer.ChannelBuffer

sealed abstract class Response
case object Stored                    extends Response
case object NotStored                 extends Response
case object Deleted                   extends Response

case class Values(values: Seq[Value]) extends Response

case class Value(key: ChannelBuffer, value: ChannelBuffer)