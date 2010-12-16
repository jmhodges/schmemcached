package com.twitter.twemcached.protocol

import org.jboss.netty.buffer.ChannelBuffer

sealed abstract class Command

abstract class StorageCommand(key: String, value: ChannelBuffer) extends Command
abstract class ArithmeticCommand(key: String, delta: Int)        extends Command
abstract class RetrievalCommand(keys: Seq[String])               extends Command

case class Set(key: String, value: ChannelBuffer)                extends StorageCommand(key, value)
case class Add(key: String, value: ChannelBuffer)                extends StorageCommand(key, value)
case class Replace(key: String, value: ChannelBuffer)            extends StorageCommand(key, value)
case class Append(key: String, value: ChannelBuffer)             extends StorageCommand(key, value)
case class Prepend(key: String, value: ChannelBuffer)            extends StorageCommand(key, value)

case class Get(keys: Seq[String])                                extends RetrievalCommand(keys)
case class Gets(keys: Seq[String])                               extends RetrievalCommand(keys)

case class Delete(key: String)                                   extends Command
case class Incr(key: String, value: Int)                         extends ArithmeticCommand(key, value)
case class Decr(key: String, value: Int)                         extends ArithmeticCommand(key, -value)