package com.twitter.twemcached.protocol

import org.jboss.netty.buffer.ChannelBuffer

sealed abstract class Command

object StorageCommand {
  def unapply(command: StorageCommand) = Some((command.data._1, command.data._2))
}
object ArithmeticCommand {
  def unapply(command: ArithmeticCommand) = Some((command.data._1, command.data._2))
}
object RetrievalCommand {
  def unapply(command: RetrievalCommand) = Some(command.data)
}

abstract class StorageCommand(val data: (String, ChannelBuffer))     extends Command
abstract class ArithmeticCommand(val data: (String, Int))            extends Command
abstract class RetrievalCommand(val data: Seq[String])               extends Command

case class Set(key: String, value: ChannelBuffer)                    extends StorageCommand((key, value))
case class Add(key: String, value: ChannelBuffer)                    extends StorageCommand((key, value))
case class Replace(key: String, value: ChannelBuffer)                extends StorageCommand((key, value))
case class Append(key: String, value: ChannelBuffer)                 extends StorageCommand((key, value))
case class Prepend(key: String, value: ChannelBuffer)                extends StorageCommand((key, value))

case class Get(keys: Seq[String])                                    extends RetrievalCommand(keys)
case class Gets(keys: Seq[String])                                   extends RetrievalCommand(keys)

case class Delete(key: String)                                       extends Command
case class Incr(key: String, value: Int)                             extends ArithmeticCommand((key, value))
case class Decr(key: String, value: Int)                             extends ArithmeticCommand((key, -value))