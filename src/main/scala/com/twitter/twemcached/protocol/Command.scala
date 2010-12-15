package com.twitter.twemcached.protocol

import scala.Function.tupled
import org.jboss.netty.buffer.{ChannelBufferIndexFinder, ChannelBuffer}
import org.jboss.netty.util.CharsetUtil
import collection.mutable.ArrayBuffer

object Command {
  val DIGITS = "^\\d+$"
  val NOREPLY = "noreply"
  private[this] val storageCommands = collection.Set("set", "add", "replace", "append", "prepend")

  def tokenize(_buffer: ChannelBuffer) = {
    val tokens = new ArrayBuffer[String]
    var buffer = _buffer
    while (buffer.capacity > 0) {
      var tokenLength = buffer.bytesBefore(ChannelBufferIndexFinder.LINEAR_WHITESPACE)
      if (tokenLength < 0) tokenLength = buffer.capacity

      tokens += buffer.slice(0, tokenLength).toString(CharsetUtil.US_ASCII)
      buffer = buffer.slice(tokenLength, buffer.capacity)
    }
    tokens
  }

  def needsData(tokens: Seq[String]) = {
    val commandName = tokens.head
    val args = tokens.tail
    if (storageCommands.contains(commandName)) {
      validateStorageCommand(args)
      Some(tokens(3).toInt)
    } else None
  }

  def parse(tokens: Seq[String], data: ChannelBuffer): Command = {
    val commandName = tokens.head
    val args = tokens.tail
    commandName match {
      case "set"     =>
        Set(validateStorageCommand(args), data)
      case "add"     =>
        Add(validateStorageCommand(args), data)
      case "replace" =>
        Replace(validateStorageCommand(args), data)
      case "append"  =>
        Append(validateStorageCommand(args), data)
      case "prepend" =>
        Prepend(validateStorageCommand(args), data)
      case _         =>
        throw new NonexistentCommand(commandName)
    }
  }

  def parse(tokens: Seq[String]): Command = {
    val commandName = tokens.head
    val args = tokens.tail
    commandName match {
      case "get"     =>
        Get(args)
      case "gets"    =>
        Get(args)
      case "delete"  =>
        Delete(validateDeleteCommand(args))
      case "incr"    =>
        tupled(Incr)(validateArithmeticCommand(args))
      case "decr"    =>
        tupled(Decr)(validateArithmeticCommand(args))
      case _         =>
        throw new NonexistentCommand(commandName)
    }
  }

  private[this] def validateStorageCommand(tokens: Seq[String]) = {
    if (tokens.size < 4)
      throw new ClientError("Too few arguments")
    if (tokens.size == 5 && tokens(4) != NOREPLY)
      throw new ClientError("Too many arguments")
    if (tokens.size > 5)
      throw new ClientError("Too many arguments")
    if (!tokens(3).matches(DIGITS))
      throw new ClientError("Bad frame length")

    tokens.head
  }

  private[this] def validateArithmeticCommand(tokens: Seq[String]) = {
    if (tokens.size < 2)
      throw new ClientError("Too few arguments")
    if (tokens.size == 3 && tokens.last != NOREPLY)
      throw new ClientError("Too many arguments")
    if (!tokens(1).matches(Command.DIGITS))
      throw new ClientError("Delta is not a number")

    (tokens.head, tokens(1).toInt)
  }

  private[this] def validateDeleteCommand(tokens: Seq[String]) = {
    if (tokens.size < 1)
      throw new ClientError("No key")
    if (tokens.size == 2 && !tokens.last.matches(Command.DIGITS))
      throw new ClientError("Timestamp is poorly formed")
    if (tokens.size > 2)
      throw new ClientError("Too many arguments")

    tokens.head
  }
}

sealed abstract class Command
abstract class StorageCommand(key: String, value: ChannelBuffer) extends Command
abstract class ArithmeticCommand(key: String, delta: Int) extends Command
abstract class RetrievalCommand(keys: Seq[String])        extends Command

case class Set(key: String, value: ChannelBuffer)                extends StorageCommand(key, value)
case class Add(key: String, value: ChannelBuffer)                extends StorageCommand(key, value)
case class Replace(key: String, value: ChannelBuffer)            extends StorageCommand(key, value)
case class Append(key: String, value: ChannelBuffer)             extends StorageCommand(key, value)
case class Prepend(key: String, value: ChannelBuffer)            extends StorageCommand(key, value)

case class Get(keys: Seq[String])                         extends RetrievalCommand(keys)
case class Gets(keys: Seq[String])                        extends RetrievalCommand(keys)

case class Delete(key: String)                            extends Command
case class Incr(key: String, value: Int)                  extends ArithmeticCommand(key, value)
case class Decr(key: String, value: Int)                  extends ArithmeticCommand(key, -value)