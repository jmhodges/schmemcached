package com.twitter.twemcached.protocol

import scala.Function.tupled

object Command {
  private[this] val DIGITS = "^\\d+$"

  def apply(string: String): Command = {
    val tokens = string.split("\\s")
    if (tokens.size < 1) throw new ClientError("No command name given")

    val commandName = tokens.head
    val args = tokens.tail

    commandName match {
      case "set"     =>
        tupled(Set)(parseStorageArgs(args))
      case "add"     =>
        tupled(Add)(parseStorageArgs(args))
      case "replace" =>
        tupled(Replace)(parseStorageArgs(args))
      case "append"  =>
        tupled(Append)(parseStorageArgs(args))
      case "prepend" =>
        tupled(Prepend)(parseStorageArgs(args))
      case "get"     =>
        Get(args)
      case "gets"    =>
        Gets(args)
      case "delete"  =>
        if (args.size < 1)
          throw new ClientError("No key")
        if (args.size == 2 && !args.last.matches(DIGITS))
          throw new ClientError("Timestamp is poorly formed")
        if (args.size > 2)
          throw new ClientError("Too many arguments")
        Delete(args.head)
      case "incr"    =>
        tupled(Incr)(parseArithmeticArgs(args))
      case "decr"    =>
        tupled(Decr)(parseArithmeticArgs(args))
      case _         =>
        throw new NonexistentCommand(commandName)
    }
  }

  private[this] def parseStorageArgs(args: Seq[String]) = {
    if (args.size < 4)
      throw new ClientError("Too few arguments")
    if (args.size == 5 && args.last != "noreply")
      throw new ClientError("Too many arguments")
    if (args.size > 5)
      throw new ClientError("Too many arguments")

    val (key, flags, exptime, bytes) = (args(0), args(1), args(2), args(3))
    (key, bytes)
  }

  private[this] def parseArithmeticArgs(args: Seq[String]) = {
    if (args.size < 2)
      throw new ClientError("Too few arguments")
    if (args.size == 3 && args.last != "noreply")
      throw new ClientError("Too many arguments")
    if (!args(1).matches(DIGITS))
      throw new ClientError("Delta is not a number")

    val key = args.head
    val delta = args(1).toInt
    (key, delta)
  }
}

sealed abstract class Command
abstract class ReplyingCommand(sendReply: Boolean)        extends Command
abstract class StorageCommand(key: String, value: String) extends ReplyingCommand(true)
abstract class ArithmeticCommand(key: String, delta: Int) extends ReplyingCommand(true)
abstract class RetrievalCommand(keys: Seq[String])        extends Command

case class Set(key: String, value: String)                extends StorageCommand(key, value)
case class Add(key: String, value: String)                extends StorageCommand(key, value)
case class Replace(key: String, value: String)            extends StorageCommand(key, value)
case class Append(key: String, value: String)             extends StorageCommand(key, value)
case class Prepend(key: String, value: String)            extends StorageCommand(key, value)

case class Get(keys: Seq[String])                         extends RetrievalCommand(keys)
case class Gets(keys: Seq[String])                        extends RetrievalCommand(keys)

case class Delete(key: String)                            extends Command
case class Incr(key: String, value: Int)                  extends ArithmeticCommand(key, value)
case class Decr(key: String, value: Int)                  extends ArithmeticCommand(key, -value)