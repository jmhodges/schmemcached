package com.twitter.twemcached.protocol

import scala.Function.tupled

object Command {
  val DIGITS = "^\\d+$"
  private[this] val storageCommands = collection.Set("set", "add", "replace", "append", "prepend")

  def isStorageCommand(commandName: String) = storageCommands.contains(commandName)

  def apply(commandName: String)(args: Seq[String]): Command = {
    commandName match {
      case "set"     =>
        tupled(Set)(StorageCommand.validate(args))
      case "add"     =>
        tupled(Add)(StorageCommand.validate(args))
      case "replace" =>
        tupled(Replace)(StorageCommand.validate(args))
      case "append"  =>
        tupled(Append)(StorageCommand.validate(args))
      case "prepend" =>
        tupled(Prepend)(StorageCommand.validate(args))
      case "get"     =>
        Get(args)
      case "gets"    =>
        Gets(args)
      case "delete"  =>
        Delete(args.head)
      case "incr"    =>
        tupled(Incr)(ArithmeticCommand.validate(args))
      case "decr"    =>
        tupled(Decr)(ArithmeticCommand.validate(args))
      case _         =>
        throw new NonexistentCommand(commandName)
    }
  }
}

sealed abstract class Command
abstract class ReplyingCommand(sendReply: Boolean)        extends Command

object StorageCommand {
  def validate(args: Seq[String]) = {
    if (args.size < 5)
      throw new ClientError("Too few arguments")
    if (args.size == 6 && args(5) != "noreply")
      throw new ClientError("Too many arguments")
    if (args.size > 6)
      throw new ClientError("Too many arguments")

    (args.head, args.last)
  }
}

abstract class StorageCommand(key: String, value: String) extends ReplyingCommand(true)

object ArithmeticCommand {
  def validate(args: Seq[String]) = {
    if (args.size < 2)
      throw new ClientError("Too few arguments")
    if (args.size == 3 && args.last != "noreply")
      throw new ClientError("Too many arguments")
    if (!args(1).matches(Command.DIGITS))
      throw new ClientError("Delta is not a number")

    (args.head, args(1).toInt)
  }
}

abstract class ArithmeticCommand(key: String, delta: Int) extends ReplyingCommand(true)
abstract class RetrievalCommand(keys: Seq[String])        extends Command

case class Set(key: String, value: String)                extends StorageCommand(key, value)
case class Add(key: String, value: String)                extends StorageCommand(key, value)
case class Replace(key: String, value: String)            extends StorageCommand(key, value)
case class Append(key: String, value: String)             extends StorageCommand(key, value)
case class Prepend(key: String, value: String)            extends StorageCommand(key, value)

case class Get(keys: Seq[String])                         extends RetrievalCommand(keys)
case class Gets(keys: Seq[String])                        extends RetrievalCommand(keys)

object Delete {
  def validate(args: Seq[String]) {
    if (args.size < 1)
      throw new ClientError("No key")
    if (args.size == 2 && !args.last.matches(Command.DIGITS))
      throw new ClientError("Timestamp is poorly formed")
    if (args.size > 2)
      throw new ClientError("Too many arguments")
  }
}

case class Delete(key: String)                            extends Command
case class Incr(key: String, value: Int)                  extends ArithmeticCommand(key, value)
case class Decr(key: String, value: Int)                  extends ArithmeticCommand(key, -value)