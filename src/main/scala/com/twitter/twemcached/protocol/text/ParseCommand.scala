package com.twitter.twemcached.protocol.text

import scala.Function.tupled
import com.twitter.twemcached.protocol._
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer
import com.twitter.twemcached.util.ChannelBufferUtils._

object ParseCommand extends Parser[Command] {
  import Parser.DIGITS
  private[this] val NOREPLY = "noreply"
  private[this] val SET = wrappedBuffer("set".getBytes)
  private[this] val ADD = wrappedBuffer("add".getBytes)
  private[this] val REPLACE = wrappedBuffer("replace".getBytes)
  private[this] val APPEND = wrappedBuffer("append".getBytes)
  private[this] val PREPEND = wrappedBuffer("prepend".getBytes)
  private[this] val GET = wrappedBuffer("get".getBytes)
  private[this] val GETS = wrappedBuffer("gets".getBytes)
  private[this] val DELETE = wrappedBuffer("delete".getBytes)
  private[this] val INCR = wrappedBuffer("incr".getBytes)
  private[this] val DECR = wrappedBuffer("decr".getBytes)
  private[this] val storageCommands = collection.Set(
    SET, ADD, REPLACE, APPEND, PREPEND)

  def needsData(tokens: Seq[ChannelBuffer]) = {
    val commandName = tokens.head
    val args = tokens.tail
    if (storageCommands.contains(commandName)) {
      validateStorageCommand(args)
      Some(tokens(4).toInt)
    } else None
  }

  def apply(tokens: Seq[ChannelBuffer], data: ChannelBuffer): Command = {
    val commandName = tokens.head
    val args = tokens.tail
    commandName match {
      case SET       => Set(validateStorageCommand(args), data)
      case ADD       => Add(validateStorageCommand(args), data)
      case REPLACE   => Replace(validateStorageCommand(args), data)
      case APPEND    => Append(validateStorageCommand(args), data)
      case PREPEND   => Prepend(validateStorageCommand(args), data)
      case _         => throw new NonexistentCommand(commandName.toString)
    }
  }

  def apply(tokens: Seq[ChannelBuffer]): Command = {
    val commandName = tokens.head
    val args = tokens.tail
    commandName match {
      case GET     => Get(args)
      case GETS    => Get(args)
      case DELETE  => Delete(validateDeleteCommand(args))
      case INCR    => tupled(Incr)(validateArithmeticCommand(args))
      case DECR    => tupled(Decr)(validateArithmeticCommand(args))
      case _       => throw new NonexistentCommand(commandName.toString)
    }
  }

  private[this] def validateStorageCommand(tokens: Seq[ChannelBuffer]) = {
    if (tokens.size < 4) throw new ClientError("Too few arguments")
    if (tokens.size == 5 && tokens(4) != NOREPLY) throw new ClientError("Too many arguments")
    if (tokens.size > 5) throw new ClientError("Too many arguments")
    if (!tokens(3).matches(DIGITS)) throw new ClientError("Bad frame length")

    tokens.head
  }

  private[this] def validateArithmeticCommand(tokens: Seq[ChannelBuffer]) = {
    if (tokens.size < 2) throw new ClientError("Too few arguments")
    if (tokens.size == 3 && tokens.last != NOREPLY) throw new ClientError("Too many arguments")
    if (!tokens(1).matches(DIGITS)) throw new ClientError("Delta is not a number")

    (tokens.head, tokens(1).toInt)
  }

  private[this] def validateDeleteCommand(tokens: Seq[ChannelBuffer]) = {
    if (tokens.size < 1) throw new ClientError("No key")
    if (tokens.size == 2 && !tokens.last.matches(DIGITS)) throw new ClientError("Timestamp is poorly formed")
    if (tokens.size > 2) throw new ClientError("Too many arguments")

    tokens.head
  }
}