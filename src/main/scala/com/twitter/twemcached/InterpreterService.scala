package com.twitter.twemcached

import com.twitter.util.Future
import com.twitter.finagle.service.Service
import protocol.Command
import org.jboss.netty.buffer.ChannelBuffer

class InterpreterService(interpreter: Interpreter) extends Service[Command, ChannelBuffer] {
  def apply(command: Command) =
    Future(interpreter(command))
}