package com.twitter.twemcached

import com.twitter.util.Future
import com.twitter.finagle.service.Service
import protocol.Command

class InterpreterService(interpreter: Interpreter) extends Service[Command, String] {
  def apply(command: Command) =
    Future(interpreter(command))
}

