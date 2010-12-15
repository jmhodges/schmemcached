package com.twitter.twemcached

import com.twitter.util.Future
import com.twitter.finagle.service.Service
import protocol.{Response, Command}

class InterpreterService(interpreter: Interpreter) extends Service[Command, Response] {
  def apply(command: Command) = Future(interpreter(command))
}