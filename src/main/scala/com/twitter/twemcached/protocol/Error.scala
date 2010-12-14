package com.twitter.twemcached.protocol

class Error(message: String) extends Exception(message)

class NonexistentCommand(message: String) extends Error(message) {
  override def toString = "ERROR\r\n"
}

class ClientError(message: String) extends Error(message) {
  override def toString = "CLIENT_ERROR " + message + "\r\n"
}

class ServerError(message: String) extends Error(message) {
  override def toString = "SERVER_ERROR " + message + "\r\n"
}