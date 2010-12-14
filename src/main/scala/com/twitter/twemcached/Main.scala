package com.twitter.twemcached

object Main {
  def main(args: Array[String]) {
    val server = new MemcachedServer(11214)
    server.start()
  }
}