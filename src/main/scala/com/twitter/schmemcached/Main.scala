package com.twitter.schmemcached

import java.net.InetSocketAddress

object Main {
  def main(args: Array[String]) {
    val server = new Server(new InetSocketAddress(11214))
    server.start()
  }
}