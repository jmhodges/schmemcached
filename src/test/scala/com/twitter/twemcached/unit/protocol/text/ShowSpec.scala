package com.twitter.twemcached.unit.protocol.text

import org.specs.Specification
import org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer
import com.twitter.twemcached.protocol.Add
import com.twitter.twemcached.protocol.text.Show

object ShowSpec extends Specification {
  "Show" should {
    "show commands" in {
      val value = wrappedBuffer("value".getBytes)
      Show(Add("key", value)) mustEqual wrappedBuffer("add key 0 0 5\r\nvalue\r\n".getBytes)
    }
  }
}