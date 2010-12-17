package com.twitter.twemcached.unit.protocol.text

import org.specs.Specification
import org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer
import com.twitter.twemcached.protocol.Add
import com.twitter.twemcached.protocol.text.Show
import com.twitter.twemcached.util.ChannelBufferUtils._

object ShowSpec extends Specification {
  "Show" should {
    "show commands" in {
      val value = "value"
      Show(Add("key", value)) mustEqual wrappedBuffer("add key 0 0 5\r\nvalue\r\n".getBytes)
    }
  }
}