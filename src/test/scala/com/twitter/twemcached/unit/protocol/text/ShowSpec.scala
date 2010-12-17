package com.twitter.twemcached.unit.protocol.text

import org.specs.Specification
import com.twitter.twemcached.protocol.Add
import com.twitter.twemcached.protocol.text.Show
import org.jboss.netty.util.CharsetUtil
import com.twitter.twemcached.util.ChannelBufferUtils._

object ShowSpec extends Specification {
  "Show" should {
    "show commands" in {
      val value = "value"
      Show(Add("key", value)).toString(CharsetUtil.UTF_8) mustEqual "add key 0 0 5\r\nvalue\r\n"
    }
  }
}