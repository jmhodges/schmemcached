package com.twitter.twemcached.protocol.text

import com.twitter.twemcached.protocol.ClientError
import org.jboss.netty.handler.codec.frame.FrameDecoder
import org.jboss.netty.channel.{SimpleChannelUpstreamHandler, ChannelHandlerContext, Channel}
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBufferIndexFinder, ChannelBuffer}

class AbstractDecoder extends SimpleChannelUpstreamHandler {
  private[this] val DELIMETER = ChannelBuffers.wrappedBuffer("\r\n".getBytes)
  
  protected object DecodeLine extends FrameDecoder {
    def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): ChannelBuffer = {
      val frameLength = buffer.bytesBefore(ChannelBufferIndexFinder.CRLF)
      if (frameLength < 0) return null

      val frame = buffer.slice(buffer.readerIndex, frameLength)
      buffer.skipBytes(frameLength + DELIMETER.capacity)
      frame
    }
  }

  protected class DecodeData(bytesNeeded: Int) extends FrameDecoder {
    def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): ChannelBuffer = {
      if (buffer.readableBytes < bytesNeeded + DELIMETER.capacity) return null
      val lastTwoBytesInFrame = buffer.slice(bytesNeeded + buffer.readerIndex, DELIMETER.capacity)

      if (!lastTwoBytesInFrame.equals(DELIMETER)) {
        throw new ClientError("Missing delimeter")
      }

      val data = buffer.slice(buffer.readerIndex, bytesNeeded)
      buffer.skipBytes(bytesNeeded + DELIMETER.capacity)
      data
    }
  }
}