package com.twitter.twemcached

import org.jboss.netty.handler.codec.frame.{Delimiters, DelimiterBasedFrameDecoder}
import org.jboss.netty.handler.codec.oneone.{OneToOneEncoder, OneToOneDecoder}
import org.jboss.netty.channel.{Channel, ChannelHandlerContext, Channels, ChannelPipelineFactory}
import protocol.{Command, ServerError}
import com.twitter.finagle.builder.Codec
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.util.CharsetUtil

class MemcachedCodec(maxFrameLength: Int) extends Codec {
  class PipelineError extends ServerError("Handler not correctly wired in the pipeline")

  private[this] val DELIMETER = ChannelBuffers.wrappedBuffer("\r\n".getBytes)

  object Decoder extends OneToOneDecoder {
    def decode(context: ChannelHandlerContext, channel: Channel, message: AnyRef): Command = {
      message match {
        case message: ChannelBuffer =>
          val command = Command(message.toString(CharsetUtil.US_ASCII))
          command
        case _ =>
          throw new PipelineError
      }
    }
  }

  object Encoder extends OneToOneEncoder {
    def encode(context: ChannelHandlerContext, channel: Channel, message: AnyRef) = {
      message match {
        case string: String =>
          ChannelBuffers.wrappedBuffer(string.getBytes)
        case _ =>
          new PipelineError
      }
    }
  }

  val serverPipelineFactory = {
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()

        pipeline.addLast("frame",
          new DelimiterBasedFrameDecoder(maxFrameLength, DELIMETER))
        pipeline.addLast("encoder", Encoder)
        pipeline.addLast("decode", Decoder)
        pipeline
      }
    }
  }

  val clientPipelineFactory = {
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()

        pipeline.addLast("encoder", Encoder)
        pipeline
      }
    }
  }
}