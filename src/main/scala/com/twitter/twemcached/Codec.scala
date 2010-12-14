package com.twitter.twemcached

import org.jboss.netty.handler.codec.frame.{Delimiters, DelimiterBasedFrameDecoder}
import org.jboss.netty.handler.codec.oneone.{OneToOneEncoder, OneToOneDecoder}
import org.jboss.netty.channel.{Channel, ChannelHandlerContext, Channels, ChannelPipelineFactory}
import protocol.{Command, ServerError}

class MemcachedCodec(maxFrameLength: Int) {
  class PipelineError extends ServerError("Handler not correctly wired in the pipeline")

  object Decoder extends OneToOneDecoder {
    def decode(context: ChannelHandlerContext, channel: Channel, message: AnyRef): Command = {
      message match {
        case message: String =>
          val command = Command(message)
          command
        case _ =>
          throw new PipelineError
      }
    }
  }

  object Encoder extends OneToOneEncoder {
    def encode(context: ChannelHandlerContext, channel: Channel, message: AnyRef) = {
      message match {
        case command: Command =>
          command.toString
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
          new DelimiterBasedFrameDecoder(maxFrameLength, Delimiters.lineDelimiter: _*))
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