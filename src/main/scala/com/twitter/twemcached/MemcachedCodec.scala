package com.twitter.twemcached

import org.jboss.netty.handler.codec.oneone.{OneToOneEncoder, OneToOneDecoder}
import com.twitter.finagle.builder.Codec
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.util.CharsetUtil
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.frame.{FixedLengthFrameDecoder, FrameDecoder, Delimiters, DelimiterBasedFrameDecoder}
import protocol._

class MemcachedCodec(maxFrameLength: Int) extends Codec {
  class PipelineError extends ServerError("Handler not correctly wired in the pipeline")

  private[this] val DELIMETER = ChannelBuffers.wrappedBuffer("\r\n".getBytes)

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

  class Decoder extends FrameDecoder {
    override def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): Command = {
      val message = super.decode(ctx, channel, buffer).asInstanceOf[ChannelBuffer]
      if (message ne null) {
        val string = message.toString(CharsetUtil.US_ASCII)
        val tokens = string.split(" ")
        val args = tokens.drop(1)
        val commandName = tokens.head
        println(commandName)
        val makeCommand = Command(commandName)(_)

        if (Command.isStorageCommand(commandName)) {
          val length = args(3).toInt
          ctx.getPipeline.addAfter("decode", "extractData", new FixedLengthFrameDecoder(length))
          ctx.getPipeline.remove("decode")
          ctx.getPipeline.addAfter("extractData", "reset", new SimpleChannelUpstreamHandler {
            override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
              ctx.getPipeline.addBefore("extractData", "decode", Decoder.this)
              ctx.getPipeline.remove("extractData")
              ctx.getPipeline.remove(this)
              val key = tokens(1)
              val data = e.getMessage.asInstanceOf[ChannelBuffer].toString(CharsetUtil.UTF_8)
              val command = makeCommand(Seq(key, data))
              Channels.fireMessageReceived(ctx, command)
            }
          })
        } else {
          Channels.fireMessageReceived(ctx, makeCommand(args))
        }
      }
    }
  }

  val serverPipelineFactory = {
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()

        pipeline.addLast("decode", new Decoder)
        pipeline.addLast("encoder", Encoder)
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