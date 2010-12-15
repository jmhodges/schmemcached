package com.twitter.twemcached

import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import com.twitter.finagle.builder.Codec
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.frame.FrameDecoder
import protocol._
import com.twitter.util.StateMachine
import org.jboss.netty.buffer.{ChannelBufferIndexFinder, ChannelBuffer, ChannelBuffers}
import text.{Parse, Show}

class MemcachedCodec(maxFrameLength: Int) extends Codec {
  class PipelineError extends ServerError("Handler not correctly wired in the pipeline")

  private[this] val DELIMETER = ChannelBuffers.wrappedBuffer("\r\n".getBytes)

  object Encoder extends OneToOneEncoder {
    def encode(context: ChannelHandlerContext, channel: Channel, message: AnyRef) = {
      message match {
        case response: Response => Show(response)
      }
    }
  }

  class Decoder extends SimpleChannelUpstreamHandler with StateMachine {
    case class AwaitingCommand() extends State
    case class AwaitingData(tokens: Seq[String]) extends State

    var pipeline: ChannelPipeline = _

    override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      pipeline = ctx.getPipeline
      awaitCommand()
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      e.getCause.printStackTrace()
      awaitCommand()
    }

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      val data = e.getMessage.asInstanceOf[ChannelBuffer]

      state match {
        case AwaitingCommand() =>
          val tokens = Parse.tokenize(data)
          val bytesNeeded = Parse.needsData(tokens)
          if (bytesNeeded.isDefined) {
            awaitData(tokens, bytesNeeded.get)
          } else {
            Channels.fireMessageReceived(ctx, Parse.parse(tokens))
          }
        case AwaitingData(tokens) =>
          Channels.fireMessageReceived(ctx, Parse(tokens, data))
          pipeline.remove("decodeData")
          awaitCommand()
      }
    }

    private[this] def awaitData(tokens: Seq[String], bytesNeeded: Int) {
      state = AwaitingData(tokens)
      pipeline.remove("decodeCommand")
      pipeline.addBefore("decoder", "decodeData", new DecodeData(bytesNeeded, this))
    }

    private[this] def awaitCommand() {
      state = AwaitingCommand()
      pipeline.addBefore("decoder", "decodeCommand", new DecodeCommand(this))
    }
  }

  class DecodeCommand(decoder: Decoder) extends FrameDecoder {
    def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): ChannelBuffer = {
      val frameLength = buffer.bytesBefore(ChannelBufferIndexFinder.CRLF)
      if (frameLength < 0) return null

      val frame = buffer.slice(buffer.readerIndex, frameLength)
      buffer.skipBytes(frameLength + DELIMETER.capacity)
      frame
    }
  }

  class DecodeData(bytesNeeded: Int, decoder: Decoder) extends FrameDecoder {
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

  val serverPipelineFactory = {
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()

        pipeline.addLast("decoder", new Decoder)
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