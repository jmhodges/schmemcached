package com.twitter.twemcached.protocol.text

import com.twitter.finagle.builder.Codec
import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}
import com.twitter.util.StorageUnit

class FinagleCodec(maxFrameLength: StorageUnit) extends Codec {
  val serverPipelineFactory = {
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()

        pipeline.addLast("decoder", new server.Decoder)
        pipeline.addLast("encoder", server.Encoder)
        pipeline
      }
    }
  }

  val clientPipelineFactory = {
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()

        pipeline.addLast("decoder", new client.Decoder)
        pipeline.addLast("encoder", client.Encoder)
        pipeline
      }
    }
  }
}