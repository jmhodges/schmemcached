package com.twitter.schmemcached.protocol.text

import com.twitter.finagle.builder.Codec
import org.jboss.netty.channel._

object Memcached extends Codec {
  val serverPipelineFactory = {
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()

        pipeline.addLast("encoder", server.Encoder)
        pipeline.addLast("decoder", new server.Decoder)
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