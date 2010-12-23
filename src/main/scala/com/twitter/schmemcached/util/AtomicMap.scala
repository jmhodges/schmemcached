package com.twitter.schmemcached.util

import scala.collection.mutable
import mutable.ArrayBuffer

/**
 * Improve concurrency with fine-grained locking. A hash of synchronized hash
 * tables, keyed on the request key.
 */
class AtomicMap[A, B](maps: Seq[mutable.Map[A, B]]) {
  private[this] val concurrencyLevel = maps.size

  def lock[C](key: A)(f: mutable.Map[A, B] => C) = {
    val map = maps(key.hashCode % concurrencyLevel)
    f.synchronized {
      f(map)
    }
  }
}