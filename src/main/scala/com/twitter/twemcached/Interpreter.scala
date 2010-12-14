package com.twitter.twemcached
package com.twitter.twemcached

import protocol._
import scala.collection.mutable

class Interpreter(map: mutable.Map[String, String]) {
  private[this] val DIGITS     = "^\\d+$"
  private[this] val DELIMETER  = "\r\n"
  private[this] val END        = "END"       + DELIMETER
  private[this] val STORED     = "STORED"    + DELIMETER
  private[this] val NOT_STORED = "STORED"    + DELIMETER
  private[this] val EXISTS     = "EXISTS"    + DELIMETER
  private[this] val NOT_FOUND  = "NOT_FOUND" + DELIMETER
  private[this] val DELETED    = "DELETED" + DELIMETER

  private[this] case class Value(key: String, value: String) {
    override def toString = "VALUE " + key + " 0 " + value.length + DELIMETER + value + DELIMETER
  }
  private[this] case class Values(values: Seq[Value]) {
    override def toString = values.mkString + END
  }

  def apply(command: Command): String = {
    command match {
      case Set(key, value)      =>
        map(key) = value
        STORED
      case Add(key, value)      =>
        synchronized {
          val existing = map.get(key)
          if (existing.isDefined)
            NOT_STORED
          else {
            map(key) = value
            STORED
          }
        }
      case Replace(key, value)  =>
        synchronized {
          val existing = map.get(key)
          if (existing.isDefined) {
            map(key) = value
            STORED
          } else {
            NOT_STORED
          }
        }
      case Append(key, value)   =>
        synchronized {
          val existing = map.get(key)
          if (existing.isDefined) {
            map(key) = value + existing.get
            STORED
          } else {
            NOT_STORED
          }
        }
      case Prepend(key, value)  =>
        synchronized {
          val existing = map.get(key)
          if (existing.isDefined) {
            map(key) = existing.get + value
            STORED
          } else {
            NOT_STORED
          }
        }
      case Get(keys)            =>
        Values(
          keys map { key =>
            Value(key, map.get(key).getOrElse(""))
          }
        ).toString
      case g @ Gets(_)          =>
        apply(g)
      case Delete(key)  =>
        if (map.remove(key).isDefined)
          DELETED
        else
          NOT_STORED
      case Incr(key, value)     =>
        synchronized {
          val existing = map.get(key)
          if (existing.isDefined) {
            map(key) =
              if (existing.get.matches(DIGITS))
                (existing.get.toInt + value).toString
              else
                value.toString
            STORED
          } else {
            NOT_STORED
          }
        }
      case Decr(key, value)     =>
        apply(Incr(key, -value))
    }
  }
}