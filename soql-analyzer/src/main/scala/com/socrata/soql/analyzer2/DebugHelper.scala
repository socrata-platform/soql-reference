package com.socrata.soql.analyzer2

import scala.annotation.tailrec

private[analyzer2] object DebugHelper {
  // simple drop-in replacement for log.debug
  def DEBUG(s: String, args: Any*): Unit = {
    val sb = new StringBuilder
    @tailrec
    def loop(s: String, args: List[Any]): List[Any] = {
      val pos = s.indexOf("{}")
      if(pos == -1) {
        sb.append(s)
        args
      } else {
        args match {
          case hd :: tl =>
            sb.append(s.substring(0, pos))
            sb.append(hd)
            loop(s.substring(pos+2), tl)
          case Nil =>
            sb.append(s)
            Nil
        }
      }
    }
    var remaining = loop(s, args.toList)
    if(remaining.nonEmpty) {
      sb.append(": ")
      sb.append(remaining.head)
      remaining = remaining.tail
      while(remaining.nonEmpty) {
        sb.append(", ")
        sb.append(remaining.head)
        remaining = remaining.tail
      }
    }
    println(sb.result())
  }
}
