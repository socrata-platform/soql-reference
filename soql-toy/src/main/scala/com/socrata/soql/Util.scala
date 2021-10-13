package com.socrata.soql

object Util {
  def printList(xs: Iterable[(Any, Any)], leftMargin: String = ""): Unit = {
    if(xs.nonEmpty) {
      val right = String.valueOf(xs.maxBy { kv => String.valueOf(kv._1).length }._1).length + 2
      for((k,v) <- xs) {
        val s = String.valueOf(k)
        println(leftMargin + s + " " + ("." * (right - s.length)) + " " + v)
      }
    }
  }
}
