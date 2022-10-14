package com.socrata.soql.analyzer2.serialization

import scala.collection.{mutable => scm}

import com.google.protobuf.{CodedOutputStream, CodedInputStream}

class StringDictionary private (indexes: scm.Map[String, Int], strings: scm.ArrayBuffer[String]) {
  def this() = this(new scm.HashMap[String, Int], new scm.ArrayBuffer[String])

  def add(s: String): Int = {
    val n = strings.length
    val i = indexes.getOrElseUpdate(s, n)
    if(i == n) {
      strings += s
    }
    i
  }

  def apply(n: Int): String =
    strings(n)

  def writeTo(stream: CodedOutputStream): Unit = {
    stream.writeUInt32NoTag(strings.length)
    for(string <- strings) {
      stream.writeStringNoTag(string)
    }
  }
}

object StringDictionary {
  def readFrom(stream: CodedInputStream): StringDictionary = {
    val indexes = new scm.HashMap[String, Int]
    val strings = new scm.ArrayBuffer[String]
    var n = stream.readUInt32()
    while(n != 0) {
      n -= 1
      val string = stream.readString()
      indexes.put(string, strings.length)
      strings += string
    }
    new StringDictionary(indexes, strings)
  }
}
