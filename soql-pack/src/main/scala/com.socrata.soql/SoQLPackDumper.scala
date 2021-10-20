package com.socrata.soql

import java.io.{DataInputStream, FileInputStream}
import org.apache.commons.io.input.CountingInputStream
import org.velvia.MsgPack
import org.velvia.MsgPackUtils._
import com.vividsolutions.jts.io.WKBReader

/**
 * App that takes a SoQLPack stream from STDIN and dumps out the header and rows
 *
 * Run in the soql-reference directory like:
 *  curl ... | sbt 'soql-pack/run'
 */
object SoQLPackDumper extends App {
  val cis = new CountingInputStream(System.in)
  val dis = new DataInputStream(cis)
  val headerMap = MsgPack.unpack(dis, MsgPack.UNPACK_RAW_AS_STRING).asInstanceOf[Map[String, Any]]
  println("--- Schema ---")
  headerMap.foreach { case (key, value) => println("%25s: %s".format(key, value)) }
  val types = headerMap.as[Seq[Map[String, String]]]("schema").map { m => m("t") }

  println("\n---")

  val reader = new WKBReader

  while (dis.available > 0) {
    print("[%10d] ".format(cis.getCount()))
    // Don't unpack raw byte arrays as Strings - they might be Geometries or other blobs
    val row = MsgPack.unpack(dis, 0).asInstanceOf[Seq[Any]]
    for (i <- 0 until row.length) {
      if (Set("point", "multiline", "multipolygon") contains types(i)) {
        print(reader.read(row(i).asInstanceOf[Array[Byte]]))
      } else if (types(i) == "text") {
        print(new String(row(i).asInstanceOf[Array[Byte]]))
      } else {
        print(row(i).toString)
      }
      print(", ")
    }
    println()
  }

}
