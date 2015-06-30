package com.socrata.soql

import com.socrata.soql.types._
import com.vividsolutions.jts.geom.Polygon
import org.scalatest.{FunSuite, MustMatchers}

class SoQLPackTest extends FunSuite with MustMatchers {
  val wkt1 = "POINT (47.123456 -122.123456)"
  val point1 = SoQLPoint.WktRep.unapply(wkt1).get

  val schema1 = Seq("id" -> SoQLID,
                    "ver" -> SoQLVersion,
                    "str" -> SoQLText,
                    "bool" -> SoQLBoolean,
                    "point" -> SoQLPoint)

  val data1: Seq[Array[SoQLValue]] = Seq(
    Array(SoQLID(1L), SoQLVersion(11L), SoQLText("first"),  SoQLBoolean(true),  SoQLPoint(point1)),
    Array(SoQLID(2L), SoQLVersion(12L), SoQLText("second"), SoQLBoolean(false), SoQLNull)
  )

  def writeThenRead(writer: java.io.OutputStream => Unit)(reader: java.io.DataInputStream => Unit) {
    val baos = new java.io.ByteArrayOutputStream
    try {
      writer(baos)
      val bais = new java.io.ByteArrayInputStream(baos.toByteArray)
      val dis = new java.io.DataInputStream(bais)
      try {
        reader(dis)
      } finally {
        dis.close
        bais.close
      }
    } finally {
      baos.close
    }
  }

  test("Can serialize SoQL rows to and from SoQLPack") {
    writeThenRead { os =>
      val w = new SoQLPackWriter(schema1)
      w.write(os, data1.toIterator)
    } { dis =>
      val r = new SoQLPackIterator(dis)
      r.geomIndex must equal (4)
      r.schema must equal (schema1)
      val outRows = r.toList
      outRows must have length (data1.length)
      outRows(0) must equal (data1(0))
      outRows(1) must equal (data1(1))
    }
  }
}