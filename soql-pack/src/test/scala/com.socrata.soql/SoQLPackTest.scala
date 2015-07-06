package com.socrata.soql

import com.socrata.soql.types._
import com.vividsolutions.jts.geom.Polygon
import java.math.{BigDecimal => BD}
import org.joda.time.DateTime
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

  val schema2 = Seq("num" -> SoQLNumber,
                    "$" -> SoQLMoney,
                    "dbl" -> SoQLDouble)

  val data2: Seq[Array[SoQLValue]] = Seq(
    Array(SoQLNumber(new BD(12345678901L)), SoQLMoney(new BD(9999)), SoQLDouble(0.1)),
    Array(SoQLNumber(new BD(-123.456)),     SoQLMoney(new BD(9.99)), SoQLDouble(-99.0))
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

    writeThenRead { os =>
      val w = new SoQLPackWriter(schema2)
      w.write(os, data2.toIterator)
    } { dis =>
      val r = new SoQLPackIterator(dis)
      r.geomIndex must equal (-1)
      r.schema must equal (schema2)
      val outRows = r.toList
      outRows must have length (data2.length)
      outRows(0) must equal (data2(0))
      outRows(1) must equal (data2(1))
    }
  }

  val dt1 = DateTime.parse("2015-03-22T12Z")
  val dt2 = DateTime.parse("2015-03-22T12:00:00-08:00")

  val schemaDT = Seq("dt" -> SoQLFixedTimestamp)

  val dataDT: Seq[Array[SoQLValue]] = Seq(
    Array(SoQLFixedTimestamp(dt1)),
    Array(SoQLFixedTimestamp(dt2))
  )

  test("Can serialize SoQL rows with date time types to and from SoQLPack") {
    writeThenRead { os =>
      val w = new SoQLPackWriter(schemaDT)
      w.write(os, dataDT.toIterator)
    } { dis =>
      val r = new SoQLPackIterator(dis)
      r.geomIndex must equal (-1)
      r.schema must equal (schemaDT)
      val outRows = r.toList
      outRows must have length (dataDT.length)
      outRows(0) must equal (dataDT(0))
      outRows(1) must equal (dataDT(1))
    }
  }
}