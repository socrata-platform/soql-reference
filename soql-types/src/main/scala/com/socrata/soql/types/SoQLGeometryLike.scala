package com.socrata.soql.types

import com.rojoma.json.v3.io.{CompactJsonWriter, JsonReader}
import com.socrata.thirdparty.geojson.JtsCodecs
import com.vividsolutions.jts.geom.{Geometry, GeometryFactory}
import com.vividsolutions.jts.io.{WKBWriter, WKBReader, WKTReader}
import javax.xml.bind.DatatypeConverter.{parseBase64Binary, printBase64Binary}
import scala.util.Try

trait SoQLGeometryLike[T <: Geometry] {
  protected val Treified: Class[T]

  private def threadLocal[T](init: => T) = new java.lang.ThreadLocal[T] {
    override def initialValue(): T = init
  }

  object JsonRep {
    def unapply(text: String): Option[T] =
      JtsCodecs.geoCodec.decode(JsonReader.fromString(text)).right.toOption.map(Treified.cast)

    def apply(geom: T): String =
      CompactJsonWriter.toString(JtsCodecs.geoCodec.encode(geom))
  }

  object WktRep {
    val gf = threadLocal { new GeometryFactory }
    val reader = threadLocal { new WKTReader(gf.get) }

    def unapply(text: String): Option[T] = {
      Try(Treified.cast(reader.get.read(text))).toOption
    }

    def apply(geom: T): String = geom.toString
  }

  object WkbRep {
    val gf = threadLocal { new GeometryFactory }
    val reader = threadLocal { new WKBReader(gf.get) }

    def unapply(bytes: Array[Byte]): Option[T] = {
      Try(Treified.cast(reader.get.read(bytes))).toOption
    }

    def apply(geom: T): Array[Byte] = {
      val writer = new WKBWriter
      writer.write(geom)
    }
  }

  // Fast Base64 WKB representation (for CJSON transport)
  object Wkb64Rep {
    def unapply(text: String): Option[T] = WkbRep.unapply(parseBase64Binary(text))
    def apply(geom: T): String           = printBase64Binary(WkbRep.apply(geom))
  }

  object EWktRep {
    private val SRIDPrefix = "SRID="
    private val separator = ";"

    def unapply(text: String): Option[(T, Int)] = {
      if (!text.toUpperCase.startsWith(SRIDPrefix)) return None
      val trimmed = text.replace(SRIDPrefix, "")

      val pieces = trimmed.split(separator)
      if (pieces.size != 2) return None

      Try(pieces(0).toInt).toOption.flatMap { srid =>
        WktRep.unapply(pieces(1)).map { geom =>
          (geom, srid)
        }
      }
    }

    def apply(geom: T, srid: Int): String = {
      s"$SRIDPrefix$srid$separator${WktRep(geom)}"
    }
  }
}
