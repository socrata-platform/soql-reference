package com.socrata.soql.types

import com.vividsolutions.jts.geom.{Geometry, GeometryFactory}
import com.vividsolutions.jts.io.{WKBWriter, WKBReader, WKTReader}
import java.io.StringWriter
import org.apache.commons.io.IOUtils
import org.geotools.geojson.geom.GeometryJSON
import scala.util.Try

trait SoQLGeometryLike[T <: Geometry] {
  final val GEO_PRECISION = 6

  object JsonRep {
    def unapply(text: String): Option[T] = {
      val json = new GeometryJSON
      Try(json.read(IOUtils.toInputStream(text)).asInstanceOf[T]).toOption
    }

    def apply(geom: T): String = {
      val json = new GeometryJSON(GEO_PRECISION)
      val sw = new StringWriter
      json.write(geom, sw)
      sw.toString
    }
  }

  object WktRep {
    def unapply(text: String): Option[T] = {
      val gf = new GeometryFactory
      val reader = new WKTReader(gf)
      Try(reader.read(text).asInstanceOf[T]).toOption
    }

    def apply(geom: T): String = geom.toString
  }

  object WkbRep {
    def unapply(bytes: Array[Byte]): Option[T] = {
      val gf = new GeometryFactory
      val reader = new WKBReader(gf)
      Try(reader.read(bytes).asInstanceOf[T]).toOption
    }

    def apply(geom: T): Array[Byte] = {
      val writer = new WKBWriter
      writer.write(geom)
    }
  }
}