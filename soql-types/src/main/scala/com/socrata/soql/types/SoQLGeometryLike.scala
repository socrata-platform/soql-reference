package com.socrata.soql.types

import com.vividsolutions.jts.geom.{Geometry, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader
import java.io.StringWriter
import org.apache.commons.io.IOUtils
import org.geotools.geojson.geom.GeometryJSON

trait SoQLGeometryLike[T<:Geometry] {
  object JsonRep {
    def unapply(text: String): Option[T] = {
      try {
        val json = new GeometryJSON
        Some(json.read(IOUtils.toInputStream(text)).asInstanceOf[T])
      }
      catch { case _: IllegalArgumentException => None }
    }

    def apply(geom: T): String = {
      val json = new GeometryJSON
      val sw = new StringWriter
      json.write(geom, sw)
      sw.toString
    }
  }

  object WktRep {
    def unapply(text: String): Option[T] = {
      try {
        val gf = new GeometryFactory
        val reader = new WKTReader(gf)
        Some(reader.read(text).asInstanceOf[T])
      }
      catch { case _: IllegalArgumentException => None }
    }

    def apply(geom: T): String = geom.toString
  }
}