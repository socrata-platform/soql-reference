package com.socrata.soql.types

import com.vividsolutions.jts.geom.Polygon
import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers

class SoQLGeometryLikeTest extends FunSuite with MustMatchers {
  test("Point : WKT & JSON apply/unapply") {
    val json = """{"type":"Point","coordinates":[47.123456,-122.123456]}"""
    val wkt = "POINT (47.6303123 -122.123456789012)"
    val geoms = Seq(SoQLPoint.JsonRep.unapply(json), SoQLPoint.WktRep.unapply(wkt))

    geoms.foreach { geom =>
      geom must not be {
        None
      }
      geom.get.getX must be {
        47.123456 plusOrMinus 0.5
      }
      geom.get.getY must be {
        -122.123456 plusOrMinus 0.5
      }
    }

    val json2 = SoQLPoint.JsonRep(geoms.last.get)
    val wkt2 = SoQLPoint.WktRep(geoms.last.get)

    json2 must not be { 'empty }
    json2 must equal { json }
    wkt2 must not be { 'empty }
    wkt2 must equal { wkt }
  }

  test("Line : WKT & JSON apply/unapply") {
    val json = """{"type":"MultiLineString","coordinates":[[[100,0.123456],[101,1]],[[102,2],[103,3]]]}"""
    val wkt = "MULTILINESTRING ((100 0.123456, 101 1), (102 2, 103 3))"
    val geoms = Seq(SoQLMultiLine.JsonRep.unapply(json), SoQLMultiLine.WktRep.unapply(wkt))

    geoms.foreach {
      geom =>
        geom must not be {
          None
        }
        val allCoords = geom.get.getCoordinates.flatMap(c => Seq(c.x, c.y))
        allCoords must equal {
          Array(100, 0.123456, 101, 1, 102, 2, 103, 3)
        }
    }

    val json2 = SoQLMultiLine.JsonRep(geoms.last.get)
    val wkt2 = SoQLMultiLine.WktRep(geoms.last.get)

    json2 must not be { 'empty }
    json2 must equal { json }
    wkt2 must not be { 'empty }
    wkt2 must equal { wkt }
  }

  test("Polygon : WKT & JSON apply/unapply") {
    val json = """{"type":"MultiPolygon","coordinates":[[[[40,40],[20,45.123456],[45,30],[40,40]]],[[[20,35],[10,30],[10,10],[30,5],[45,20],[20,35]],[[30,20],[20,15],[20,25],[30,20]]]]}"""
    val wkt = "MULTIPOLYGON (((40 40, 20 45.123456, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))"
    val geoms = Seq(SoQLMultiPolygon.JsonRep.unapply(json), SoQLMultiPolygon.WktRep.unapply(wkt))

    geoms.foreach {
      geom =>
        geom must not be {
          None
        }

        val polygon1 = geom.get.getGeometryN(0).asInstanceOf[Polygon]
        polygon1.getExteriorRing.getCoordinates.flatMap(c => Seq(c.x, c.y)) must equal {
          Array(40, 40, 20, 45.123456, 45, 30, 40, 40)
        }

        val polygon2 = geom.get.getGeometryN(1).asInstanceOf[Polygon]
        polygon2.getExteriorRing.getCoordinates.flatMap(c => Seq(c.x, c.y)) must equal {
          Array(20, 35, 10, 30, 10, 10, 30, 5, 45, 20, 20, 35)
        }
        polygon2.getNumInteriorRing must equal {
          1
        }
        polygon2.getInteriorRingN(0).getCoordinates.flatMap(c => Seq(c.x, c.y)) must equal {
          Array(30, 20, 20, 15, 20, 25, 30, 20)
        }
    }

    val json2 = SoQLMultiPolygon.JsonRep(geoms.last.get)
    val wkt2 = SoQLMultiPolygon.WktRep(geoms.last.get)

    json2 must not be { 'empty }
    json2 must equal { json }
    wkt2 must not be { 'empty }
    wkt2 must equal { wkt }
  }
}
