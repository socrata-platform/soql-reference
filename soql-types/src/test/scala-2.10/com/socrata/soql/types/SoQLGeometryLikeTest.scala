package com.socrata.soql.types

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers

class SoQLGeometryLikeTest extends FunSuite with MustMatchers {
  test("Point : WKT & JSON apply/unapply") {
    val json = """{"type":"Point","coordinates":[47.6303123,-122.123456789012]}"""
    val wkt = "POINT (47.6303123 -122.123456789012)"
    val geoms = Seq(SoQLPoint.JsonRep.unapply(json), SoQLPoint.WktRep.unapply(wkt))

    geoms.foreach { geom =>
      geom must not be {
        None
      }
      geom.get.getX must be {
        47.6303123 plusOrMinus 0.5
      }
      geom.get.getY must be {
        -122.123456789012 plusOrMinus 0.5
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
    val json = """{"type":"LineString","coordinates":[[102,0.123456789012],[103,1],[104,0.0],[105,1]]}"""
    val wkt = "LINESTRING (102 0.123456789012, 103 1, 104 0, 105 1)"
    val geoms = Seq(SoQLLine.JsonRep.unapply(json), SoQLLine.WktRep.unapply(wkt))

    geoms.foreach {
      geom =>
        geom must not be {
          None
        }
        val allCoords = geom.get.getCoordinates.flatMap(c => Seq(c.x, c.y))
        allCoords must equal {
          Array(102, 0.123456789012, 103, 1, 104, 0, 105, 1)
        }
    }

    val json2 = SoQLLine.JsonRep(geoms.last.get)
    val wkt2 = SoQLLine.WktRep(geoms.last.get)

    json2 must not be { 'empty }
    json2 must equal { json }
    wkt2 must not be { 'empty }
    wkt2 must equal { wkt }
  }

  test("Polygon : WKT & JSON apply/unapply") {
    val json = """{"type":"Polygon","coordinates":[[[-2,2],[2,2.123456789012],[2,-2],[-2,-2],[-2,2]],[[-1,1],[1,1],[1,-1],[-1,-1],[-1,1]]]}"""
    val wkt = "POLYGON ((-2 2, 2 2.123456789012, 2 -2, -2 -2, -2 2), (-1 1, 1 1, 1 -1, -1 -1, -1 1))"
    val geoms = Seq(SoQLPolygon.JsonRep.unapply(json), SoQLPolygon.WktRep.unapply(wkt))

    geoms.foreach {
      geom =>
        geom must not be {
          None
        }
        val allExteriorRingCoords = geom.get.getExteriorRing().getCoordinates.flatMap(c => Seq(c.x, c.y))
        allExteriorRingCoords must equal {
          Array(-2, 2, 2, 2.123456789012, 2, -2, -2, -2, -2, 2)
        }
        geom.get.getNumInteriorRing() must equal {
          1
        }
        val allInteriorRingCoords = geom.get.getInteriorRingN(0).getCoordinates.flatMap(c => Seq(c.x, c.y))
        allInteriorRingCoords must equal {
          Array(-1, 1, 1, 1, 1, -1, -1, -1, -1, 1)
        }
    }

    val json2 = SoQLPolygon.JsonRep(geoms.last.get)
    val wkt2 = SoQLPolygon.WktRep(geoms.last.get)

    json2 must not be { 'empty }
    json2 must equal { json }
    wkt2 must not be { 'empty }
    wkt2 must equal { wkt }
  }
}
