package com.socrata.soql.types

import com.vividsolutions.jts.geom.Polygon
import javax.xml.bind.DatatypeConverter.parseBase64Binary
import org.scalatest.FunSuite
import org.scalatest.MustMatchers

class SoQLGeometryLikeTest extends FunSuite with MustMatchers {
  test("Point : WKT & JSON apply/unapply") {
    val json = """{"type":"Point","coordinates":[47.123456,-122.123456]}"""
    val wkt = "POINT (47.123456 -122.123456)"
    val geoms = Seq(SoQLPoint.JsonRep.unapply(json), SoQLPoint.WktRep.unapply(wkt))

    geoms.foreach { geom =>
      geom must not be (None)
      geom.get.getX must be {
        47.123456 +- 0.0000005
      }
      geom.get.getY must be {
        -122.123456 +- 0.0000005
      }
    }

    val json2 = SoQLPoint.JsonRep(geoms.last.get)
    val wkt2 = SoQLPoint.WktRep(geoms.last.get)

    json2 must not be { Symbol("empty") }
    json2 must equal { json }
    wkt2 must not be { Symbol("empty") }
    wkt2 must equal { wkt }
  }

  test("Point: WKB & WKB64 apply/unapply") {
    val wkb64 = "AAAAAAHAPgpa8K4hcEBITaQDgJ5U"
    val wkb = parseBase64Binary(wkb64)
    val geoms = Seq(SoQLPoint.Wkb64Rep.unapply(wkb64), SoQLPoint.WkbRep.unapply(wkb))

    geoms.foreach { geom =>
      geom must not be (None)
      geom.get.getX must be {
        -30.04045 +- 0.000001
      }
      geom.get.getY must be {
        48.606567 +- 0.000001
      }
    }

    val wkb64_2 = SoQLPoint.Wkb64Rep(geoms.last.get)
    val wkb2 = SoQLPoint.WkbRep(geoms.last.get)

    wkb64_2 must not have size { 0 }
    wkb64_2 must equal { wkb64 }
    wkb2 must not have size { 0 }
    wkb2 must equal { wkb }
  }

  test("MultiLine : WKT & JSON apply/unapply") {
    val json = """{"type":"MultiLineString","coordinates":[[[100.0,0.123456],[101.0,1.0]],[[102.0,2.0],[103.0,3.0]]]}"""
    val wkt = "MULTILINESTRING ((100 0.123456, 101 1), (102 2, 103 3))"
    val geoms = Seq(SoQLMultiLine.JsonRep.unapply(json), SoQLMultiLine.WktRep.unapply(wkt))

    geoms.foreach {
      geom =>
        geom must not be (None)
        val allCoords = geom.get.getCoordinates.flatMap(c => Seq(c.x, c.y))
        allCoords must equal {
          Array(100, 0.123456, 101, 1, 102, 2, 103, 3)
        }
    }

    val json2 = SoQLMultiLine.JsonRep(geoms.last.get)
    val wkt2 = SoQLMultiLine.WktRep(geoms.last.get)

    json2 must not be { Symbol("empty") }
    json2 must equal { json }
    wkt2 must not be { Symbol("empty") }
    wkt2 must equal { wkt }
  }

  test("MultiPolygon : WKT & JSON apply/unapply") {
    val json =
      """{"type":"MultiPolygon","coordinates":[[[[40.0,40.0],[20.0,45.123456],[45.0,30.0],[40.0,40.0]]],
        |                                      [[[20.0,35.0],[10.0,30.0],[10.0,10.0],[30.0,5.0],[45.0,20.0],[20.0,35.0]],
        |                                       [[30.0,20.0],[20.0,15.0],[20.0,25.0],[30.0,20.0]]]]}""".stripMargin
    val wkt = "MULTIPOLYGON (((40 40, 20 45.123456, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))"
    val geoms = Seq(SoQLMultiPolygon.JsonRep.unapply(json), SoQLMultiPolygon.WktRep.unapply(wkt))

    geoms.foreach {
      geom =>
        geom must not be (None)

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

    json2 must not be { Symbol("empty") }
    json2 must equal { json.replaceAll("""\s""", "") }
    wkt2 must not be { Symbol("empty") }
    wkt2 must equal { wkt }
  }


  test("Polygon : WKT & JSON apply/unapply") {
    val json =
      """{"type" : "Polygon", "coordinates": [[[0.0, 0.0], [0.0, 20.0], [20.0, 20.0], [0.0, 0.0]]]}""".stripMargin
    val wkt = "POLYGON ((0 0, 0 20, 20 20, 0 0))"
    val polys = Seq(SoQLPolygon.JsonRep.unapply(json), SoQLPolygon.WktRep.unapply(wkt))
    polys.foreach {
      poly =>
        poly must not be (None)

        poly.get.getExteriorRing.getCoordinates.flatMap(coords => Seq(coords.x, coords.y)) must equal {
          Array(0, 0, 0, 20, 20, 20, 0, 0)
        }
    }

    val serializedJS = SoQLPolygon.JsonRep(polys.last.get)
    val serializedWKT = SoQLPolygon.WktRep(polys.last.get)

    serializedJS must equal { json.replaceAll("""\s""", "") }
    serializedWKT must equal { wkt }

  }

  test("Multipoint : WKT & JSON apply/unapply") {
    val json =
      """{ "type": "MultiPoint", "coordinates": [ [0.0, 0.0], [0.0, 20.0], [20.0, 20.0]]}""".stripMargin
    val wkt = "MULTIPOINT ((0 0), (0 20), (20 20))"
    val pts = Seq(SoQLMultiPoint.JsonRep.unapply(json), SoQLMultiPoint.WktRep.unapply(wkt))
    pts.foreach {
      pt =>
        pt must not be (None)

        pt.get.getCoordinates.flatMap(c => Seq(c.x, c.y)) must equal {
          Array(0, 0, 0, 20, 20, 20)
        }

    }

    val serializedJS = SoQLMultiPoint.JsonRep(pts.last.get)
    val serializedWKT = SoQLMultiPoint.WktRep(pts.last.get)

    serializedJS must equal { json.replaceAll("""\s""", "") }
    serializedWKT must equal { wkt }

  }

  test("Line : WKT & JSON apply/unapply") {
    val json =
      """{"type" : "LineString", "coordinates": [[0.0, 0.0], [0.0, 20.0], [20.0, 20.0]]}""".stripMargin
    val wkt = "LINESTRING (0 0, 0 20, 20 20)"
    val lines = Seq(SoQLLine.JsonRep.unapply(json), SoQLLine.WktRep.unapply(wkt))
    lines.foreach {
      line =>
        line must not be (None)
        line.get.getCoordinates.flatMap(c => Seq(c.x, c.y)) must equal {
          Array(0, 0, 0, 20, 20, 20)
        }
    }
    val serializedJS = SoQLLine.JsonRep(lines.last.get)
    val serializedWKT = SoQLLine.WktRep(lines.last.get)

    serializedJS must equal { json.replaceAll("""\s""", "") }
    serializedWKT must equal { wkt }

  }



  test("Apply/unapply valid EWKT") {
    val ewkt = "SRID=4326;POINT (123 456)"
    val SoQLPoint.EWktRep(point, srid) = ewkt
    point.getX must be (123)
    point.getY must be (456)
    srid must be (4326)

    SoQLPoint.EWktRep(point, srid) must be (ewkt)
  }

  test("Parse totally invalid EWKT") {
    val ewkt = "the quick brown fox jumped over the lazy dog"
    SoQLPoint.EWktRep.unapply(ewkt) must be (None)
  }

  test("Parse EWKT with invalid SRID") {
    val ewkt = "SRID=wgs84;POINT(123 456)"
    SoQLPoint.EWktRep.unapply(ewkt) must be (None)
  }

  test("Parse EWKT with invalid geometry") {
    val ewkt = "SRID=wgs84;giraffe"
    SoQLPoint.EWktRep.unapply(ewkt) must be (None)
  }

  test("multipolygon round trip") {
    val wkt = "MULTIPOLYGON (((1 1, 2 1, 2 2, 1 2, 1 1)))"
    val geom = SoQLMultiPolygon.WktRep.unapply(wkt).get
    val roundTrip = new String(SoQLMultiPolygon.WktRep.apply(geom))
    roundTrip must be (wkt)
  }

  test("point round trip") {
    val wkt = "POINT (1 2)"
    val geom = SoQLPoint.WktRep.unapply(wkt).get
    val roundTrip = new String(SoQLPoint.WktRep.apply(geom))
    roundTrip must be (wkt)
  }

  test("point is not accepted as multipolygon") {
    SoQLMultiPolygon.WktRep.unapply("POINT (1 2)") must be (None)
  }

  test("multipolygon is not accepted as point") {
    SoQLPoint.WktRep.unapply("MULTIPOLYGON (((1 1, 2 1, 2 2, 1 2, 1 1)))") must be (None)
  }

  test("JSON parser handles non floating point numbers") {
    val json = """{"type":"Point","coordinates":[47,-122]}"""
    val geom = SoQLPoint.JsonRep.unapply(json)

    geom must not be (None)
    geom.get.getX must be (47)
    geom.get.getY must be (-122)

    val json2 = SoQLPoint.JsonRep(geom.get)

    json2 must be ("""{"type":"Point","coordinates":[47.0,-122.0]}""")
  }
}
