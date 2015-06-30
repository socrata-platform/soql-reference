package com.socrata.soql

import com.rojoma.simplearm.util._
import com.socrata.soql.types._
import com.vividsolutions.jts.io.WKBWriter
import java.io.{DataOutputStream, OutputStream}
import org.velvia.MsgPack

/**
 * Writes a stream of SoQL values in SoQLPack format - an efficient, MessagePack-based SoQL transport medium.
 *   - Much more efficient than CJSON, GeoJSON, etc... especially for geometries
 *   - Designed to be very streaming friendly
 *   - MessagePack format means easier to implement clients in any language
 *
 * The structure of the content is pretty much identical to CJSON,
 * but framed as follows:
 *   +0000  P bytes - CJSON-like header, MessagePack object/map, currently with the following entries:
 *                        "row_count" -> integer count of rows
 *                        "geometry_index" -> index within row of geometry shape
 *                        "schema" -> MsgPack equiv of [{"c": fieldName1, "t": fieldType1}, ...]
 *   +P     R bytes - first row - MessagePack array
 *                        geometries are WKB binary blobs
 *                        Text fields are strings
 *                        Number fields are numbers
 *                        All other fields... TBD
 *   +P+R           - second row
 *   All other rows are encoded as MessagePack arrays and follow sequentially.
 *
 *   NOTE: Unlike CJSON, the columns are not rearranged in alphabetical field order, but the original
 *   order in the Array[SoQLValue] and CSchema are retained.
 *
 * @param schema a Seq of tuples of (ColumnName, SoQLType)
 * @oaran extraHeaders a map of extra header info to pass along
 */
class SoQLPackWriter(schema: Seq[(String, SoQLType)],
                     extraHeaders: Map[String, Any] = Map.empty) {

  val geoColumn = getGeoColumnIndex(schema.map(_._2))

  private def getGeoColumnIndex(columns: Seq[SoQLType]): Int = {
    val geoColumnIndices = columns.zipWithIndex.collect {
      case (typ, index) if typ.isInstanceOf[SoQLGeometryLike[_]] => index
    }

    // Just return -1 if no geo column or more than one
    if (geoColumnIndices.size != 1) return -1

    geoColumnIndices(0)
  }

  /**
   * Serializes the rows into SoQLPack binary format, writing it out to the ostream.
   * The caller must be responsible for closing the output stream.
=   */
  def write(ostream: OutputStream, rows: Iterator[Array[SoQLValue]]) {
    for {
      dos <- managed(new DataOutputStream(ostream))
    } yield {
      val schemaMaps = schema.map { case (name, typ) =>
        Map("c" -> name, "t" -> typ.toString)
      }.toSeq

      val headerMap: Map[String, Any] = Map("schema" -> schemaMaps,
                                            "geometry_index" -> geoColumn) ++ extraHeaders
      MsgPack.pack(headerMap, dos)

      // end of header

      val wkbWriter = new WKBWriter

      for (row <- rows) {
        val values: Seq[Any] = (0 until row.length).map { i =>
          // TODO: turn this logic into a MessagePackColumnRep
          row(i) match {
            case SoQLPoint(pt)       => wkbWriter.write(pt)
            case SoQLMultiLine(ml)   => wkbWriter.write(ml)
            case SoQLMultiPolygon(p) => wkbWriter.write(p)
            case SoQLPolygon(p)      => wkbWriter.write(p)
            case SoQLLine(l)         => wkbWriter.write(l)
            case SoQLMultiPoint(mp)  => wkbWriter.write(mp)
            case SoQLText(str)       => str
            case SoQLNull            => null
            case null                => null
            case SoQLBoolean(bool)   => bool
            // For anything else, we rely on the JsonColumnRep and translate from JValues
            case other: SoQLValue    => ???
          }
        }
        MsgPack.pack(values, dos)
      }
      dos.flush()
    }
  }
}