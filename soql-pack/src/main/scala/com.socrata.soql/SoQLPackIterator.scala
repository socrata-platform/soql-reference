package com.socrata.soql

import java.io.DataInputStream

import com.socrata.soql.environment.TypeName
import com.socrata.soql.types._
import com.vividsolutions.jts.io.WKBReader
import org.slf4j.LoggerFactory
import org.velvia.{MsgPack, InvalidMsgPackDataException}
import org.velvia.MsgPackUtils._

/**
 * An Iterator that streams in rows of SoQLValues from a SoQLPack binary stream.
 * Note that there is no good way to detect the end of a stream, other than to try reading from
 * it in hasNext and cache the results.....
 */
class SoQLPackIterator(dis: DataInputStream) extends Iterator[Array[SoQLValue]] {
  private val logger = LoggerFactory.getLogger(getClass)
  private val reader = new WKBReader

  var nextRow: Option[Seq[Any]] = None
  var rowNum = 0

  val headers = MsgPack.unpack(dis, MsgPack.UNPACK_RAW_AS_STRING).asInstanceOf[Map[String, Any]]
  logger.debug("Unpacked SoQLPack headers: " + headers)

  lazy val geomName = colNames(geomIndex)
  val geomIndex = headers.asInt("geometry_index")
  lazy val colNames = schema.map(_._1).toArray

  // Schema is a Seq of (ColumnName, SoQLType)
  val schema = headers("schema").asInstanceOf[Seq[Map[String, String]]].map { colInfo =>
    colInfo("c") -> SoQLType.typesByName(TypeName(colInfo("t")))
  }

  private val decoders = schema.map { case (name, typ) => SoQLPackDecoder.decoderByType(typ) }

  // Only allow nextRow to be filled once
  // NOTE: if IOException is raised during this, make sure the stream hasn't been closed prior
  // to reading from it.
  def hasNext: Boolean = {
    // $COVERAGE-OFF$ For some reason scoverage can't handle this line.
    if (nextRow.isEmpty) {
      // $COVERAGE-ON$
      try {
        logger.trace("Unpacking row {}", rowNum)
        nextRow = Some(MsgPack.unpack(dis, 0).asInstanceOf[Seq[Any]])
        rowNum += 1
      } catch {
        case e: InvalidMsgPackDataException =>
          logger.debug("Probably reached end of data at rowNum {}, got {}", rowNum, e.getMessage)
          nextRow = None
        case e: ClassCastException =>
          logger.debug("Corrupt data at rowNum {}, got {}", rowNum, e.getMessage)
          throw new InvalidMsgPackDataException("Unable to unpack stream, corrupt data.")
        // $COVERAGE-OFF$
        // This case should ideally never happen, and is caught at a higher level.
        case e: Exception =>
          logger.error("Unexpected exception at rowNum {}", rowNum, e)
          throw e
          // $COVERAGE-ON$
      }
    }
    nextRow.isDefined
  }

  def next(): Array[SoQLValue] = {
    val row = nextRow.get.toArray
    nextRow = None    // MUST reset to avoid an endless loop
    var i = 0
    row.map { item =>
      val soql = Option(item).flatMap(decoders(i)).getOrElse(SoQLNull)
      i += 1
      soql
    }
  }
}
