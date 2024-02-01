package com.socrata.soql.types

import java.util.Base64

import com.google.common.collect.{Interner, Interners}
import com.rojoma.json.v3.ast.{JValue, JString, JObject}
import com.rojoma.json.v3.codec.DecodeError
import com.rojoma.json.v3.io.{CompactJsonWriter, JsonReader}
import com.socrata.thirdparty.geojson.JtsCodecs
import com.vividsolutions.jts.geom.{Geometry, GeometryFactory}
import com.vividsolutions.jts.io.{WKBWriter, WKBReader, WKTReader, WKTWriter}

import SoQLGeometryLike._

trait SoQLGeometryLike[T <: Geometry] {
  protected val Treified: Class[T]

  protected trait GeoCJsonRep[C <: SoQLValue] extends CJsonRep[C, SoQLValue] {
    protected def unwrapper(c: C): T
    protected def wrapper(t: T): C

    // Accept any of, in decending order of precedence:
    //   * base64-encoded WKB
    //   * WKT
    //   * GeoJSON
    override protected final def fromJValueImpl(v: JValue): Either[DecodeError, C] =
      v match {
        case JString(Wkb64Rep(p)) => Right(wrapper(p))
        case JString(WktRep(p)) => Right(wrapper(p))
        case s@JString(_) => Left(DecodeError.InvalidValue(s))
        case o: JObject =>
          JtsCodecs.geoCodec.decode(o) match {
            case Right(c) =>
              val geoValue =
                try {
                  Treified.cast(c)
                } catch {
                  case _ : ClassCastException =>
                    return Left(DecodeError.InvalidValue(o))
                }
              Right(wrapper(geoValue))
            case Left(err) => Left(err)
          }
        case other =>
          Left(DecodeError.Multiple(
                 Seq(DecodeError.InvalidType(expected = JString, got = other.jsonType),
                     DecodeError.InvalidType(expected = JObject, got = other.jsonType))))
      }

    override final def toJValue(v: C) =
      JString(Wkb64Rep(unwrapper(v)))
  }

  object JsonRep {
    def unapply(text: String): Option[T] =
      JtsCodecs.geoCodec.decode(JsonReader.fromString(text)).toOption.map(Treified.cast)

    def apply(geom: T): String =
      CompactJsonWriter.toString(JtsCodecs.geoCodec.encode(geom))
  }

  object JValueRep {
    def unapply(v: JValue): Either[DecodeError, T] =
      JtsCodecs.geoCodec.decode(v).flatMap { decoded =>
        try { Right(Treified.cast(decoded)) }
        catch {
          case _: ClassCastException =>
            Left(DecodeError.InvalidValue(v))
        }
      }

    def apply(geom: T): JValue =
      JtsCodecs.geoCodec.encode(geom)
  }

  object WktRep {
    def unapply(text: String): Option[T] = {
      try {
        val geom = interner.intern(wktReader.read(text))
        Some(Treified.cast(geom))
      } catch {
        case _: Exception => None
      }
    }

    def apply(geom: T): String = wktWriter.write(geom)
  }

  object WkbRep {
    def unapply(bytes: Array[Byte]): Option[T] = {
      try {
        val geom = interner.intern(wkbReader.read(bytes))
        Some(Treified.cast(geom))
      } catch {
        case _: Exception => None
      }
    }

    def apply(geom: T): Array[Byte] = {
      wkbWriter.write(geom)
    }
  }

  // Fast Base64 WKB representation (for CJSON transport)
  object Wkb64Rep {
    def unapply(text: String): Option[T] = {
      val bits = try {
        Base64.getDecoder.decode(text)
      } catch {
        case _ : IllegalArgumentException =>
          // Sadness
          return None
      }
      WkbRep.unapply(bits)
    }
    def apply(geom: T): String = Base64.getEncoder.encodeToString(WkbRep.apply(geom))
  }

  object EWktRep {
    private val SRIDPrefix = "SRID="
    private val separator = ";"

    def unapply(text: String): Option[(T, Int)] = {
      if (!text.toUpperCase.startsWith(SRIDPrefix)) return None
      val trimmed = text.replace(SRIDPrefix, "")

      val pieces = trimmed.split(separator)
      if (pieces.size != 2) return None

      val maybeSrid = try {
        Some(pieces(0).toInt)
      } catch {
        case _: Exception => None
      }

      maybeSrid.flatMap { srid =>
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

object SoQLGeometryLike {
  private def threadLocal[T](init: => T) = new java.lang.ThreadLocal[T] {
    override def initialValue(): T = init
  }

  private val gf = threadLocal { new GeometryFactory }
  private val wktReader_ = threadLocal { new WKTReader(gf.get) }
  private val wkbReader_ = threadLocal { new WKBReader(gf.get) }
  private val wktWriter_ = threadLocal { new WKTWriter }
  private val wkbWriter_ = threadLocal { new WKBWriter }

  def wktReader = wktReader_.get
  def wkbReader = wkbReader_.get
  def wktWriter = wktWriter_.get
  def wkbWriter = wkbWriter_.get

  val interner: Interner[Geometry] = Interners.newWeakInterner()
}
