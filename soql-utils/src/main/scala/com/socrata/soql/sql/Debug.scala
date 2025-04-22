package com.socrata.soql.sql

import com.rojoma.json.v3.ast.{JString, JValue}
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode, DecodeError}
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, AllowMissing}

import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer, Version}

case class Debug(
  sql: Option[Debug.Sql.Format],
  explain: Option[Debug.Explain],
  @AllowMissing("Debug.default.inhibitRun")
  inhibitRun: Boolean,
  @AllowMissing("Debug.default.useCache")
  useCache: Boolean,

  // Temporary hack to enable turning _off_ merging system columns to
  // see what breaks.  This will go away once the experiment is done
  // as part of EN-77404
  @AllowMissing("Debug.default.mergeSystemColumns")
  mergeSystemColumns: Boolean
)

object Debug {
  // An "empty" set of debugging configurations; using this should be
  // equivalent to not requesting debug info at all.
  val default = Debug(
    None,
    None,
    inhibitRun = false,
    useCache = true,
    mergeSystemColumns = true
  )

  object Sql {
    sealed abstract class Format
    object Format {
      case object Compact extends Format
      case object Pretty extends Format

      implicit val jCodec = new JsonEncode[Format] with JsonDecode[Format] {
        private val compact = JString("compact")
        private val pretty = JString("pretty")

        def encode(f: Format) =
          f match {
            case Compact => compact
            case Pretty => pretty
          }

        def decode(v: JValue) =
          v match {
            case `compact` => Right(Compact)
            case `pretty` => Right(Pretty)
            case JString(_) => Left(DecodeError.InvalidValue(v))
            case other => Left(DecodeError.InvalidType(expected = JString, got = other.jsonType))
          }
      }

      implicit val serialize = new Readable[Format] with Writable[Format] {
        def writeTo(buffer: WriteBuffer, t: Format): Unit =
          t match {
            case Compact => buffer.write(0)
            case Pretty => buffer.write(1)
          }

        def readFrom(buffer: ReadBuffer): Format =
          buffer.read[Int]() match {
            case 0 => Compact
            case 1 => Pretty
            case other => fail("Unknown sql format " + other)
          }
      }
    }
  }

  case class Explain(
    @AllowMissing("false")
    analyze: Boolean,
    @AllowMissing("Explain.Format.Text")
    format: Explain.Format
  )
  object Explain {
    sealed abstract class Format
    object Format {
      case object Text extends Format
      case object Json extends Format

      implicit val jCodec = new JsonEncode[Format] with JsonDecode[Format] {
        private val text = JString("text")
        private val json = JString("json")

        def encode(f: Format) =
          f match {
            case Text => text
            case Json => json
          }

        def decode(v: JValue) =
          v match {
            case `text` => Right(Text)
            case `json` => Right(Json)
            case JString(_) => Left(DecodeError.InvalidValue(v))
            case other => Left(DecodeError.InvalidType(expected = JString, got = other.jsonType))
          }
      }

      implicit val serialize = new Readable[Format] with Writable[Format] {
        def writeTo(buffer: WriteBuffer, t: Format): Unit =
          t match {
            case Text => buffer.write(0)
            case Json => buffer.write(1)
          }

        def readFrom(buffer: ReadBuffer): Format =
          buffer.read[Int]() match {
            case 0 => Text
            case 1 => Json
            case other => fail("Unknown explain format " + other)
          }
      }
    }

    // When decoding, this accepts just a format-string as well as the
    // full object form.
    implicit val jCodec = new JsonEncode[Explain] with JsonDecode[Explain] {
      private val auto = AutomaticJsonCodecBuilder[Explain]

      def encode(e: Explain) =
        auto.encode(e)

      def decode(v: JValue) = {
        auto.decode(v) match {
          case Right(r) =>
            Right(r)
          case Left(e1) =>
            JsonDecode.fromJValue[Format](v) match {
              case Right(f) => Right(Explain(analyze = false, format = f))
              case Left(e2) => Left(DecodeError.join(Seq(e1, e2)))
            }
        }
      }
    }

    implicit val serialize = new Readable[Explain] with Writable[Explain] {
      def writeTo(buffer: WriteBuffer, t: Explain): Unit = {
        val Explain(
          analyze,
          format
        ) = t
        buffer.write(analyze)
        buffer.write(format)
      }

      def readFrom(buffer: ReadBuffer): Explain =
        Explain(
          analyze = buffer.read[Boolean](),
          format = buffer.read[Format]()
        )
    }
  }

  implicit val jCodec = AutomaticJsonCodecBuilder[Debug]

  implicit val serialize = new Readable[Debug] with Writable[Debug] {
    def writeTo(buffer: WriteBuffer, t: Debug): Unit = {
      val Debug(
        sql,
        explain,
        inhibitRun,
        useCache,
        mergeSystemColumns
      ) = t

      buffer.write(sql)
      buffer.write(explain)
      buffer.write(inhibitRun)
      buffer.write(useCache)
      buffer.write(mergeSystemColumns)
    }

    def readFrom(buffer: ReadBuffer): Debug =
      buffer.version match {
        case Version.V3 =>
          Debug(
            sql = buffer.read[Option[Sql.Format]](),
            explain = buffer.read[Option[Explain]](),
            inhibitRun = buffer.read[Boolean](),
            useCache = buffer.read[Boolean](),
            mergeSystemColumns = default.mergeSystemColumns,
          )
        case Version.V4 =>
          Debug(
            sql = buffer.read[Option[Sql.Format]](),
            explain = buffer.read[Option[Explain]](),
            inhibitRun = buffer.read[Boolean](),
            useCache = buffer.read[Boolean](),
            mergeSystemColumns = buffer.read[Boolean]()
          )
      }
  }
}
