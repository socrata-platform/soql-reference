package com.socrata.soql.stdlib

import scala.collection.compat._
import scala.collection.immutable.SortedMap

import com.rojoma.json.v3.ast.{JValue, JNumber, JString}
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode, DecodeError}
import com.rojoma.json.v3.util.{AutomaticJsonCodec, JsonKey, AutomaticJsonCodecBuilder, WrapperJsonCodec}
import org.joda.time.{DateTime, LocalDateTime}

import com.socrata.soql.types._

@AutomaticJsonCodec
case class UserContext(
  @JsonKey("t") text: Map[String, SoQLText],
  @JsonKey("b") bool: Map[String, SoQLBoolean],
  @JsonKey("n") num: Map[String, SoQLNumber],
  @JsonKey("f") floating: Map[String, SoQLFloatingTimestamp],
  @JsonKey("F") fixed: Map[String, SoQLFixedTimestamp]
) {
  def canonicalized = UserContext(
    text = SortedMap.from(text),
    bool = SortedMap.from(bool),
    num = SortedMap.from(num),
    floating = SortedMap.from(floating),
    fixed = SortedMap.from(fixed)
  )

  def nonEmpty =
    text.nonEmpty || bool.nonEmpty || num.nonEmpty || floating.nonEmpty || fixed.nonEmpty
}

object UserContext {
  private implicit val soqltextCodec = WrapperJsonCodec[SoQLText](SoQLText(_), _.value)
  private implicit val soqlboolCodec = WrapperJsonCodec[SoQLBoolean](SoQLBoolean(_), _.value)
  private implicit val soqlnumCodec = WrapperJsonCodec[SoQLNumber](SoQLNumber(_), _.value)
  private implicit val soqlfloatCodec = new JsonEncode[SoQLFloatingTimestamp] with JsonDecode[SoQLFloatingTimestamp] {
    def encode(t: SoQLFloatingTimestamp) = JString(SoQLFloatingTimestamp.StringRep(t.value))
    def decode(v: JValue) =
      v match {
        case JString(SoQLFloatingTimestamp.StringRep(t)) => Right(SoQLFloatingTimestamp(t))
        case JString(_) => Left(DecodeError.InvalidValue(v))
        case _ => Left(DecodeError.InvalidType(expected = JString, got = v.jsonType))
      }
  }

  private implicit val soqlfixedCodec = new JsonEncode[SoQLFixedTimestamp] with JsonDecode[SoQLFixedTimestamp] {
    def encode(t: SoQLFixedTimestamp) = JString(SoQLFixedTimestamp.StringRep(t.value))
    def decode(v: JValue) =
      v match {
        case JString(SoQLFixedTimestamp.StringRep(t)) => Right(SoQLFixedTimestamp(t))
        case JString(_) => Left(DecodeError.InvalidValue(v))
        case _ => Left(DecodeError.InvalidType(expected = JString, got = v.jsonType))
      }
  }

  val empty = UserContext(Map.empty, Map.empty, Map.empty, Map.empty, Map.empty)
}

case class Context(
  @JsonKey("s") system: Map[String, String],
  @JsonKey("u") user: UserContext
) {
  def augmentSystemContext(k: String, v: String) = copy(system = system + (k->v))
  def canonicalized = Context(SortedMap.from(system), user.canonicalized)
  def nonEmpty = system.nonEmpty || user.nonEmpty
}

object Context {
  val empty = Context(Map.empty, UserContext.empty)

  // backward compatibility: context used to be just a Map[String,
  // String], so if we receive that, accept it.  Once fully released,
  // this can become a auto-annotation on Context.
  implicit val jCodec = new JsonEncode[Context] with JsonDecode[Context] {
    private val auto = AutomaticJsonCodecBuilder[Context]
    override def encode(cs: Context) = auto.encode(cs)
    override def decode(x: JValue) =
      auto.decode(x) match {
        case Right(cs) => Right(cs)
        case Left(e) =>
          JsonDecode.fromJValue[Map[String, String]](x) match {
            case Right(ok) => Right(Context(system = ok, user = UserContext.empty))
            case Left(_) => Left(e)
          }
      }
  }
}
