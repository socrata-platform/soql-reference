package com.socrata.soql.stdlib

import scala.collection.compat._
import scala.collection.immutable.SortedMap

import com.rojoma.json.v3.ast.{JValue, JNumber, JString}
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode, DecodeError}
import com.rojoma.json.v3.util.{AutomaticJsonCodec, JsonKey, AutomaticJsonCodecBuilder, WrapperJsonCodec}
import org.joda.time.{DateTime, LocalDateTime}

import com.socrata.soql.types._

case class UserContext(
  text: Map[String, SoQLText],
  bool: Map[String, SoQLBoolean],
  num: Map[String, SoQLNumber],
  floating: Map[String, SoQLFloatingTimestamp],
  fixed: Map[String, SoQLFixedTimestamp]
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

  implicit val jCodec = new JsonEncode[UserContext] with JsonDecode[UserContext] {
    @AutomaticJsonCodec
    private case class OptionalizedUserContext(
      @JsonKey("t") text: Option[Map[String, SoQLText]],
      @JsonKey("b") bool: Option[Map[String, SoQLBoolean]],
      @JsonKey("n") num: Option[Map[String, SoQLNumber]],
      @JsonKey("f") floating: Option[Map[String, SoQLFloatingTimestamp]],
      @JsonKey("F") fixed: Option[Map[String, SoQLFixedTimestamp]]
    )

    def encode(uc: UserContext) = {
      val UserContext(text, bool, num, floating, fixed) = uc
      JsonEncode.toJValue(OptionalizedUserContext(
                            text = Some(text).filter(_.nonEmpty),
                            bool = Some(bool).filter(_.nonEmpty),
                            num = Some(num).filter(_.nonEmpty),
                            floating = Some(floating).filter(_.nonEmpty),
                            fixed = Some(fixed).filter(_.nonEmpty)
                          ))
    }

    def decode(x: JValue) =
      JsonDecode.fromJValue[OptionalizedUserContext](x).map { ouc =>
        UserContext(
          text = ouc.text.getOrElse(Map.empty),
          bool = ouc.bool.getOrElse(Map.empty),
          num = ouc.num.getOrElse(Map.empty),
          floating = ouc.floating.getOrElse(Map.empty),
          fixed = ouc.fixed.getOrElse(Map.empty)
        )
      }
  }

  val empty = UserContext(Map.empty, Map.empty, Map.empty, Map.empty, Map.empty)
}

case class Context(
  system: Map[String, String],
  user: UserContext
) {
  def augmentSystemContext(k: String, v: String) = copy(system = system + (k->v))
  def canonicalized = Context(SortedMap.from(system), user.canonicalized)
  def nonEmpty = system.nonEmpty || user.nonEmpty
}

object Context {
  val empty = Context(Map.empty, UserContext.empty)

  implicit val jCodec = new JsonEncode[Context] with JsonDecode[Context] {
    @AutomaticJsonCodec
    private case class OptionalizedContext(
      @JsonKey("s") system: Option[Map[String, String]],
      @JsonKey("u") user: Option[UserContext]
    )

    override def encode(cs: Context) = {
      val Context(system, user) = cs
      JsonEncode.toJValue(OptionalizedContext(
                            system = Some(system).filter(_.nonEmpty),
                            user = Some(user).filter(_.nonEmpty)
                          ))
    }

    override def decode(x: JValue) =
      JsonDecode.fromJValue[OptionalizedContext](x) match {
        case Right(oc) =>
          Right(Context(
                  system = oc.system.getOrElse(Map.empty),
                  user = oc.user.getOrElse(UserContext.empty)
                ))
        case Left(e) =>
          // backward compatibility: context used to be just a Map[String,
          // String], so if we receive that, accept it.  Once fully released,
          // this can go away and decode can become fromJValue(..).map { ... }
          // just like UserContext's
          JsonDecode.fromJValue[Map[String, String]](x) match {
            case Right(ok) => Right(Context(system = ok, user = UserContext.empty))
            case Left(_) => Left(e)
          }
      }
  }
}
