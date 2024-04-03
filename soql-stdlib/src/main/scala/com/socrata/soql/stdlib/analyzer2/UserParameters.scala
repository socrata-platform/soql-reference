package com.socrata.soql.stdlib.analyzer2

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode}
import com.rojoma.json.v3.util.{AutomaticJsonCodec, SimpleHierarchyCodecBuilder, InternalTag, NullForNone, AllowMissing}

import com.socrata.soql.analyzer2
import com.socrata.soql.analyzer2.CanonicalName
import com.socrata.soql.environment.HoleName
import com.socrata.soql.types._

object UserParameters {
  val empty = UserParameters(Map.empty, Map.empty)

  sealed abstract class Value {
    private[UserParameters] def toPossibleValue: analyzer2.UserParameters.PossibleValue[SoQLType, SoQLValue]
  }

  object Value {
    private def fromCJson[T <: SoQLValue](rep: CJsonRep[T, SoQLValue]) =
      new JsonEncode[T] with JsonDecode[T] {
        def encode(t: T) = rep.toJValue(t)
        def decode(v: JValue) = rep.fromJValueRequired(v)
      }

    private implicit val textCodec = fromCJson(SoQLText.cjsonRep)
    private implicit val numCodec = fromCJson(SoQLNumber.cjsonRep)
    private implicit val booleanCodec = fromCJson(SoQLBoolean.cjsonRep)
    private implicit val fixedTimestampCodec = fromCJson(SoQLFixedTimestamp.cjsonRep)
    private implicit val floatingTimestampCodec = fromCJson(SoQLFloatingTimestamp.cjsonRep)

    @AutomaticJsonCodec
    case class Text(@NullForNone value: Option[SoQLText]) extends Value {
      private[UserParameters] override def toPossibleValue =
        value match {
          case Some(s) => analyzer2.UserParameters.Value(s)
          case None => analyzer2.UserParameters.Null(SoQLText)
        }
    }
    @AutomaticJsonCodec
    case class Number(@NullForNone value: Option[SoQLNumber]) extends Value {
      private[UserParameters] override def toPossibleValue =
        value match {
          case Some(n) => analyzer2.UserParameters.Value(n)
          case None => analyzer2.UserParameters.Null(SoQLNumber)
        }
    }
    @AutomaticJsonCodec
    case class Bool(@NullForNone value: Option[SoQLBoolean]) extends Value {
      private[UserParameters] override def toPossibleValue =
        value match {
          case Some(b) => analyzer2.UserParameters.Value(b)
          case None => analyzer2.UserParameters.Null(SoQLBoolean)
        }
    }
    @AutomaticJsonCodec
    case class FixedTimestamp(@NullForNone value: Option[SoQLFixedTimestamp]) extends Value {
      private[UserParameters] override def toPossibleValue =
        value match {
          case Some(ts) => analyzer2.UserParameters.Value(ts)
          case None => analyzer2.UserParameters.Null(SoQLFixedTimestamp)
        }
    }
    @AutomaticJsonCodec
    case class FloatingTimestamp(@NullForNone value: Option[SoQLFloatingTimestamp]) extends Value {
      private[UserParameters] override def toPossibleValue =
        value match {
          case Some(ts) => analyzer2.UserParameters.Value(ts)
          case None => analyzer2.UserParameters.Null(SoQLFloatingTimestamp)
        }
    }

    implicit val jCodec = SimpleHierarchyCodecBuilder[Value](InternalTag("type"))
      .branch[Text]("text")
      .branch[Number]("number")
      .branch[Bool]("boolean")
      .branch[FixedTimestamp]("fixed_timestamp")
      .branch[FloatingTimestamp]("floating_timestamp")
      .build
  }
}

@AutomaticJsonCodec
case class UserParameters(
  @AllowMissing("Map.empty")
  qualified: Map[CanonicalName, Map[HoleName, UserParameters.Value]],
  @AllowMissing("Map.empty")
  unqualified: Map[HoleName, UserParameters.Value]
) {
  def toUserParameters: analyzer2.UserParameters[SoQLType, SoQLValue] = {
    analyzer2.UserParameters(
      qualified.iterator.map { case (cn, m) =>
        cn -> m.iterator.map { case (hn, v) => hn -> v.toPossibleValue }.toMap
      }.toMap,
      unqualified.iterator.map { case (hn, v) => hn -> v.toPossibleValue }.toMap
    )
  }
}
