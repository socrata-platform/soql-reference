package com.socrata.soql.exceptions

import scala.util.parsing.input.Position
import scala.reflect.ClassTag

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.util.{SimpleHierarchyCodecBuilder, InternalTag, AutomaticJsonCodecBuilder}
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode, DecodeError}
import com.rojoma.json.v3.matcher._

import com.socrata.soql.environment.{TypeName, FunctionName, ColumnName}
import com.socrata.soql.parsing.SoQLPosition

sealed abstract class SoQLException(m: String, p: Position) extends RuntimeException(m + ":\n" + p.longString) {
  def position: Position
}

object SoQLException {
  private def nameDecode[T](v: JValue, f: String => T) =
    v match {
      case JString(s) => Right(f(s))
      case JArray(Seq(JString(_), JString(s))) => Right(f(s))
      case otherArray : JArray => Left(DecodeError.InvalidValue(otherArray))
      case other => Left(DecodeError.join(Seq(
                                            DecodeError.InvalidType(got = other.jsonType, expected = JString),
                                            DecodeError.InvalidType(got = other.jsonType, expected = JArray))))
    }

  private implicit object ColumnNameCodec extends JsonEncode[ColumnName] with JsonDecode[ColumnName] {
    def encode(v: ColumnName) = JsonEncode.toJValue(v.caseFolded, v.name)
    def decode(v: JValue) = nameDecode(v, ColumnName)
  }
  private implicit object FunctionNameCodec extends JsonEncode[FunctionName] with JsonDecode[FunctionName] {
    def encode(v: FunctionName) = JsonEncode.toJValue(v.caseFolded, v.name)
    def decode(v: JValue) = nameDecode(v, FunctionName)
  }
  private implicit object TypeNameCodec extends JsonEncode[TypeName] with JsonDecode[TypeName] {
    def encode(v: TypeName) = JsonEncode.toJValue(v.caseFolded, v.name)
    def decode(v: JValue) = nameDecode(v, TypeName)
  }
  private implicit object PositionCodec extends JsonEncode[Position] with JsonDecode[Position] {
    private val row = Variable[Int]
    private val col = Variable[Int]
    private val text = Variable[String]
    private val pattern =
      PObject(
        "row" -> row,
        "column" -> col,
        "text" -> text
      )

    def encode(p: Position) =
      pattern.generate(row := p.line, col := p.column, text := p.longString.split('\n')(0))

    def decode(v: JValue) =
      pattern.matches(v).right.map { results =>
        SoQLPosition(row(results), col(results), text(results), col(results))
      }
  }

  private implicit object CharCodec extends JsonEncode[Char] with JsonDecode[Char] {
    def encode(c: Char) = JString(c.toString)
    def decode(v: JValue) = v match {
      case JString(s) if s.length == 1 => Right(s.charAt(0))
      case other: JString => Left(DecodeError.InvalidValue(other))
      case other => Left(DecodeError.InvalidType(expected = JString, got = other.jsonType))
    }
  }

  private implicit class AugCodec(shc: SimpleHierarchyCodecBuilder[SoQLException]) {
    def and[T <: SoQLException : ClassTag](tag: String, codec: JsonEncode[T] with JsonDecode[T]) =
      shc.branch[T](tag)(codec, codec, implicitly)
  }

  implicit val jCodec = new JsonEncode[SoQLException] with JsonDecode[SoQLException] {
    // ugggh
    private val rawCodec = SimpleHierarchyCodecBuilder[SoQLException](InternalTag("type")).
      // AggregateCheckException
      and("aggregate-in-ungrouped-context", AutomaticJsonCodecBuilder[AggregateInUngroupedContext]).
      and("column-not-in-group-bys", AutomaticJsonCodecBuilder[ColumnNotInGroupBys]).
      // AliasAnalysisException
      and("repeated-exclusion", AutomaticJsonCodecBuilder[RepeatedException]).
      and("duplicate-alias", AutomaticJsonCodecBuilder[DuplicateAlias]).
      and("no-such-column", AutomaticJsonCodecBuilder[NoSuchColumn]).
      and("no-such-table", AutomaticJsonCodecBuilder[NoSuchTable]).
      and("circular-alias", AutomaticJsonCodecBuilder[CircularAliasDefinition]).
      // LexerException
      and("unexpected-escape", AutomaticJsonCodecBuilder[UnexpectedEscape]).
      and("bad-unicode-escape", AutomaticJsonCodecBuilder[BadUnicodeEscapeCharacter]).
      and("unicode-character-out-of-range", AutomaticJsonCodecBuilder[UnicodeCharacterOutOfRange]).
      and("unexpected-character", AutomaticJsonCodecBuilder[UnexpectedCharacter]).
      and("unexpected-eof", AutomaticJsonCodecBuilder[UnexpectedEOF]).
      and("unterminated-string", AutomaticJsonCodecBuilder[UnterminatedString]).
      // BadParse
      and("bad-parse", AutomaticJsonCodecBuilder[BadParse]).
      // TypecheckException
      and("no-such-function", AutomaticJsonCodecBuilder[NoSuchFunction]).
      and("type-mismatch", AutomaticJsonCodecBuilder[TypeMismatch]).
      and("ambiguous-call", AutomaticJsonCodecBuilder[AmbiguousCall]).
      and("non-boolean-where", AutomaticJsonCodecBuilder[NonBooleanWhere]).
      and("non-groupable-group-by", AutomaticJsonCodecBuilder[NonGroupableGroupBy]).
      and("non-boolean-having", AutomaticJsonCodecBuilder[NonBooleanHaving]).
      and("unorderable-order-by", AutomaticJsonCodecBuilder[UnorderableOrderBy]).
      build

    def encode(e: SoQLException) = {
      val JObject(fields) = rawCodec.encode(e)
      JObject(fields + ("english" -> JString(e.getMessage)))
    }
    def decode(v: JValue) = rawCodec.decode(v)
  }
}

sealed trait AggregateCheckException extends SoQLException
case class AggregateInUngroupedContext(function: FunctionName, clause: String, position: Position) extends SoQLException("Cannot use aggregate function `" + function + "' in " + clause, position) with AggregateCheckException
case class ColumnNotInGroupBys(column: ColumnName, position: Position) extends SoQLException("Column `" + column + "' not in group bys", position) with AggregateCheckException

sealed trait AliasAnalysisException extends SoQLException
case class RepeatedException(name: ColumnName, position: Position) extends SoQLException("Column `" + name + "' has already been excluded", position) with AliasAnalysisException // this should be called RepeatedExclusion
case class DuplicateAlias(name: ColumnName, position: Position) extends SoQLException("There is already a column named `" + name + "' selected", position) with AliasAnalysisException
case class NoSuchColumn(name: ColumnName, position: Position) extends SoQLException("No such column `" + name + "'", position) with AliasAnalysisException with TypecheckException
case class NoSuchTable(qualifier: String, position: Position) extends SoQLException("No such table `" + qualifier + "'", position) with AliasAnalysisException with TypecheckException
case class CircularAliasDefinition(name: ColumnName, position: Position) extends SoQLException("Circular reference while defining alias `" + name + "'", position) with AliasAnalysisException

sealed trait LexerException extends SoQLException
case class UnexpectedEscape(char: Char, position: Position) extends SoQLException("Unexpected escape character", position) with LexerException
case class BadUnicodeEscapeCharacter(char: Char, position: Position) extends SoQLException("Bad character in unicode escape", position) with LexerException
case class UnicodeCharacterOutOfRange(value: Int, position:Position) extends SoQLException("Unicode character out of range", position) with LexerException
case class UnexpectedCharacter(char: Char, position: Position) extends SoQLException("Unexpected character", position) with LexerException
case class UnexpectedEOF(position: Position) extends SoQLException("Unexpected end of input", position) with LexerException
case class UnterminatedString(position: Position) extends SoQLException("Unterminated string", position) with LexerException

case class BadParse(message: String, position: Position) extends SoQLException(message, position)

sealed trait TypecheckException extends SoQLException
case class NoSuchFunction(name: FunctionName, arity: Int, position: Position) extends SoQLException("No such function `" + name + "/" + arity + "'", position) with TypecheckException
case class TypeMismatch(name: FunctionName, actual: TypeName, position: Position) extends SoQLException("Cannot pass a value of type `" + actual + "' to function `" + name + "'", position) with TypecheckException
case class AmbiguousCall(name: FunctionName, position: Position) extends SoQLException("Ambiguous call to `" + name + "'", position) with TypecheckException

case class NonBooleanWhere(typ: TypeName, position: Position) extends SoQLException("Cannot filter by an expression of type `" + typ + "'", position) with TypecheckException
case class NonGroupableGroupBy(typ: TypeName, position: Position) extends SoQLException("Cannot group by an expression of type `" + typ + "'", position) with TypecheckException
case class NonBooleanHaving(typ: TypeName, position: Position) extends SoQLException("Cannot filter by an expression of type `" + typ + "'", position) with TypecheckException
case class UnorderableOrderBy(typ: TypeName, position: Position) extends SoQLException("Cannot order by an expression of type `" + typ + "'", position) with TypecheckException
