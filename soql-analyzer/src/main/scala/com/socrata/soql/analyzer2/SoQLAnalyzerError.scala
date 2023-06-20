package com.socrata.soql.analyzer2

import scala.annotation.tailrec
import scala.util.parsing.input.{Position, NoPosition}
import scala.reflect.ClassTag

import com.rojoma.json.v3.ast.{JValue, JObject, JNull, JString}
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode, DecodeError}
import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.matcher._
import com.rojoma.json.v3.util.{SimpleHierarchyEncodeBuilder, SimpleHierarchyDecodeBuilder, SimpleHierarchyCodecBuilder, AutomaticJsonEncodeBuilder, AutomaticJsonDecodeBuilder, AutomaticJsonCodecBuilder, InternalTag, TagAndValue, NoTag}
import com.rojoma.json.v3.util.OrJNull.implicits._

import com.socrata.soql.collection.CovariantSet
import com.socrata.soql.environment.{ColumnName, ResourceName, HoleName, FunctionName, TypeName}
import com.socrata.soql.parsing.{RecursiveDescentParser, SoQLPosition}

sealed abstract class SoQLAnalyzerError[+RNS, +Data <: SoQLAnalyzerError.Payload](msg: String)

object SoQLAnalyzerError {
  private implicit class AugCodec[T <: AnyRef](shc: SimpleHierarchyCodecBuilder[T]) {
    def and[U <: T : ClassTag](tag: String, codec: JsonEncode[U] with JsonDecode[U]) =
      shc.branch[U](tag)(codec, codec, implicitly)
  }

  private implicit object PositionCodec extends JsonEncode[Position] with JsonDecode[Position] {
    private val row = Variable[Int]()
    private val col = Variable[Int]()
    private val text = Variable[String]()
    private val pattern =
      PObject(
        "row" -> row,
        "column" -> col,
        "text" -> text
      )

    def encode(p: Position) =
      p match {
        case NoPosition =>
          JNull
        case other =>
          pattern.generate(row := p.line, col := p.column, text := p.longString.split('\n')(0))
      }

    def decode(v: JValue) =
      v match {
        case JNull =>
          Right(NoPosition)
        case other =>
          pattern.matches(v).map { results =>
            SoQLPosition(row(results), col(results), text(results), col(results))
          }
      }
  }

  sealed abstract class NonTextualError(msg: String) extends SoQLAnalyzerError[Nothing, Nothing](msg)
  object NonTextualError {
    implicit val jCodec = SimpleHierarchyCodecBuilder[NonTextualError](InternalTag("type"))
      .and("invalid-parameter-type", AutomaticJsonCodecBuilder[InvalidParameterType])
      .build
  }
  case class TextualError[+RNS, +Data <: Payload](scope: RNS, canonicalName: Option[CanonicalName], position: Position, data: Data) extends SoQLAnalyzerError[RNS, Data](data.msg)
  object TextualError {
    implicit def textualErrorEncode[RNS: JsonEncode, Data <: Payload] = new JsonEncode[TextualError[RNS, Data]] {
      def encode(x: TextualError[RNS, Data]) = {
        val JObject(fields) = dataCodec.exactlyCodec.encode(x.data)
        json"""{
          scope: ${x.scope},
          canonicalName: ${x.canonicalName.orJNull},
          position: ${x.position},
          ..$fields,
          english: ${x.data.msg}
        }"""
      }
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

  trait DataCodec[T <: Payload] {
    protected def baseDataCodec = SimpleHierarchyCodecBuilder[T](TagAndValue("type", "data"))
    private[SoQLAnalyzerError] def exactlyCodec: JsonEncode[T] with JsonDecode[T]
  }

  // bit of a hack to make it so that if some "case object" is changed
  // to a "case class", it'll cause a compiler error in the relevant
  // codec.
  sealed trait Singleton
  private def singletonCodec[T <: Singleton](x: T) = new JsonEncode[T] with JsonDecode[T] {
    def encode(x: T) = JObject.canonicalEmpty
    def decode(v: JValue) = v match {
      case JObject(_) => Right(x)
      case other => Left(DecodeError.InvalidType(expected = JObject, got = other.jsonType))
    }
  }

  case class InvalidParameterType(
    canonicalName: Option[CanonicalName],
    param: HoleName,
    expected: TypeName,
    got: TypeName
  ) extends NonTextualError(s"Invalid parameter value for ${canonicalName.fold("")(_.name + "/")}${param}: expected ${expected} but got ${got}")

  sealed abstract class Payload(val msg: String)

  sealed abstract class TableFinderError(msg: String) extends Payload(msg)
  object TableFinderError {
    case class NotFound(name: ResourceName) extends TableFinderError(s"Dataset $name not found")
    case class PermissionDenied(name: ResourceName) extends TableFinderError(s"Permission denied accessing $name")
    case class RecursiveQuery(stack: Seq[CanonicalName]) extends TableFinderError(s"Recursive query found in saved views ${stack.mkString(", ")}")

    private[SoQLAnalyzerError] def augment[P >: TableFinderError <: AnyRef : ClassTag](b: SimpleHierarchyCodecBuilder[P]): SimpleHierarchyCodecBuilder[P] =
      ParserError.augment(
        b.
          and("not-found", AutomaticJsonCodecBuilder[NotFound]).
          and("permission-denied", AutomaticJsonCodecBuilder[PermissionDenied]).
          and("recursive-query", AutomaticJsonCodecBuilder[RecursiveQuery])
      )

    implicit val dataCodec = new DataCodec[TableFinderError] {
      private[SoQLAnalyzerError] val exactlyCodec = augment(baseDataCodec).build
    }
  }

  sealed abstract class ParserError(msg: String) extends TableFinderError(msg)

  object ParserError {
    case class UnexpectedEscape(char: Char) extends ParserError("Unexpected escape character")
    case class BadUnicodeEscapeCharacter(char: Char) extends ParserError("Bad character in unicode escape")
    case class UnicodeCharacterOutOfRange(value: Int) extends ParserError("Unicode character out of range")
    case class UnexpectedCharacter(char: Char) extends ParserError("Unicode character out of range")
    case object UnexpectedEOF extends ParserError("Unexpected end of input") with Singleton
    case object UnterminatedString extends ParserError("Unterminated string") with Singleton

    case class ExpectedToken(
      expectations: Seq[String],
      got: String
    ) extends ParserError(RecursiveDescentParser.expectationStringsToEnglish(expectations, got))
    case object ExpectedLeafQuery extends ParserError("Expected a non-compound query on the right side of a pipe operator") with Singleton
    case object UnexpectedStarSelect extends ParserError("Star selections must come at the start of the select-list") with Singleton
    case object UnexpectedSystemStarSelect extends ParserError("System column star selections must come before user column star selections") with Singleton

    private[SoQLAnalyzerError] def augment[P >: ParserError <: AnyRef : ClassTag](b: SimpleHierarchyCodecBuilder[P]): SimpleHierarchyCodecBuilder[P] =
      b.
        and("unexpected-escape", AutomaticJsonCodecBuilder[UnexpectedEscape]).
        and("bad-unicode-escape-character", AutomaticJsonCodecBuilder[BadUnicodeEscapeCharacter]).
        and("unicode-character-out-of-range", AutomaticJsonCodecBuilder[UnicodeCharacterOutOfRange]).
        and("unexpected-character", AutomaticJsonCodecBuilder[UnexpectedCharacter]).
        and("unexpected-eof", singletonCodec(UnexpectedEOF)).
        and("unterminated-string", singletonCodec(UnterminatedString)).
        and("expected-token", AutomaticJsonCodecBuilder[ExpectedToken]).
        and("expected-leaf-query", singletonCodec(ExpectedLeafQuery)).
        and("unexpected-star-select", singletonCodec(UnexpectedStarSelect)).
        and("unexpected-system-star-select", singletonCodec(UnexpectedSystemStarSelect))

    implicit val dataCodec = new DataCodec[ParserError] {
      private[SoQLAnalyzerError] val exactlyCodec = augment(baseDataCodec).build
    }
  }

  sealed abstract class AnalysisError(msg: String) extends Payload(msg)
  object AnalysisError {
    private[SoQLAnalyzerError] def augment[P >: AnalysisError <: AnyRef : ClassTag](b: SimpleHierarchyCodecBuilder[P]): SimpleHierarchyCodecBuilder[P] =
      AliasAnalysisError.augment(
        TypecheckError.augment(
          b.
            and("expected-boolean", AutomaticJsonCodecBuilder[ExpectedBoolean]).
            and("incorrect-number-of-udf-parameters", AutomaticJsonCodecBuilder[IncorrectNumberOfUdfParameters]).
            and("distinct-not-prefix-of-order-by", singletonCodec(DistinctNotPrefixOfOrderBy)).
            and("order-by-must-be-selected-when-distinct", singletonCodec(OrderByMustBeSelectedWhenDistinct)).
            and("invalid-group-by", AutomaticJsonCodecBuilder[InvalidGroupBy]).
            and("parameters-for-non-udf", AutomaticJsonCodecBuilder[ParametersForNonUDF]).
            and("table-alias-already-exists", AutomaticJsonCodecBuilder[TableAliasAlreadyExists]).
            and("from-required", singletonCodec(FromRequired)).
            and("from-forbidden", singletonCodec(FromForbidden)).
            and("from-this-without-context", singletonCodec(FromThisWithoutContext)).
            and("table-operation-type-mismatch", AutomaticJsonCodecBuilder[TableOperationTypeMismatch]).
            and("literal-not-allowed-in-group-by", singletonCodec(LiteralNotAllowedInGroupBy)).
            and("literal-not-allowed-in-order-by", singletonCodec(LiteralNotAllowedInOrderBy)).
            and("literal-not-allowed-in-distinct-on", singletonCodec(LiteralNotAllowedInDistinctOn)).
            and("aggregate-function-not-allowed", AutomaticJsonCodecBuilder[AggregateFunctionNotAllowed]).
            and("ungrouped-column-reference", singletonCodec(UngroupedColumnReference)).
            and("window-function-not-allowed", AutomaticJsonCodecBuilder[WindowFunctionNotAllowed]).
            and("parameterless-udf", AutomaticJsonCodecBuilder[ParameterlessTableFunction]).
            and("illegal-this-reference", singletonCodec(IllegalThisReference)).
            and("reserved-table-name", AutomaticJsonCodecBuilder[ReservedTableName])
        )
      )

    implicit val dataCodec = new DataCodec[AnalysisError] {
      private[SoQLAnalyzerError] val exactlyCodec = augment(baseDataCodec).build
    }

    case class ExpectedBoolean(
      got: TypeName
    ) extends AnalysisError("Expected boolean, but got ${got}")

    case class IncorrectNumberOfUdfParameters(
      udf: ResourceName,
      expected: Int,
      got: Int
    ) extends AnalysisError(s"UDF expected ${expected} parameter(s) but got ${got}")

    case object DistinctNotPrefixOfOrderBy extends AnalysisError("When both DISTINCT ON and ORDER BY are present, the DISTINCT BY's expression list must be a prefix of the ORDER BY") with Singleton
    case object OrderByMustBeSelectedWhenDistinct extends AnalysisError("When both DISTINCT and ORDER BY are present, all columns in ORDER BY must also be selected") with Singleton
    case class InvalidGroupBy(typ: TypeName) extends AnalysisError(s"Cannot GROUP BY an expression of type ${typ}")
    case class ParametersForNonUDF(nonUdf: ResourceName) extends AnalysisError("Cannot provide parameters to a non-UDF")
    case class TableAliasAlreadyExists(alias: ResourceName) extends AnalysisError(s"Table alias ${alias} already exists")
    case object FromRequired extends AnalysisError("FROM required in a query without an implicit context") with Singleton
    case object FromForbidden extends AnalysisError("FROM (other than FROM @this) forbidden in a query with an implicit context") with Singleton
    case object FromThisWithoutContext extends AnalysisError("FROM @this cannot be used in a query without an implicit context") with Singleton
    case class TableOperationTypeMismatch(left: Seq[TypeName], right: Seq[TypeName]) extends AnalysisError("The left- and right-hand sides of a table operation must have the same schema")
    case object LiteralNotAllowedInGroupBy extends AnalysisError("Literal values are not allowed in GROUP BY") with Singleton
    case object LiteralNotAllowedInOrderBy extends AnalysisError("Literal values are not allowed in ORDER BY") with Singleton
    case object LiteralNotAllowedInDistinctOn extends AnalysisError("Literal values are not allowed in DISTINCT ON") with Singleton
    case class AggregateFunctionNotAllowed(name: FunctionName) extends AnalysisError("Aggregate function not allowed here")
    case object UngroupedColumnReference extends AnalysisError("Reference to a column not specified in GROUP BY") with Singleton
    case class WindowFunctionNotAllowed(name: FunctionName) extends AnalysisError("Window function not allowed here")
    case class ParameterlessTableFunction(name: ResourceName) extends AnalysisError("UDFs require parameters")
    case object IllegalThisReference extends AnalysisError("@this can only be used as `FROM @this`, not in joins") with Singleton
    case class ReservedTableName(name: ResourceName) extends AnalysisError("Table name '$name' is reserved")

    sealed trait TypecheckError extends AnalysisError

    object TypecheckError {
      private[SoQLAnalyzerError] def augment[P >: TypecheckError <: AnyRef : ClassTag](b: SimpleHierarchyCodecBuilder[P]): SimpleHierarchyCodecBuilder[P] =
        b.
          and("no-such-column", AutomaticJsonCodecBuilder[NoSuchColumn]).
          and("unknown-udf-parameter", AutomaticJsonCodecBuilder[UnknownUDFParameter]).
          and("unknown-user-parameter", AutomaticJsonCodecBuilder[UnknownUserParameter]).
          and("no-such-function", AutomaticJsonCodecBuilder[NoSuchFunction]).
          and("type-mismatch", AutomaticJsonCodecBuilder[TypeMismatch]).
          and("requires-window", AutomaticJsonCodecBuilder[RequiresWindow]).
          and("non-aggregate", AutomaticJsonCodecBuilder[NonAggregate]).
          and("non-window-function", AutomaticJsonCodecBuilder[NonWindowFunction]).
          and("distinct-with-over", AutomaticJsonCodecBuilder[DistinctWithOver]).
          and("groups-requires-order-by", singletonCodec(GroupsRequiresOrderBy)).
          and("unordered-order-by", AutomaticJsonCodecBuilder[UnorderedOrderBy])


      implicit val dataCodec = new DataCodec[TypecheckError] {
        private[SoQLAnalyzerError] val exactlyCodec = augment(baseDataCodec).build
      }

      case class UnorderedOrderBy(
        typ: TypeName
      ) extends AnalysisError(s"Cannot ORDER BY or DISTINCT ON an expression of type ${typ}") with TypecheckError

      case class NoSuchColumn(
        qualifier: Option[ResourceName],
        name: ColumnName
      ) extends AnalysisError(s"No such column ${qualifier.fold("")("@" + _ + ".")}${name}") with TypecheckError with AliasAnalysisError

      case class UnknownUDFParameter(
        name: HoleName
      ) extends AnalysisError(s"No such UDF parameter ${name}") with TypecheckError

      case class UnknownUserParameter(
        view: Option[CanonicalName],
        name: HoleName
      ) extends AnalysisError(s"No such user parameter ${view.fold("")(_.name + "/")}${name}") with TypecheckError

      case class NoSuchFunction(
        name: FunctionName,
        arity: Int
      ) extends AnalysisError(s"No such function ${name}/${arity}") with TypecheckError

      case class TypeMismatch(
        expected: Set[TypeName],
        found: TypeName
      ) extends AnalysisError(s"Type mismatch: found ${found}") with TypecheckError

      case class RequiresWindow(
        name: FunctionName
      ) extends AnalysisError(s"${name} requires a window clause") with TypecheckError

      case class NonAggregate(
        name: FunctionName
      ) extends AnalysisError(s"${name} is not an aggregate function") with TypecheckError

      case class NonWindowFunction(
        name: FunctionName
      ) extends AnalysisError(s"${name} is not a window function") with TypecheckError

      case class DistinctWithOver(
      ) extends AnalysisError(s"Cannot use DISTINCT with OVER") with TypecheckError

      case object GroupsRequiresOrderBy extends AnalysisError(s"GROUPS mode requires and ORDER BY in the window definition") with TypecheckError with Singleton
    }

    sealed trait AliasAnalysisError extends AnalysisError
    object AliasAnalysisError {
      private[SoQLAnalyzerError] def augment[P >: AliasAnalysisError <: AnyRef : ClassTag](b: SimpleHierarchyCodecBuilder[P]): SimpleHierarchyCodecBuilder[P] =
        b.
          and("repeated-exclusion", AutomaticJsonCodecBuilder[RepeatedExclusion]).
          and("duplicate-alias", AutomaticJsonCodecBuilder[DuplicateAlias]).
          and("circular-alias-definition", AutomaticJsonCodecBuilder[CircularAliasDefinition])

      implicit val dataCodec = new DataCodec[AliasAnalysisError] {
        private[SoQLAnalyzerError] val exactlyCodec = augment(baseDataCodec).build
      }

      case class RepeatedExclusion(
        name: ColumnName
      ) extends AnalysisError("Column `" + name + "' has already been excluded") with AliasAnalysisError

      case class DuplicateAlias(
        name: ColumnName
      ) extends AnalysisError("There is already a column named `" + name + "' selected") with AliasAnalysisError

      case class CircularAliasDefinition(
        name: ColumnName
      ) extends AnalysisError("Circular reference while defining alias `" + name + "'") with AliasAnalysisError
    }
  }

  implicit object dataCodec extends DataCodec[Payload] {
    private[SoQLAnalyzerError] def filter(t: Payload) =
      Some(t)
    val exactlyCodec =
      AnalysisError.augment(TableFinderError.augment(baseDataCodec)).build
  }

  private implicit def textualErrorDecode[RNS: JsonDecode, Data <: Payload : DataCodec] = new JsonDecode[TextualError[RNS, Data]] {
    private implicit val dataCodec = implicitly[DataCodec[Data]]

    private val scope = Variable.decodeOnly[RNS]()
    private val canonicalName = Variable.decodeOnly[CanonicalName]()
    private val position = Variable.decodeOnly[Position]()

    private val pattern = PObject(
      "scope" -> scope,
      "canonicalName" -> FirstOf(canonicalName, JNull),
      "position" -> position
    )

    def decode(x: JValue) = {
      pattern.matches(x).flatMap { results =>
        dataCodec.exactlyCodec.decode(x).map { data =>
          TextualError(scope(results), canonicalName.get(results), position(results), data)
        }
      }
    }
  }

  // ick, but there are (currently) only two branches here, so...
  implicit def jEncode[RNS: JsonEncode, Data <: Payload : DataCodec]: JsonEncode[SoQLAnalyzerError[RNS, Data]] =
    SimpleHierarchyEncodeBuilder[SoQLAnalyzerError[RNS, Data]](NoTag).
      branch[TextualError[RNS, Data]].
      branch[NonTextualError].
      build

  implicit def jDecode[RNS: JsonDecode, Data <: Payload : DataCodec]: JsonDecode[SoQLAnalyzerError[RNS, Data]] =
    SimpleHierarchyDecodeBuilder[SoQLAnalyzerError[RNS, Data]](NoTag).
      branch[TextualError[RNS, Data]].
      branch[NonTextualError].
      build
}
