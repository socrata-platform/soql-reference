package com.socrata.soql.analyzer2

import scala.annotation.tailrec
import scala.language.higherKinds
import scala.reflect.ClassTag

import com.rojoma.json.v3.ast.{JValue, JObject, JNull, JString}
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode, DecodeError}
import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.matcher._
import com.rojoma.json.v3.util.{AutomaticJsonEncodeBuilder, AutomaticJsonDecodeBuilder, AutomaticJsonCodecBuilder, InternalTag, TagAndValue, NoTag, AutomaticJsonCodec}
import com.rojoma.json.v3.util.OrJNull.implicits._

import com.socrata.soql.collection.CovariantSet
import com.socrata.soql.environment.{ColumnName, ResourceName, ScopedResourceName, Source, HoleName, FunctionName, TypeName}
import com.socrata.soql.jsonutils.PositionCodec
import com.socrata.soql.parsing.{RecursiveDescentParser, SoQLPosition}
import com.socrata.soql.util.{SoQLErrorCodec, SoQLErrorEncode, SoQLErrorDecode, EncodedError, ErrorHierarchyCodecBuilder}

sealed abstract class SoQLError[+RNS] {
  def maybeSource: Option[Source[RNS]]
}
object SoQLError {
  def errorCodecs[RNS : JsonEncode : JsonDecode, T >: SoQLError[RNS] <: AnyRef](
    codecs: SoQLErrorCodec.ErrorCodecs[T] = new SoQLErrorCodec.ErrorCodecs[T]
  ): SoQLErrorCodec.ErrorCodecs[T] =
    SoQLAnalyzerError.errorCodecs[RNS, T](TableFinderError.errorCodecs[RNS, T](codecs))
}

trait TextualError[+RNS] extends SoQLError[RNS] {
  val source: Source[RNS]
  override final def maybeSource = Some(source)
}
object TextualError {
  def simpleEncode[RNS: JsonEncode, T[X] <: TextualError[X]](tag: String, msg: String) = {
    new SoQLErrorEncode[T[RNS]] {
      override val code = tag
      def encode(err: T[RNS]) = {
        result(JObject.canonicalEmpty, msg, err.source)
      }
    }
  }

  def simpleDecode[RNS: JsonDecode, T[X] <: TextualError[X]](tag: String, f: (Source[RNS]) => T[RNS]) = {
    new SoQLErrorDecode[T[RNS]] {
      override val code = tag
      def decode(v: EncodedError) =
        for {
          source <- source[RNS](v)
        } yield {
          f(source)
        }
    }
  }
}

trait PossiblyTextualError[+RNS] extends SoQLError[RNS] {
  val source: Option[Source[RNS]]
  override final def maybeSource = source
}
object PossiblyTextualError {
  def simpleEncode[RNS: JsonEncode, T[X] <: PossiblyTextualError[X]](tag: String, msg: String) = {
    new SoQLErrorEncode[T[RNS]] {
      override val code = tag
      def encode(err: T[RNS]) = {
        result(JObject.canonicalEmpty, msg, err.source)
      }
    }
  }

  def simpleDecode[RNS: JsonDecode, T[X] <: PossiblyTextualError[X]](tag: String, f: (Option[Source[RNS]]) => T[RNS]) = {
    new SoQLErrorDecode[T[RNS]] {
      override val code = tag
      def decode(v: EncodedError) =
        for {
          source <- sourceOpt[RNS](v)
        } yield {
          f(source)
        }
    }
  }
}

sealed abstract class TableFinderError[+RNS] extends SoQLError[RNS]
sealed abstract class TableFinderLookupError[+RNS] extends TableFinderError[RNS] with PossiblyTextualError[RNS]
object TableFinderError {
  case class NotFound[+RNS](source: Option[Source[RNS]], name: ResourceName) extends TableFinderLookupError[RNS]
  object NotFound {
    private val tag = "soql.tablefinder.dataset-not-found"

    @AutomaticJsonCodec
    private case class Fields(name: ResourceName)

    implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[NotFound[RNS]] {
      override val code = tag
      def encode(err: NotFound[RNS]) =
        result(Fields(err.name), "Dataset not found: " + err.name.name, err.source)
    }

    implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[NotFound[RNS]] {
      override val code = tag
      def decode(v: EncodedError) =
        for {
          fields <- data[Fields](v)
          source <- sourceOpt[RNS](v)
        } yield {
          NotFound(source, fields.name)
        }
    }
  }

  case class PermissionDenied[+RNS](source: Option[Source[RNS]], name: ResourceName) extends TableFinderLookupError[RNS]
  object PermissionDenied {
    private val tag = "soql.tablefinder.permission-denied"

    @AutomaticJsonCodec
    private case class Fields(name: ResourceName)

    implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[PermissionDenied[RNS]] {
      override val code = tag
      def encode(err: PermissionDenied[RNS]) =
        result(Fields(err.name), "Permission denied: " + err.name.name, err.source)
    }

    implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[PermissionDenied[RNS]] {
      override val code = tag
      def decode(v: EncodedError) =
        for {
          fields <- data[Fields](v)
          source <- sourceOpt[RNS](v)
        } yield {
          PermissionDenied(source, fields.name)
        }
    }
  }

  case class RecursiveQuery[+RNS](definiteSource: Source[RNS], stack: Seq[CanonicalName]) extends TableFinderLookupError[RNS] {
    val source = Some(definiteSource)
  }
  object RecursiveQuery {
    private val tag = "soql.tablefinder.recursive-query"

    @AutomaticJsonCodec
    private case class Fields(stack: Seq[CanonicalName])

    implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[RecursiveQuery[RNS]] {
      override val code = tag
      def encode(err: RecursiveQuery[RNS]) =
        result(Fields(err.stack), "Recursive query detected: " + err.stack.map(_.name).mkString(", "), err.source)
    }

    implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[RecursiveQuery[RNS]] {
      override val code = tag
      def decode(v: EncodedError) =
        for {
          fields <- data[Fields](v)
          source <- source[RNS](v)
        } yield {
          RecursiveQuery(source, fields.stack)
        }
    }
  }

  def errorCodecs[RNS : JsonEncode : JsonDecode, T >: TableFinderError[RNS] <: AnyRef](
    codecs: SoQLErrorCodec.ErrorCodecs[T] = new SoQLErrorCodec.ErrorCodecs[T]
  ): SoQLErrorCodec.ErrorCodecs[T] =
    ParserError.errorCodecs[RNS, T](codecs)
      .branch[NotFound[RNS]]
      .branch[PermissionDenied[RNS]]
      .branch[RecursiveQuery[RNS]]
}

sealed abstract class ParserError[+RNS] extends TableFinderError[RNS] with TextualError[RNS]

object ParserError {
  private implicit object CharCodec extends JsonEncode[Char] with JsonDecode[Char] {
    def encode(c: Char) = JString(c.toString)
    def decode(v: JValue) = v match {
      case JString(s) if s.length == 1 => Right(s.charAt(0))
      case other: JString => Left(DecodeError.InvalidValue(other))
      case other => Left(DecodeError.InvalidType(expected = JString, got = other.jsonType))
    }
  }

  case class UnexpectedEscape[+RNS](source: Source[RNS], char: Char) extends ParserError[RNS]
  object UnexpectedEscape {
    private val tag = "soql.parser.unexpected-escape"

    @AutomaticJsonCodec
    private case class Fields(char: Char)

    implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[UnexpectedEscape[RNS]] {
      override val code = tag
      def encode(err: UnexpectedEscape[RNS]) =
        result(Fields(err.char), "Unexpected escape character: " + JString(err.char.toString), err.source)
    }

    implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[UnexpectedEscape[RNS]] {
      override val code = tag
      def decode(v: EncodedError) =
        for {
          fields <- data[Fields](v)
          source <- source[RNS](v)
        } yield {
          UnexpectedEscape(source, fields.char)
        }
    }
  }

  case class BadUnicodeEscapeCharacter[+RNS](source: Source[RNS], char: Char) extends ParserError[RNS]
  object BadUnicodeEscapeCharacter {
    private val tag = "soql.parser.bad-unicode-escape"

    @AutomaticJsonCodec
    private case class Fields(char: Char)

    implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[BadUnicodeEscapeCharacter[RNS]] {
      override val code = tag
      def encode(err: BadUnicodeEscapeCharacter[RNS]) =
        result(Fields(err.char), "Bad character in unicode escape: " + JString(err.char.toString), err.source)
    }

    implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[BadUnicodeEscapeCharacter[RNS]] {
      override val code = tag
      def decode(v: EncodedError) =
        for {
          fields <- data[Fields](v)
          source <- source[RNS](v)
        } yield {
          BadUnicodeEscapeCharacter(source, fields.char)
        }
    }
  }

  case class UnicodeCharacterOutOfRange[+RNS](source: Source[RNS], codepoint: Int) extends ParserError[RNS]
  object UnicodeCharacterOutOfRange {
    private val tag = "soql.parser.unicode-character-out-of-range"

    @AutomaticJsonCodec
    private case class Fields(codepoint: Int)

    implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[UnicodeCharacterOutOfRange[RNS]] {
      override val code = tag
      def encode(err: UnicodeCharacterOutOfRange[RNS]) =
        result(Fields(err.codepoint), "Unicode codepoint out of range: " + err.codepoint, err.source)
    }

    implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[UnicodeCharacterOutOfRange[RNS]] {
      override val code = tag
      def decode(v: EncodedError) =
        for {
          fields <- data[Fields](v)
          source <- source[RNS](v)
        } yield {
          UnicodeCharacterOutOfRange(source, fields.codepoint)
        }
    }
  }

  case class UnexpectedCharacter[+RNS](source: Source[RNS], char: Char) extends ParserError[RNS]
  object UnexpectedCharacter {
    private val tag = "soql.parser.unexpected-character"

    @AutomaticJsonCodec
    private case class Fields(char: Char)

    implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[UnexpectedCharacter[RNS]] {
      override val code = tag
      def encode(err: UnexpectedCharacter[RNS]) =
        result(Fields(err.char), "Unexpected character: " + JString(err.char.toString), err.source)
    }

    implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[UnexpectedCharacter[RNS]] {
      override val code = tag
      def decode(v: EncodedError) =
        for {
          fields <- data[Fields](v)
          source <- source[RNS](v)
        } yield {
          UnexpectedCharacter(source, fields.char)
        }
    }
  }

  case class UnexpectedEOF[+RNS](source: Source[RNS]) extends ParserError[RNS]
  object UnexpectedEOF {
    private val tag = "soql.parser.unexpected-end-of-input"

    implicit def encode[RNS: JsonEncode] = TextualError.simpleEncode[RNS, UnexpectedEOF](tag, "Unexpected end of input")
    implicit def decode[RNS: JsonDecode] = TextualError.simpleDecode[RNS, UnexpectedEOF](tag, UnexpectedEOF(_))
  }

  case class UnterminatedString[+RNS](source: Source[RNS]) extends ParserError[RNS]
  object UnterminatedString {
    private val tag = "soql.parser.unterminated-string"

    implicit def encode[RNS: JsonEncode] = TextualError.simpleEncode[RNS, UnterminatedString](tag, "Unterminated string")
    implicit def decode[RNS: JsonDecode] = TextualError.simpleDecode[RNS, UnterminatedString](tag, UnterminatedString(_))
  }

  case class ExpectedToken[+RNS](
    source: Source[RNS],
    expectations: Seq[String],
    got: String
  ) extends ParserError[RNS]
  object ExpectedToken {
    private val tag = "soql.parser.expected-token"

    @AutomaticJsonCodec
    private case class Fields(expectations: Seq[String], got: String)

    implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[ExpectedToken[RNS]] {
      override val code = tag
      def encode(err: ExpectedToken[RNS]) =
        result(Fields(err.expectations, err.got), RecursiveDescentParser.expectationStringsToEnglish(err.expectations, err.got), err.source)
    }

    implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[ExpectedToken[RNS]] {
      override val code = tag
      def decode(v: EncodedError) =
        for {
          fields <- data[Fields](v)
          source <- source[RNS](v)
        } yield {
          ExpectedToken(source, fields.expectations, fields.got)
        }
    }
  }

  case class ExpectedLeafQuery[+RNS](source: Source[RNS]) extends ParserError[RNS]
  object ExpectedLeafQuery {
    private val tag = "soql.parser.expected-leaf-query"

    implicit def encode[RNS: JsonEncode] = TextualError.simpleEncode[RNS, ExpectedLeafQuery](tag, "Expected a non-compound query on the right side of a pipe operator")
    implicit def decode[RNS: JsonDecode] = TextualError.simpleDecode[RNS, ExpectedLeafQuery](tag, ExpectedLeafQuery(_))
  }

  case class UnexpectedStarSelect[+RNS](source: Source[RNS]) extends ParserError[RNS]
  object UnexpectedStarSelect {
    private val tag = "soql.parser.unexpected-star-select"

    implicit def encode[RNS: JsonEncode] = TextualError.simpleEncode[RNS, UnexpectedStarSelect](tag, "Star selections must come at the start of the select-list")
    implicit def decode[RNS: JsonDecode] = TextualError.simpleDecode[RNS, UnexpectedStarSelect](tag, UnexpectedStarSelect(_))
  }

  case class UnexpectedSystemStarSelect[+RNS](source: Source[RNS]) extends ParserError[RNS]
  object UnexpectedSystemStarSelect {
    private val tag = "soql.parser.unexpected-system-star-select"

    implicit def encode[RNS: JsonEncode] = TextualError.simpleEncode[RNS, UnexpectedSystemStarSelect](tag, "System column star selections must come at the start of the select-list, before user column star selections")
    implicit def decode[RNS: JsonDecode] = TextualError.simpleDecode[RNS, UnexpectedSystemStarSelect](tag, UnexpectedSystemStarSelect(_))
  }

  def errorCodecs[RNS : JsonEncode : JsonDecode, T >: ParserError[RNS] <: AnyRef](
    codecs: SoQLErrorCodec.ErrorCodecs[T] = new SoQLErrorCodec.ErrorCodecs[T]
  ): SoQLErrorCodec.ErrorCodecs[T] =
    codecs
      .branch[UnexpectedEscape[RNS]]
      .branch[BadUnicodeEscapeCharacter[RNS]]
      .branch[UnicodeCharacterOutOfRange[RNS]]
      .branch[UnexpectedCharacter[RNS]]
      .branch[UnexpectedEOF[RNS]]
      .branch[UnterminatedString[RNS]]
      .branch[ExpectedToken[RNS]]
      .branch[ExpectedLeafQuery[RNS]]
      .branch[UnexpectedStarSelect[RNS]]
      .branch[UnexpectedSystemStarSelect[RNS]]
}

sealed abstract class SoQLAnalyzerError[+RNS] extends SoQLError[RNS]
object SoQLAnalyzerError {
  case class InvalidParameterType(
    qualifier: Option[CanonicalName],
    param: HoleName,
    expected: TypeName,
    got: TypeName
  ) extends SoQLAnalyzerError[Nothing] {
    override final def maybeSource = None
  }
  object InvalidParameterType {
    implicit val codec = new SoQLErrorCodec[InvalidParameterType]("invalid-parameter-type") {
      private implicit val jCodec = AutomaticJsonCodecBuilder[InvalidParameterType]

      def encode(err: InvalidParameterType) =
        result(
          err,
          s"Invalid parameter value for ${err.qualifier.fold("")(_.name + "/")}${err.param}: expected ${err.expected} but got ${err.got}"
        )

      def decode(v: EncodedError) =
        data[InvalidParameterType](v)
    }
  }

  sealed abstract class TextualSoQLAnalyzerError[+RNS] extends SoQLAnalyzerError[RNS] with TextualError[RNS]
  case class ExpectedBoolean[+RNS](
    source: Source[RNS],
    got: TypeName
  ) extends TextualSoQLAnalyzerError[RNS]
  object ExpectedBoolean {
    private val tag = "soql.analyzer.expected-boolean"

    @AutomaticJsonCodec
    private case class Fields(got: TypeName)

    implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[ExpectedBoolean[RNS]] {
      override val code = tag
      def encode(err: ExpectedBoolean[RNS]) =
        result(Fields(err.got), s"Expected a boolean expression but got ${err.got}", err.source)
    }

    implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[ExpectedBoolean[RNS]] {
      override val code = tag
      def decode(v: EncodedError) =
        for {
          fields <- data[Fields](v)
          source <- source[RNS](v)
        } yield {
          ExpectedBoolean(source, fields.got)
        }
    }
  }

  case class IncorrectNumberOfUdfParameters[+RNS](
    source: Source[RNS],
    udf: ResourceName,
    expected: Int,
    got: Int
  ) extends TextualSoQLAnalyzerError[RNS]
  object IncorrectNumberOfUdfParameters {
    private val tag = "soql.analyzer.incorrect-udf-parameter-count"

    @AutomaticJsonCodec
    private case class Fields(udf: ResourceName, expected: Int, got: Int)

    implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[IncorrectNumberOfUdfParameters[RNS]] {
      override val code = tag
      def encode(err: IncorrectNumberOfUdfParameters[RNS]) =
        result(Fields(err.udf, err.expected, err.got), s"Incorrect number of parameters to ${err.udf.name}: expected ${err.expected} but got ${err.got}", err.source)
    }

    implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[IncorrectNumberOfUdfParameters[RNS]] {
      override val code = tag
      def decode(v: EncodedError) =
        for {
          fields <- data[Fields](v)
          source <- source[RNS](v)
        } yield {
          IncorrectNumberOfUdfParameters(source, fields.udf, fields.expected, fields.got)
        }
    }
  }

  case class DistinctOnNotPrefixOfOrderBy[+RNS](
    source: Source[RNS]
  ) extends TextualSoQLAnalyzerError[RNS]
  object DistinctOnNotPrefixOfOrderBy {
    private val tag = "soql.analyzer.distinct-on-not-prefix-of-order-by"

    implicit def encode[RNS: JsonEncode] = TextualError.simpleEncode[RNS, DistinctOnNotPrefixOfOrderBy](tag, "The expressions in DISTINCT ON must lead in the ORDER BY clause")
    implicit def decode[RNS: JsonDecode] = TextualError.simpleDecode[RNS, DistinctOnNotPrefixOfOrderBy](tag, DistinctOnNotPrefixOfOrderBy(_))
  }

  case class OrderByMustBeSelectedWhenDistinct[+RNS](
    source: Source[RNS],
  ) extends TextualSoQLAnalyzerError[RNS]
  object OrderByMustBeSelectedWhenDistinct {
    private val tag = "soql.analyzer.order-by-must-be-selected-when-distinct"

    implicit def encode[RNS: JsonEncode] = TextualError.simpleEncode[RNS, OrderByMustBeSelectedWhenDistinct](tag, "The expressions in ORDER BY must be selected when using DISTINCT")
    implicit def decode[RNS: JsonDecode] = TextualError.simpleDecode[RNS, OrderByMustBeSelectedWhenDistinct](tag, OrderByMustBeSelectedWhenDistinct(_))
  }

  case class InvalidGroupBy[+RNS](
    source: Source[RNS],
    typ: TypeName
  ) extends TextualSoQLAnalyzerError[RNS]
  object InvalidGroupBy {
    private val tag = "soql.analyzer.invalid-group-by"

    @AutomaticJsonCodec
    private case class Fields(typ: TypeName)

    implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[InvalidGroupBy[RNS]] {
      override val code = tag
      def encode(err: InvalidGroupBy[RNS]) =
        result(Fields(err.typ), s"Cannot GROUP BY an expression of type ${err.typ}", err.source)
    }

    implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[InvalidGroupBy[RNS]] {
      override val code = tag
      def decode(v: EncodedError) =
        for {
          fields <- data[Fields](v)
          source <- source[RNS](v)
        } yield {
          InvalidGroupBy(source, fields.typ)
        }
    }
  }

  case class ParametersForNonUDF[+RNS](
    source: Source[RNS],
    nonUdf: ResourceName
  ) extends TextualSoQLAnalyzerError[RNS]
  object ParametersForNonUDF {
    private val tag = "soql.analyzer.parameters-for-non-udf"

    @AutomaticJsonCodec
    private case class Fields(nonUdf: ResourceName)

    implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[ParametersForNonUDF[RNS]] {
      override val code = tag
      def encode(err: ParametersForNonUDF[RNS]) =
        result(Fields(err.nonUdf), s"Cannot provide parameters to non-UDF ${err.nonUdf.name}", err.source)
    }

    implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[ParametersForNonUDF[RNS]] {
      override val code = tag
      def decode(v: EncodedError) =
        for {
          fields <- data[Fields](v)
          source <- source[RNS](v)
        } yield {
          ParametersForNonUDF(source, fields.nonUdf)
        }
    }
  }

  case class TableAliasAlreadyExists[+RNS](
    source: Source[RNS],
    alias: ResourceName
  ) extends TextualSoQLAnalyzerError[RNS]
  object TableAliasAlreadyExists {
    private val tag = "soql.analyzer.table-alias-already-exists"

    @AutomaticJsonCodec
    private case class Fields(alias: ResourceName)

    implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[TableAliasAlreadyExists[RNS]] {
      override val code = tag
      def encode(err: TableAliasAlreadyExists[RNS]) =
        result(Fields(err.alias), s"Table alias ${err.alias.name} already exists", err.source)
    }

    implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[TableAliasAlreadyExists[RNS]] {
      override val code = tag
      def decode(v: EncodedError) =
        for {
          fields <- data[Fields](v)
          source <- source[RNS](v)
        } yield {
          TableAliasAlreadyExists(source, fields.alias)
        }
    }
  }

  case class FromRequired[+RNS](
    source: Source[RNS],
  ) extends TextualSoQLAnalyzerError[RNS]
  object FromRequired {
    private val tag = "soql.analyzer.from-required"

    implicit def encode[RNS: JsonEncode] = TextualError.simpleEncode[RNS, FromRequired](tag, "FROM required in a query without an implicit context")
    implicit def decode[RNS: JsonDecode] = TextualError.simpleDecode[RNS, FromRequired](tag, FromRequired(_))
  }

  case class FromForbidden[+RNS](
    source: Source[RNS],
  ) extends TextualSoQLAnalyzerError[RNS]
  object FromForbidden {
    private val tag = "soql.analyzer.from-forbidden"

    implicit def encode[RNS: JsonEncode] = TextualError.simpleEncode[RNS, FromForbidden](tag, "FROM (other than FROM @this) forbidden in a query with an implicit context")
    implicit def decode[RNS: JsonDecode] = TextualError.simpleDecode[RNS, FromForbidden](tag, FromForbidden(_))
  }

  case class FromThisWithoutContext[+RNS](
    source: Source[RNS],
  ) extends TextualSoQLAnalyzerError[RNS]
  object FromThisWithoutContext {
    private val tag = "soql.analyzer.from-this-without-context"

    implicit def encode[RNS: JsonEncode] = TextualError.simpleEncode[RNS, FromThisWithoutContext](tag, "FROM @this forbidden in a query without an implicit context")
    implicit def decode[RNS: JsonDecode] = TextualError.simpleDecode[RNS, FromThisWithoutContext](tag, FromThisWithoutContext(_))
  }

  case class TableOperationTypeMismatch[+RNS](
    source: Source[RNS],
    left: Seq[TypeName],
    right: Seq[TypeName]
  ) extends TextualSoQLAnalyzerError[RNS]
  object TableOperationTypeMismatch {
    private val tag = "soql.analyzer.table-operation-type-mismatch"

    @AutomaticJsonCodec
    private case class Fields(left: Seq[TypeName], right: Seq[TypeName])

    implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[TableOperationTypeMismatch[RNS]] {
      override val code = tag
      def encode(err: TableOperationTypeMismatch[RNS]) =
        result(Fields(err.left, err.right), s"The left- and right-hand sides of a table operation must have the same schema", err.source)
    }

    implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[TableOperationTypeMismatch[RNS]] {
      override val code = tag
      def decode(v: EncodedError) =
        for {
          fields <- data[Fields](v)
          source <- source[RNS](v)
        } yield {
          TableOperationTypeMismatch(source, fields.left, fields.right)
        }
    }
  }

  case class LiteralNotAllowedInGroupBy[+RNS](
    source: Source[RNS],
  ) extends TextualSoQLAnalyzerError[RNS]
  object LiteralNotAllowedInGroupBy {
    private val tag = "soql.analyzer.literal-not-allowed-in-group-by"

    implicit def encode[RNS: JsonEncode] = TextualError.simpleEncode[RNS, LiteralNotAllowedInGroupBy](tag, "Literals are not allowed in GROUP BY clauses")
    implicit def decode[RNS: JsonDecode] = TextualError.simpleDecode[RNS, LiteralNotAllowedInGroupBy](tag, LiteralNotAllowedInGroupBy(_))
  }

  case class LiteralNotAllowedInOrderBy[+RNS](
    source: Source[RNS],
  ) extends TextualSoQLAnalyzerError[RNS]
  object LiteralNotAllowedInOrderBy {
    private val tag = "soql.analyzer.literal-not-allowed-in-order-by"

    implicit def encode[RNS: JsonEncode] = TextualError.simpleEncode[RNS, LiteralNotAllowedInOrderBy](tag, "Literals are not allowed in ORDER BY clauses")
    implicit def decode[RNS: JsonDecode] = TextualError.simpleDecode[RNS, LiteralNotAllowedInOrderBy](tag, LiteralNotAllowedInOrderBy(_))
  }

  case class LiteralNotAllowedInDistinctOn[+RNS](
    source: Source[RNS],
  ) extends TextualSoQLAnalyzerError[RNS]
  object LiteralNotAllowedInDistinctOn {
    private val tag = "soql.analyzer.literal-not-allowed-in-distinct-on"

    implicit def encode[RNS: JsonEncode] = TextualError.simpleEncode[RNS, LiteralNotAllowedInDistinctOn](tag, "Literals are not allowed in DISTINCT ON clauses")
    implicit def decode[RNS: JsonDecode] = TextualError.simpleDecode[RNS, LiteralNotAllowedInDistinctOn](tag, LiteralNotAllowedInDistinctOn(_))
  }

  case class AggregateFunctionNotAllowed[+RNS](
    source: Source[RNS],
    name: FunctionName
  ) extends TextualSoQLAnalyzerError[RNS]
  object AggregateFunctionNotAllowed {
    private val tag = "soql.analyzer.aggregate-function-not-allowed"

    @AutomaticJsonCodec
    private case class Fields(name: FunctionName)

    implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[AggregateFunctionNotAllowed[RNS]] {
      override val code = tag
      def encode(err: AggregateFunctionNotAllowed[RNS]) =
        result(Fields(err.name), s"Aggregate function ${err.name.name} not allowed here", err.source)
    }

    implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[AggregateFunctionNotAllowed[RNS]] {
      override val code = tag
      def decode(v: EncodedError) =
        for {
          fields <- data[Fields](v)
          source <- source[RNS](v)
        } yield {
          AggregateFunctionNotAllowed(source, fields.name)
        }
    }
  }

  case class UngroupedColumnReference[+RNS](
    source: Source[RNS],
  ) extends TextualSoQLAnalyzerError[RNS]
  object UngroupedColumnReference {
    private val tag = "soql.analyzer.ungrouped-column-reference"

    implicit def encode[RNS: JsonEncode] = TextualError.simpleEncode[RNS, UngroupedColumnReference](tag, "Reference to a column not specified in GROUP BY")
    implicit def decode[RNS: JsonDecode] = TextualError.simpleDecode[RNS, UngroupedColumnReference](tag, UngroupedColumnReference(_))
  }

  case class WindowFunctionNotAllowed[+RNS](
    source: Source[RNS],
    name: FunctionName
  ) extends TextualSoQLAnalyzerError[RNS]
  object WindowFunctionNotAllowed {
    private val tag = "soql.analyzer.window-function-not-allowed"

    @AutomaticJsonCodec
    private case class Fields(name: FunctionName)

    implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[WindowFunctionNotAllowed[RNS]] {
      override val code = tag
      def encode(err: WindowFunctionNotAllowed[RNS]) =
        result(Fields(err.name), s"Window function ${err.name.name} not allowed here", err.source)
    }

    implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[WindowFunctionNotAllowed[RNS]] {
      override val code = tag
      def decode(v: EncodedError) =
        for {
          fields <- data[Fields](v)
          source <- source[RNS](v)
        } yield {
          WindowFunctionNotAllowed(source, fields.name)
        }
    }
  }

  case class ParameterlessTableFunction[+RNS](
    source: Source[RNS],
    name: ResourceName
  ) extends TextualSoQLAnalyzerError[RNS]
  object ParameterlessTableFunction {
    private val tag = "soql.analyzer.parameterless-table-function"

    @AutomaticJsonCodec
    private case class Fields(name: ResourceName)

    implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[ParameterlessTableFunction[RNS]] {
      override val code = tag
      def encode(err: ParameterlessTableFunction[RNS]) =
        result(Fields(err.name), s"UDFs require parameters", err.source)
    }

    implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[ParameterlessTableFunction[RNS]] {
      override val code = tag
      def decode(v: EncodedError) =
        for {
          fields <- data[Fields](v)
          source <- source[RNS](v)
        } yield {
          ParameterlessTableFunction(source, fields.name)
        }
    }
  }

  case class IllegalThisReference[+RNS](
    source: Source[RNS],
  ) extends TextualSoQLAnalyzerError[RNS]
  object IllegalThisReference {
    private val tag = "soql.analyzer.illegal-this-reference"

    implicit def encode[RNS: JsonEncode] = TextualError.simpleEncode[RNS, IllegalThisReference](tag, "@this can only be used a FROM @this, not in joins")
    implicit def decode[RNS: JsonDecode] = TextualError.simpleDecode[RNS, IllegalThisReference](tag, IllegalThisReference(_))
  }

  case class ReservedTableName[+RNS](
    source: Source[RNS],
    name: ResourceName
  ) extends TextualSoQLAnalyzerError[RNS]
  object ReservedTableName {
    private val tag = "soql.analyzer.reserved-table-name"

    @AutomaticJsonCodec
    private case class Fields(name: ResourceName)

    implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[ReservedTableName[RNS]] {
      override val code = tag
      def encode(err: ReservedTableName[RNS]) =
        result(Fields(err.name), s"Table name '${err.name}' is reserved", err.source)
    }

    implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[ReservedTableName[RNS]] {
      override val code = tag
      def decode(v: EncodedError) =
        for {
          fields <- data[Fields](v)
          source <- source[RNS](v)
        } yield {
          ReservedTableName(source, fields.name)
        }
    }
  }

  sealed trait TypecheckError[+RNS] extends SoQLAnalyzerError[RNS] with TextualError[RNS]

  object TypecheckError {
    case class UnorderedOrderBy[+RNS](
      source: Source[RNS],
      typ: TypeName
    ) extends TypecheckError[RNS]
    object UnorderedOrderBy {
      private val tag = "soql.analyzer.typechecker.unordered-order-by"

      @AutomaticJsonCodec
      private case class Fields(typ: TypeName)

      implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[UnorderedOrderBy[RNS]] {
        override val code = tag
        def encode(err: UnorderedOrderBy[RNS]) =
          result(Fields(err.typ), s"Cannot ORDER BY or DISTINCT ON an expression of type ${err.typ}", err.source)
      }

      implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[UnorderedOrderBy[RNS]] {
        override val code = tag
        def decode(v: EncodedError) =
          for {
            fields <- data[Fields](v)
            source <- source[RNS](v)
          } yield {
            UnorderedOrderBy(source, fields.typ)
          }
      }
    }

    case class NoSuchColumn[+RNS](
      source: Source[RNS],
      qualifier: Option[ResourceName],
      name: ColumnName
    ) extends TypecheckError[RNS] with AliasAnalysisError[RNS]
    object NoSuchColumn {
      private val tag = "soql.analyzer.typechecker.no-such-column"

      @AutomaticJsonCodec
      private case class Fields(qualifier: Option[ResourceName], name: ColumnName)

      implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[NoSuchColumn[RNS]] {
        override val code = tag
        def encode(err: NoSuchColumn[RNS]) =
          result(Fields(err.qualifier, err.name), s"No such column ${err.qualifier.fold("")("@" + _ + ".")}${err.name}", err.source)
      }

      implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[NoSuchColumn[RNS]] {
        override val code = tag
        def decode(v: EncodedError) =
          for {
            fields <- data[Fields](v)
            source <- source[RNS](v)
          } yield {
            NoSuchColumn(source, fields.qualifier, fields.name)
          }
      }
    }

    case class UnknownUDFParameter[+RNS](
      source: Source[RNS],
      name: HoleName
    ) extends TypecheckError[RNS]
    object UnknownUDFParameter {
      private val tag = "soql.analyzer.typechecker.unknown-udf-parameter"

      @AutomaticJsonCodec
      private case class Fields(name: HoleName)

      implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[UnknownUDFParameter[RNS]] {
        override val code = tag
        def encode(err: UnknownUDFParameter[RNS]) =
          result(Fields(err.name), s"No such UDF parameter ${err.name}", err.source)
      }

      implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[UnknownUDFParameter[RNS]] {
        override val code = tag
        def decode(v: EncodedError) =
          for {
            fields <- data[Fields](v)
            source <- source[RNS](v)
          } yield {
            UnknownUDFParameter(source, fields.name)
          }
      }
    }

    case class UnknownUserParameter[+RNS](
      source: Source[RNS],
      view: Option[CanonicalName],
      name: HoleName
    ) extends TypecheckError[RNS]
    object UnknownUserParameter {
      private val tag = "soql.analyzer.typechecker.unknown-user-parameter"

      @AutomaticJsonCodec
      private case class Fields(view: Option[CanonicalName], name: HoleName)

      implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[UnknownUserParameter[RNS]] {
        override val code = tag
        def encode(err: UnknownUserParameter[RNS]) =
          result(Fields(err.view, err.name), s"No such user parameter ${err.view.fold("")(_.name + "/")}${err.name}", err.source)
      }

      implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[UnknownUserParameter[RNS]] {
        override val code = tag
        def decode(v: EncodedError) =
          for {
            fields <- data[Fields](v)
            source <- source[RNS](v)
          } yield {
            UnknownUserParameter(source, fields.view, fields.name)
          }
      }
    }

    case class NoSuchFunction[+RNS](
      source: Source[RNS],
      name: FunctionName,
      arity: Int
    ) extends TypecheckError[RNS]
    object NoSuchFunction {
      private val tag = "soql.analyzer.typechecker.no-such-function"

      @AutomaticJsonCodec
      private case class Fields(name: FunctionName, arity: Int)

      implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[NoSuchFunction[RNS]] {
        override val code = tag
        def encode(err: NoSuchFunction[RNS]) =
          result(Fields(err.name, err.arity), s"No such function ${err.name}/${err.name}", err.source)
      }

      implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[NoSuchFunction[RNS]] {
        override val code = tag
        def decode(v: EncodedError) =
          for {
            fields <- data[Fields](v)
            source <- source[RNS](v)
          } yield {
            NoSuchFunction(source, fields.name, fields.arity)
          }
      }
    }

    case class TypeMismatch[+RNS](
      source: Source[RNS],
      expected: Set[TypeName],
      found: TypeName
    ) extends TypecheckError[RNS]
    object TypeMismatch {
      private val tag = "soql.analyzer.typechecker.type-mismatch"

      @AutomaticJsonCodec
      private case class Fields(expected: Set[TypeName], found: TypeName)

      implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[TypeMismatch[RNS]] {
        override val code = tag
        def encode(err: TypeMismatch[RNS]) =
          result(Fields(err.expected, err.found), s"Type mismatch: found ${err.found}", err.source)
      }

      implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[TypeMismatch[RNS]] {
        override val code = tag
        def decode(v: EncodedError) =
          for {
            fields <- data[Fields](v)
            source <- source[RNS](v)
          } yield {
            TypeMismatch(source, fields.expected, fields.found)
          }
      }
    }

    case class RequiresWindow[+RNS](
      source: Source[RNS],
      name: FunctionName
    ) extends TypecheckError[RNS]
    object RequiresWindow {
      private val tag = "soql.analyzer.typechecker.requires-window"

      @AutomaticJsonCodec
      private case class Fields(name: FunctionName)

      implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[RequiresWindow[RNS]] {
        override val code = tag
        def encode(err: RequiresWindow[RNS]) =
          result(Fields(err.name), s"${err.name} requires a window clause", err.source)
      }

      implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[RequiresWindow[RNS]] {
        override val code = tag
        def decode(v: EncodedError) =
          for {
            fields <- data[Fields](v)
            source <- source[RNS](v)
          } yield {
            RequiresWindow(source, fields.name)
          }
      }
    }

    case class IllegalStartFrameBound[+RNS](
      source: Source[RNS],
      bound: String
    ) extends TypecheckError[RNS]
    object IllegalStartFrameBound {
      private val tag = "soql.analyzer.typechecker.illegal-start-frame-bound"

      @AutomaticJsonCodec
      private case class Fields(bound: String)

      implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[IllegalStartFrameBound[RNS]] {
        override val code = tag
        def encode(err: IllegalStartFrameBound[RNS]) =
          result(Fields(err.bound), s"${err.bound} cannot be used as a starting frame bound", err.source)
      }

      implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[IllegalStartFrameBound[RNS]] {
        override val code = tag
        def decode(v: EncodedError) =
          for {
            fields <- data[Fields](v)
            source <- source[RNS](v)
          } yield {
            IllegalStartFrameBound(source, fields.bound)
          }
      }
    }

    case class IllegalEndFrameBound[+RNS](
      source: Source[RNS],
      bound: String
    ) extends TypecheckError[RNS]
    object IllegalEndFrameBound {
      private val tag = "soql.analyzer.typechecker.illegal-end-frame-bound"

      @AutomaticJsonCodec
      private case class Fields(bound: String)

      implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[IllegalEndFrameBound[RNS]] {
        override val code = tag
        def encode(err: IllegalEndFrameBound[RNS]) =
          result(Fields(err.bound), s"${err.bound} cannot be used as a ending frame bound", err.source)
      }

      implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[IllegalEndFrameBound[RNS]] {
        override val code = tag
        def decode(v: EncodedError) =
          for {
            fields <- data[Fields](v)
            source <- source[RNS](v)
          } yield {
            IllegalEndFrameBound(source, fields.bound)
          }
      }
    }

    case class MismatchedFrameBound[+RNS](
      source: Source[RNS],
      start: String,
      end: String
    ) extends TypecheckError[RNS]
    object MismatchedFrameBound {
      private val tag = "soql.analyzer.typechecker.mismatched-frame-bound"

      @AutomaticJsonCodec
      private case class Fields(start: String, end: String)

      implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[MismatchedFrameBound[RNS]] {
        override val code = tag
        def encode(err: MismatchedFrameBound[RNS]) =
          result(Fields(err.start, err.end), s"${err.start} cannot be bollowed by ${err.end}", err.source)
      }

      implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[MismatchedFrameBound[RNS]] {
        override val code = tag
        def decode(v: EncodedError) =
          for {
            fields <- data[Fields](v)
            source <- source[RNS](v)
          } yield {
            MismatchedFrameBound(source, fields.start, fields.end)
          }
      }
    }

    case class NonAggregateFunction[+RNS](
      source: Source[RNS],
      name: FunctionName
    ) extends TypecheckError[RNS]
    object NonAggregateFunction {
      private val tag = "soql.analyzer.typechecker.non-aggregate-function"

      @AutomaticJsonCodec
      private case class Fields(name: FunctionName)

      implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[NonAggregateFunction[RNS]] {
        override val code = tag
        def encode(err: NonAggregateFunction[RNS]) =
          result(Fields(err.name), s"${err.name} is not an aggregate function", err.source)
      }

      implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[NonAggregateFunction[RNS]] {
        override val code = tag
        def decode(v: EncodedError) =
          for {
            fields <- data[Fields](v)
            source <- source[RNS](v)
          } yield {
            NonAggregateFunction(source, fields.name)
          }
      }
    }

    case class NonWindowFunction[+RNS](
      source: Source[RNS],
      name: FunctionName
    ) extends TypecheckError[RNS]
    object NonWindowFunction {
      private val tag = "soql.analyzer.typechecker.non-window-function"

      @AutomaticJsonCodec
      private case class Fields(name: FunctionName)

      implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[NonWindowFunction[RNS]] {
        override val code = tag
        def encode(err: NonWindowFunction[RNS]) =
          result(Fields(err.name), s"${err.name} is not a window function", err.source)
      }

      implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[NonWindowFunction[RNS]] {
        override val code = tag
        def decode(v: EncodedError) =
          for {
            fields <- data[Fields](v)
            source <- source[RNS](v)
          } yield {
            NonWindowFunction(source, fields.name)
          }
      }
    }

    case class DistinctWithOver[+RNS](
      source: Source[RNS]
    ) extends TypecheckError[RNS]
    object DistinctWithOver {
      private val tag = "soql.analyzer.typechecker.distinct-with-over"

      implicit def encode[RNS: JsonEncode] = TextualError.simpleEncode[RNS, DistinctWithOver](tag, "Cannot use DISTINCT with OVER")
      implicit def decode[RNS: JsonDecode] = TextualError.simpleDecode[RNS, DistinctWithOver](tag, DistinctWithOver(_))
    }

    case class GroupsRequiresOrderBy[+RNS](
      source: Source[RNS]
    ) extends TypecheckError[RNS]
    object GroupsRequiresOrderBy {
      private val tag = "soql.analyzer.typechecker.groups-requires-order-by"

      implicit def encode[RNS: JsonEncode] = TextualError.simpleEncode[RNS, GroupsRequiresOrderBy](tag, "GROUPS mode requires an ORDER BY in the window definition")
      implicit def decode[RNS: JsonDecode] = TextualError.simpleDecode[RNS, GroupsRequiresOrderBy](tag, GroupsRequiresOrderBy(_))
    }

    def errorCodecs[RNS : JsonEncode : JsonDecode, T >: TypecheckError[RNS] <: AnyRef](
      codecs: SoQLErrorCodec.ErrorCodecs[T] = new SoQLErrorCodec.ErrorCodecs[T]
    ): SoQLErrorCodec.ErrorCodecs[T] =
      codecs
        .branch[UnorderedOrderBy[RNS]]
        .branch[NoSuchColumn[RNS]]
        .branch[UnknownUDFParameter[RNS]]
        .branch[UnknownUserParameter[RNS]]
        .branch[NoSuchFunction[RNS]]
        .branch[TypeMismatch[RNS]]
        .branch[RequiresWindow[RNS]]
        .branch[IllegalStartFrameBound[RNS]]
        .branch[IllegalEndFrameBound[RNS]]
        .branch[MismatchedFrameBound[RNS]]
        .branch[NonAggregateFunction[RNS]]
        .branch[NonWindowFunction[RNS]]
        .branch[DistinctWithOver[RNS]]
        .branch[GroupsRequiresOrderBy[RNS]]
  }

  sealed trait AliasAnalysisError[+RNS] extends SoQLAnalyzerError[RNS] with TextualError[RNS]
  object AliasAnalysisError {
    case class RepeatedExclusion[+RNS](
      source: Source[RNS],
      name: ColumnName
    ) extends AliasAnalysisError[RNS]
    object RepeatedExclusion {
      private val tag = "soql.analyzer.alias.repeated-exclusion"

      @AutomaticJsonCodec
      private case class Fields(name: ColumnName)

      implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[RepeatedExclusion[RNS]] {
        override val code = tag
        def encode(err: RepeatedExclusion[RNS]) =
          result(Fields(err.name), s"Column ${err.name} has already been excluded", err.source)
      }

      implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[RepeatedExclusion[RNS]] {
        override val code = tag
        def decode(v: EncodedError) =
          for {
            fields <- data[Fields](v)
            source <- source[RNS](v)
          } yield {
            RepeatedExclusion(source, fields.name)
          }
      }
    }

    case class DuplicateAlias[+RNS](
      source: Source[RNS],
      name: ColumnName
    ) extends AliasAnalysisError[RNS]
    object DuplicateAlias {
      private val tag = "soql.analyzer.alias.duplicate-alias"

      @AutomaticJsonCodec
      private case class Fields(name: ColumnName)

      implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[DuplicateAlias[RNS]] {
        override val code = tag
        def encode(err: DuplicateAlias[RNS]) =
          result(Fields(err.name), s"There is already a column named ${err.name} selected", err.source)
      }

      implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[DuplicateAlias[RNS]] {
        override val code = tag
        def decode(v: EncodedError) =
          for {
            fields <- data[Fields](v)
            source <- source[RNS](v)
          } yield {
            DuplicateAlias(source, fields.name)
          }
      }
    }

    case class CircularAliasDefinition[+RNS](
      source: Source[RNS],
      name: ColumnName
    ) extends AliasAnalysisError[RNS]
    object CircularAliasDefinition {
      private val tag = "soql.analyzer.alias.circular-alias-definition"

      @AutomaticJsonCodec
      private case class Fields(name: ColumnName)

      implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[CircularAliasDefinition[RNS]] {
        override val code = tag
        def encode(err: CircularAliasDefinition[RNS]) =
          result(Fields(err.name), s"Circular reference while defining alias ${err.name}", err.source)
      }

      implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[CircularAliasDefinition[RNS]] {
        override val code = tag
        def decode(v: EncodedError) =
          for {
            fields <- data[Fields](v)
            source <- source[RNS](v)
          } yield {
            CircularAliasDefinition(source, fields.name)
          }
      }
    }

    private[SoQLAnalyzerError] def errorCodecsMinusNoSuchColumn[RNS : JsonEncode : JsonDecode, T >: AliasAnalysisError[RNS] <: AnyRef](
      codecs: SoQLErrorCodec.ErrorCodecs[T]
    ): SoQLErrorCodec.ErrorCodecs[T] =
      codecs
        .branch[RepeatedExclusion[RNS]]
        .branch[DuplicateAlias[RNS]]
        .branch[CircularAliasDefinition[RNS]]

    def errorCodecs[RNS : JsonEncode : JsonDecode, T >: AliasAnalysisError[RNS] <: AnyRef](
      codecs: SoQLErrorCodec.ErrorCodecs[T] = new SoQLErrorCodec.ErrorCodecs[T]
    ): SoQLErrorCodec.ErrorCodecs[T] =
      errorCodecsMinusNoSuchColumn[RNS, T](codecs)
        .branch[TypecheckError.NoSuchColumn[RNS]]
  }

  def errorCodecs[RNS : JsonEncode : JsonDecode, T >: SoQLAnalyzerError[RNS] <: AnyRef](
    codecs: SoQLErrorCodec.ErrorCodecs[T] = new SoQLErrorCodec.ErrorCodecs[T]
  ): SoQLErrorCodec.ErrorCodecs[T] =
    AliasAnalysisError.errorCodecsMinusNoSuchColumn[RNS, T](TypecheckError.errorCodecs[RNS, T](codecs))
      .branch[InvalidParameterType]
      .branch[ExpectedBoolean[RNS]]
      .branch[IncorrectNumberOfUdfParameters[RNS]]
      .branch[DistinctOnNotPrefixOfOrderBy[RNS]]
      .branch[OrderByMustBeSelectedWhenDistinct[RNS]]
      .branch[InvalidGroupBy[RNS]]
      .branch[ParametersForNonUDF[RNS]]
      .branch[TableAliasAlreadyExists[RNS]]
      .branch[FromRequired[RNS]]
      .branch[FromForbidden[RNS]]
      .branch[FromThisWithoutContext[RNS]]
      .branch[TableOperationTypeMismatch[RNS]]
      .branch[LiteralNotAllowedInGroupBy[RNS]]
      .branch[LiteralNotAllowedInOrderBy[RNS]]
      .branch[LiteralNotAllowedInDistinctOn[RNS]]
      .branch[AggregateFunctionNotAllowed[RNS]]
      .branch[UngroupedColumnReference[RNS]]
      .branch[WindowFunctionNotAllowed[RNS]]
      .branch[ParameterlessTableFunction[RNS]]
      .branch[IllegalThisReference[RNS]]
      .branch[ReservedTableName[RNS]]
}
