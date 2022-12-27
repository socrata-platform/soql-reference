package com.socrata.soql.analyzer2

import scala.annotation.tailrec
import scala.util.parsing.input.Position

import com.rojoma.json.v3.util.{SimpleHierarchyCodecBuilder, AutomaticJsonEncodeBuilder, AutomaticJsonDecodeBuilder, InternalTag}

import com.socrata.soql.collection.CovariantSet
import com.socrata.soql.environment.{ColumnName, ResourceName, HoleName, FunctionName, TypeName}
import com.socrata.soql.parsing.RecursiveDescentParser

sealed abstract class SoQLAnalyzerError[+RNS](msg: String)

object SoQLAnalyzerError {
  sealed abstract class NonTextualError(msg: String) extends SoQLAnalyzerError[Nothing](msg)

  sealed abstract class ParameterError(msg: String) extends NonTextualError(msg)
  case class InvalidParameterType(
    canonicalName: Option[CanonicalName],
    param: HoleName,
    expected: TypeName,
    got: TypeName
  ) extends ParameterError(s"Invalid parameter value for ${canonicalName.fold("")(_.name + "/")}${param}: expected ${expected} but got ${got}")

  sealed abstract class Payload(val msg: String)

  case class TextualError[+RNS, +Data <: Payload](scope: RNS, canonicalName: Option[CanonicalName], position: Position, data: Data) extends SoQLAnalyzerError[RNS](data.msg)

  sealed abstract class ParserError(msg: String) extends Payload(msg)

  object ParserError {
    case class UnexpectedEscape(char: Char) extends ParserError("Unexpected escape character")
    case class BadUnicodeEscapeCharacter(char: Char) extends ParserError("Bad character in unicode escape")
    case class UnicodeCharacterOutOfRange(value: Int) extends ParserError("Unicode character out of range")
    case class UnexpectedCharacter(char: Char) extends ParserError("Unicode character out of range")
    case object UnexpectedEOF extends ParserError("Unexpected end of input")
    case object UnterminatedString extends ParserError("Unterminated string")

    case class ExpectedToken(
      expectations: Seq[String],
      got: String
    ) extends ParserError(RecursiveDescentParser.expectationStringsToEnglish(expectations, got))
    case object ExpectedLeafQuery extends ParserError("Expected a non-compound query on the right side of a pipe operator")
    case object UnexpectedStarSelect extends ParserError("Star selections must come at the start of the select-list")
    case object UnexpectedSystemStarSelect extends ParserError("System column star selections must come before user column star selections")
  }

  sealed abstract class AnalysisError(msg: String) extends Payload(msg)
  object AnalysisError {
    case class ExpectedBoolean(
      got: TypeName
    ) extends AnalysisError("Expected boolean, but got ${got}")

    case class IncorrectNumberOfUdfParameters(
      udf: ResourceName,
      expected: Int,
      got: Int
    ) extends AnalysisError(s"UDF expected ${expected} parameter(s) but got ${got}")

    case object DistinctNotPrefixOfOrderBy extends AnalysisError("When both DISTINCT ON and ORDER BY are present, the DISTINCT BY's expression list must be a prefix of the ORDER BY")

    case object OrderByMustBeSelectedWhenDistinct extends AnalysisError("When both DISTINCT and ORDER BY are present, all columns in ORDER BY must also be selected")

    case class InvalidGroupBy(typ: TypeName) extends AnalysisError(s"Cannot GROUP BY an expression of type ${typ}")

    case class UnorderedOrderBy(typ: TypeName) extends AnalysisError(s"Cannot ORDER BY or DISTINCT ON an expression of type ${typ}")

    case class ParametersForNonUDF(nonUdf: ResourceName) extends AnalysisError("Cannot provide parameters to a non-UDF")

    case class TableAliasAlreadyExists(alias: ResourceName) extends AnalysisError(s"Table alias ${alias} already exists")

    case object FromRequired extends AnalysisError("FROM required in a query without an implicit context")

    case object FromForbidden extends AnalysisError("FROM (other than FROM @this) forbidden in a query with an implicit context")

    case object FromThisWithoutContext extends AnalysisError("FROM @this cannot be used in a query without an implicit context")

    case class TableOperationTypeMismatch(left: Seq[TypeName], right: Seq[TypeName]) extends AnalysisError("The left- and right-hand sides of a table operation must have the same schema")

    case object LiteralNotAllowedInGroupBy extends AnalysisError("Literal values are not allowed in GROUP BY")

    case object LiteralNotAllowedInOrderBy extends AnalysisError("Literal values are not allowed in ORDER BY")

    case object LiteralNotAllowedInDistinctOn extends AnalysisError("Literal values are not allowed in DISTINCT ON")

    case class AggregateFunctionNotAllowed(name: FunctionName) extends AnalysisError("Aggregate function not allowed here")

    case object UngroupedColumnReference extends AnalysisError("Reference to a column not specified in GROUP BY")

    case class WindowFunctionNotAllowed(name: FunctionName) extends AnalysisError("Window function not allowed here")

    case class ParameterlessTableFunction(name: ResourceName) extends AnalysisError("UDFs require parameters")

    case object IllegalThisReference extends AnalysisError("@this can only be used as `FROM @this`, not in joins")

    case class ReservedTableName(name: ResourceName) extends AnalysisError("Table name '$name' is reserved")

    sealed trait TypecheckError extends AnalysisError

    object TypecheckError {
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

      case object GroupsRequiresOrderBy extends AnalysisError(s"GROUPS mode requires and ORDER BY in the window definition") with TypecheckError
    }

    sealed trait AliasAnalysisError extends AnalysisError
    object AliasAnalysisError {
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
}
