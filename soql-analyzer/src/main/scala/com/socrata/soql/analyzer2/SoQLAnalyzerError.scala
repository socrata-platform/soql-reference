package com.socrata.soql.analyzer2

import scala.util.parsing.input.Position

import com.socrata.soql.collection.CovariantSet
import com.socrata.soql.environment.{ColumnName, ResourceName, HoleName, FunctionName, TypeName}

sealed abstract class SoQLAnalyzerError[+RNS](msg: String)

object SoQLAnalyzerError {
  sealed abstract class ParameterError(msg: String) extends SoQLAnalyzerError[Nothing](msg)
  case class InvalidParameterType(canonicalName: Option[CanonicalName], param: HoleName, expected: TypeName, got: TypeName) extends ParameterError("")

  sealed abstract class AnalysisError[+RNS](msg: String) extends SoQLAnalyzerError[RNS](msg) {
    def this() = this("")

    val scope: RNS
    val canonicalName: Option[CanonicalName]
    val position: Position
  }

  case class ExpectedBoolean[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    got: TypeName,
    position: Position
  ) extends AnalysisError[RNS]

  case class IncorrectNumberOfUdfParameters[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    udf: ResourceName,
    expected: Int,
    got: Int,
    position: Position
  ) extends AnalysisError[RNS]

  case class DistinctNotPrefixOfOrderBy[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    position: Position
  ) extends AnalysisError[RNS]

  case class InvalidGroupBy[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    typ: TypeName,
    position: Position
  ) extends AnalysisError[RNS]

  case class UnorderedOrderBy[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    typ: TypeName,
    position: Position
  ) extends AnalysisError[RNS]

  case class ParametersForNonUDF[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    nonUdf: ResourceName,
    position: Position
  ) extends AnalysisError[RNS]

  case class AliasAlreadyExists[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    alias: ResourceName,
    position: Position
  ) extends AnalysisError[RNS]

  case class FromRequired[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    position: Position
  ) extends AnalysisError[RNS]

  case class FromForbidden[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    position: Position
  ) extends AnalysisError[RNS]

  case class FromThisWithoutContext[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    position: Position
  ) extends AnalysisError[RNS]

  case class TableOperationTypeMismatch[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    left: Seq[TypeName],
    right: Seq[TypeName],
    position: Position
  ) extends AnalysisError[RNS]

  case class LiteralNotAllowedInGroupBy[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    position: Position
  ) extends AnalysisError[RNS]

  case class LiteralNotAllowedInOrderBy[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    position: Position
  ) extends AnalysisError[RNS]

  case class LiteralNotAllowedInDistinctOn[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    position: Position
  ) extends AnalysisError[RNS]

  case class AggregateFunctionNotAllowed[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    name: FunctionName,
    position: Position
  ) extends AnalysisError[RNS]

  case class UngroupedColumnReference[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    position: Position
  ) extends AnalysisError[RNS]

  case class WindowFunctionNotAllowed[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    name: FunctionName,
    position: Position
  ) extends AnalysisError[RNS]

  case class ParameterlessTableFunction[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    name: ResourceName,
    position: Position
  ) extends AnalysisError[RNS]

  case class IllegalThisReference[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    position: Position
  ) extends AnalysisError[RNS]

  case class ReservedTableName[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    name: ResourceName,
    position: Position
  ) extends AnalysisError[RNS]

  sealed trait TypecheckError[+RNS] extends AnalysisError[RNS]
  object TypecheckError {
    case class NoSuchColumn[+RNS](
      scope: RNS,
      canonicalName: Option[CanonicalName],
      qualifier: Option[ResourceName],
      name: ColumnName,
      position: Position
    ) extends AnalysisError[RNS](s"No such column ${name.name}") with TypecheckError[RNS] with AliasAnalysisError[RNS]

    case class UnknownUDFParameter[+RNS](
      scope: RNS,
      canonicalName: Option[CanonicalName],
      name: HoleName,
      position: Position
    ) extends AnalysisError[RNS](s"No such UDF parameter ${name.name}") with TypecheckError[RNS]

    case class UnknownUserParameter[+RNS](
      scope: RNS,
      canonicalName: Option[CanonicalName],
      view: Option[CanonicalName],
      name: HoleName,
      position: Position
    ) extends AnalysisError[RNS](s"No such user parameter ${view.fold("")(_.name + "/")}${name.name}") with TypecheckError[RNS]

    case class NoSuchFunction[+RNS](
      scope: RNS,
      canonicalName: Option[CanonicalName],
      name: FunctionName,
      arity: Int,
      position: Position
    ) extends AnalysisError[RNS](s"No such function ${name}/${arity}") with TypecheckError[RNS]

    case class TypeMismatch[+RNS](
      scope: RNS,
      canonicalName: Option[CanonicalName],
      expected: Set[TypeName],
      found: TypeName,
      position: Position
    ) extends AnalysisError[RNS](s"Type mismatch: found ${found}") with TypecheckError[RNS]

    case class RequiresWindow[+RNS](
      scope: RNS,
      canonicalName: Option[CanonicalName],
      name: FunctionName,
      position: Position
    ) extends AnalysisError[RNS](s"${name.name} requires a window clause") with TypecheckError[RNS]

    case class NonAggregate[+RNS](
      scope: RNS,
      canonicalName: Option[CanonicalName],
      name: FunctionName,
      position: Position
    ) extends AnalysisError[RNS](s"${name.name} is not an aggregate function") with TypecheckError[RNS]

    case class NonWindowFunction[+RNS](
      scope: RNS,
      canonicalName: Option[CanonicalName],
      name: FunctionName,
      position: Position
    ) extends AnalysisError[RNS](s"${name.name} is not a window function") with TypecheckError[RNS]

    case class GroupsRequiresOrderBy[+RNS](
      scope: RNS,
      canonicalName: Option[CanonicalName],
      position: Position
    ) extends AnalysisError[RNS](s"GROUPS mode requires and ORDER BY in the window definition") with TypecheckError[RNS]
  }

  sealed trait AliasAnalysisError[+RNS] extends AnalysisError[RNS]
  object AliasAnalysisError {
    case class RepeatedExclusion[+RNS](
      scope: RNS,
      canonicalName: Option[CanonicalName],
      name: ColumnName,
      position: Position
    ) extends AnalysisError[RNS]("Column `" + name + "' has already been excluded") with AliasAnalysisError[RNS]

    case class DuplicateAlias[+RNS](
      scope: RNS,
      canonicalName: Option[CanonicalName],
      name: ColumnName,
      position: Position
    ) extends AnalysisError[RNS]("There is already a column named `" + name + "' selected") with AliasAnalysisError[RNS]

    case class CircularAliasDefinition[+RNS](
      scope: RNS,
      canonicalName: Option[CanonicalName],
      name: ColumnName,
      position: Position
    ) extends AnalysisError[RNS]("Circular reference while defining alias `" + name + "'") with AliasAnalysisError[RNS]
  }
}
