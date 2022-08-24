package com.socrata.soql.analyzer2

import scala.util.parsing.input.Position

import com.socrata.soql.collection.CovariantSet
import com.socrata.soql.environment.{ColumnName, ResourceName, HoleName, FunctionName}

sealed abstract class SoQLAnalyzerError[+RNS, +CT](msg: String) {
  def this() = this("")

  val scope: RNS
  val canonicalName: Option[CanonicalName]
  val position: Position
}

object SoQLAnalyzerError {
  case class ExpectedBoolean[+RNS, +CT](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    got: CT,
    position: Position
  ) extends SoQLAnalyzerError[RNS, CT]

  case class IncorrectNumberOfUdfParameters[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    udf: ResourceName,
    expected: Int,
    got: Int,
    position: Position
  ) extends SoQLAnalyzerError[RNS, Nothing]

  case class DistinctNotPrefixOfOrderBy[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    position: Position
  ) extends SoQLAnalyzerError[RNS, Nothing]

  case class InvalidGroupBy[+RNS, +CT](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    typ: CT,
    position: Position
  ) extends SoQLAnalyzerError[RNS, CT]

  case class UnorderedOrderBy[+RNS, +CT](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    typ: CT,
    position: Position
  ) extends SoQLAnalyzerError[RNS, CT]

  case class ParametersForNonUDF[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    nonUdf: ResourceName,
    position: Position
  ) extends SoQLAnalyzerError[RNS, Nothing]

  case class AliasAlreadyExists[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    alias: ResourceName,
    position: Position
  ) extends SoQLAnalyzerError[RNS, Nothing]

  case class FromRequired[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    position: Position
  ) extends SoQLAnalyzerError[RNS, Nothing]

  case class FromForbidden[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    position: Position
  ) extends SoQLAnalyzerError[RNS, Nothing]

  case class FromThisWithoutContext[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    position: Position
  ) extends SoQLAnalyzerError[RNS, Nothing]

  case class TableOperationTypeMismatch[+RNS, +CT](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    left: Seq[CT],
    right: Seq[CT],
    position: Position
  ) extends SoQLAnalyzerError[RNS, Nothing]

  case class LiteralNotAllowedInGroupBy[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    position: Position
  ) extends SoQLAnalyzerError[RNS, Nothing]

  case class LiteralNotAllowedInOrderBy[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    position: Position
  ) extends SoQLAnalyzerError[RNS, Nothing]

  case class LiteralNotAllowedInDistinctOn[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    position: Position
  ) extends SoQLAnalyzerError[RNS, Nothing]

  case class AggregateFunctionNotAllowed[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    name: FunctionName,
    position: Position
  ) extends SoQLAnalyzerError[RNS, Nothing]

  case class UngroupedColumnReference[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    position: Position
  ) extends SoQLAnalyzerError[RNS, Nothing]

  case class WindowFunctionNotAllowed[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    name: FunctionName,
    position: Position
  ) extends SoQLAnalyzerError[RNS, Nothing]

  case class ParameterlessTableFunction[+RNS](
    scope: RNS,
    canonicalName: Option[CanonicalName],
    name: ResourceName,
    position: Position
  ) extends SoQLAnalyzerError[RNS, Nothing]

  sealed trait TypecheckError[+RNS, +CT] extends SoQLAnalyzerError[RNS, CT]
  object TypecheckError {
    case class NoSuchColumn[+RNS](
      scope: RNS,
      canonicalName: Option[CanonicalName],
      qualifier: Option[ResourceName],
      name: ColumnName,
      position: Position
    ) extends SoQLAnalyzerError[RNS, Nothing](s"No such column ${name.name}") with TypecheckError[RNS, Nothing] with AliasAnalysisError[RNS]

    case class UnknownUDFParameter[+RNS](
      scope: RNS,
      canonicalName: Option[CanonicalName],
      name: HoleName,
      position: Position
    ) extends SoQLAnalyzerError[RNS, Nothing](s"No such UDF parameter ${name.name}") with TypecheckError[RNS, Nothing]

    case class UnknownUserParameter[+RNS](
      scope: RNS,
      canonicalName: Option[CanonicalName],
      view: Option[CanonicalName],
      name: HoleName,
      position: Position
    ) extends SoQLAnalyzerError[RNS, Nothing](s"No such user parameter ${view.fold("")(_.name + "/")}${name.name}") with TypecheckError[RNS, Nothing]

    case class NoSuchFunction[+RNS](
      scope: RNS,
      canonicalName: Option[CanonicalName],
      name: FunctionName,
      arity: Int,
      position: Position
    ) extends SoQLAnalyzerError[RNS, Nothing](s"No such function ${name}/${arity}") with TypecheckError[RNS, Nothing]

    case class TypeMismatch[+RNS, +CT](
      scope: RNS,
      canonicalName: Option[CanonicalName],
      expected: CovariantSet[CT],
      found: CT,
      position: Position
    ) extends SoQLAnalyzerError[RNS, CT](s"Type mismatch: found ${found}") with TypecheckError[RNS, CT]

    case class RequiresWindow[+RNS](
      scope: RNS,
      canonicalName: Option[CanonicalName],
      name: FunctionName,
      position: Position
    ) extends SoQLAnalyzerError[RNS, Nothing](s"${name.name} requires a window clause") with TypecheckError[RNS, Nothing]

    case class NonAggregate[+RNS](
      scope: RNS,
      canonicalName: Option[CanonicalName],
      name: FunctionName,
      position: Position
    ) extends SoQLAnalyzerError[RNS, Nothing](s"${name.name} is not an aggregate function") with TypecheckError[RNS, Nothing]

    case class NonWindowFunction[+RNS](
      scope: RNS,
      canonicalName: Option[CanonicalName],
      name: FunctionName,
      position: Position
    ) extends SoQLAnalyzerError[RNS, Nothing](s"${name.name} is not a window function") with TypecheckError[RNS, Nothing]

    case class GroupsRequiresOrderBy[+RNS](
      scope: RNS,
      canonicalName: Option[CanonicalName],
      position: Position
    ) extends SoQLAnalyzerError[RNS, Nothing](s"GROUPS mode requires and ORDER BY in the window definition") with TypecheckError[RNS, Nothing]
  }

  sealed trait AliasAnalysisError[+RNS] extends SoQLAnalyzerError[RNS, Nothing]
  object AliasAnalysisError {
    case class RepeatedExclusion[+RNS](
      scope: RNS,
      canonicalName: Option[CanonicalName],
      name: ColumnName,
      position: Position
    ) extends SoQLAnalyzerError[RNS, Nothing]("Column `" + name + "' has already been excluded") with AliasAnalysisError[RNS]

    case class DuplicateAlias[+RNS](
      scope: RNS,
      canonicalName: Option[CanonicalName],
      name: ColumnName,
      position: Position
    ) extends SoQLAnalyzerError[RNS, Nothing]("There is already a column named `" + name + "' selected") with AliasAnalysisError[RNS]

    case class CircularAliasDefinition[+RNS](
      scope: RNS,
      canonicalName: Option[CanonicalName],
      name: ColumnName,
      position: Position
    ) extends SoQLAnalyzerError[RNS, Nothing]("Circular reference while defining alias `" + name + "'") with AliasAnalysisError[RNS]
  }
}
