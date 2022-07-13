package com.socrata.soql.typechecker

import com.socrata.soql.ast.{Hint, Materialized, NoChainMerge, NoRollup}
import com.socrata.soql.environment.{ColumnName, TableName}
import com.socrata.soql.exceptions.TypecheckException
import com.socrata.soql.typed

trait HintTypechecker[Type, Value] { this: Typechecker[Type, Value] =>

  type THint = typed.Hint[ColumnName, Type]

  def typecheckHint(hint: Hint, aliases: Map[ColumnName, Expr], from: Option[TableName]): THint = {
    typecheck(hint) match {
      case Left(tm) => throw tm
      case Right(h) => h
    }
  }

  private def typecheck(e: Hint): Either[TypecheckException, THint] = e match {
    case Materialized(pos) =>
      Right(typed.Materialized(pos))
    case NoRollup(pos) =>
      Right(typed.NoRollup(pos))
    case NoChainMerge(pos) =>
      Right(typed.NoChainMerge(pos))
  }
}
