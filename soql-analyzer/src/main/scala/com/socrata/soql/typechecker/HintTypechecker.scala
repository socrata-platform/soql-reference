package com.socrata.soql.typechecker

import com.socrata.soql.ast.{Hint, Materialized}
import com.socrata.soql.environment.{ColumnName, TableName}
import com.socrata.soql.exceptions.TypecheckException
import com.socrata.soql.typed

trait HintTypechecker[Type] { this: Typechecker[Type] =>

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
  }
}
