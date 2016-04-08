package com.socrata.soql.typechecker

import com.socrata.soql.ast._
import com.socrata.soql.environment.{FunctionName, DatasetContext}

/**
 * This class rewrites subscript functions to functions that access subtype columns before type check
 * because soql cannot directly use property subscript syntax (location.prop) to access sub-columns of different types.
 * The names of these functions must be type_subColumnType by convention.
 */
class SubscriptConverter[Type](typeInfo: TypeInfo[Type], functionInfo: FunctionInfo[Type])
                              (implicit ctx: DatasetContext[Type])
  extends (Expression => Expression) { self =>

  val columns = ctx.schema

  def apply(e: Expression) = convert(e)

  private def convert(e: Expression): Expression = e match {
    case c@ColumnRef(_) => c
    case c@ColumnOrAliasRef(_) => c
    case l: Literal => l
    case fc@FunctionCall(SpecialFunctions.Subscript,
                         params@Seq(ColumnOrAliasRef(columnName), StringLiteral(prop))) =>
      columns.get(columnName) match {
        case Some(t) =>
          val typeName = typeInfo.typeNameFor(t)
          // By convention, sub-column function must be named type_subcolumn
          val fnName = FunctionName(s"${typeName}_$prop")
          val subColumnFn = functionInfo.functionsWithArity(fnName, 1)
          // Require exactly ONE sub-column function
          if (subColumnFn.size == 1) {
            fc.copy(functionName = fnName, parameters = params.take(1))(fc.position, fc.functionNamePosition)
          } else {
            convertFunctionCall(fc)
          }
        case _ =>
          convertFunctionCall(fc)
      }
    case fc@FunctionCall(_, _) =>
      convertFunctionCall(fc)
  }

  private def convertFunctionCall(fc: FunctionCall): Expression = {
    fc.copy(parameters = fc.parameters.map(convert(_)))(fc.position, fc.functionNamePosition)
  }
}
