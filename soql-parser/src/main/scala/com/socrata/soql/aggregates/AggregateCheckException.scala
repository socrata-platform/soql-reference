package com.socrata.soql.aggregates

import scala.util.parsing.input.Position

import com.socrata.soql.SoQLException
import com.socrata.soql.names.{ColumnName, FunctionName}

class AggregateCheckException(msg: String, pos: Position) extends SoQLException(msg, pos)
class AggregateInUngroupedContext(val function: FunctionName, clause: String, position: Position) extends AggregateCheckException("Cannot use aggregate function `" + function + "' in " + clause, position)
class ColumnNotInGroupBys(val column: ColumnName, position: Position) extends AggregateCheckException("Column `" + column + "' not in group bys", position)

