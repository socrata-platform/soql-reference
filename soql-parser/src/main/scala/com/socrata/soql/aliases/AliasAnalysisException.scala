package com.socrata.soql.aliases

import scala.util.parsing.input.Position

import com.socrata.soql.SoQLException
import com.socrata.soql.names.ColumnName

class AliasAnalysisException(msg: String, pos: Position) extends SoQLException(msg, pos)
class RepeatedExceptionException(val name: ColumnName, position: Position) extends AliasAnalysisException("Column `" + name + "' has already been excluded", position)
class DuplicateAliasException(val name: ColumnName, position: Position) extends AliasAnalysisException("There is already a column named `" + name + "' selected", position)
class NoSuchColumnException(val name: ColumnName, position: Position) extends AliasAnalysisException("No such column `" + name + "'", position)
class CircularAliasDefinitionException(val name: ColumnName, position: Position) extends AliasAnalysisException("Circular reference while defining alias `" + name + "'", position)
