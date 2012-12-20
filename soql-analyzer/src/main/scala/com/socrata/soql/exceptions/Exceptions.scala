package com.socrata.soql.exceptions

import scala.util.parsing.input.Position

import com.socrata.soql.environment.{TypeName, FunctionName, ColumnName}

sealed abstract class SoQLException(m: String, p: Position) extends RuntimeException(m + ":\n" + p.longString) {
  def position: Position
}

sealed trait AggregateCheckException extends SoQLException
case class AggregateInUngroupedContext(function: FunctionName, clause: String, position: Position) extends SoQLException("Cannot use aggregate function `" + function + "' in " + clause, position) with AggregateCheckException
case class ColumnNotInGroupBys(column: ColumnName, position: Position) extends SoQLException("Column `" + column + "' not in group bys", position) with AggregateCheckException

sealed trait AliasAnalysisException extends SoQLException
case class RepeatedException(name: ColumnName, position: Position) extends SoQLException("Column `" + name + "' has already been excluded", position) with AliasAnalysisException
case class DuplicateAlias(name: ColumnName, position: Position) extends SoQLException("There is already a column named `" + name + "' selected", position) with AliasAnalysisException
case class NoSuchColumn(name: ColumnName, position: Position) extends SoQLException("No such column `" + name + "'", position) with AliasAnalysisException with TypecheckException
case class CircularAliasDefinition(name: ColumnName, position: Position) extends SoQLException("Circular reference while defining alias `" + name + "'", position) with AliasAnalysisException

sealed trait LexerException extends SoQLException
case class UnexpectedEscape(char: Char, position: Position) extends SoQLException("Unexpected escape character", position) with LexerException
case class BadUnicodeEscapeCharacter(char: Char, position: Position) extends SoQLException("Bad character in unicode escape", position) with LexerException
case class UnicodeCharacterOutOfRange(value: Int, position:Position) extends SoQLException("Unicode character out of range", position) with LexerException
case class UnexpectedCharacter(char: Char, position: Position) extends SoQLException("Unexpected character", position) with LexerException
case class UnexpectedEOF(position: Position) extends SoQLException("Unexpected end of input", position) with LexerException
case class UnterminatedString(position: Position) extends SoQLException("Unterminated string", position) with LexerException

case class BadParse(message: String, position: Position) extends SoQLException(message, position)

sealed trait TypecheckException extends SoQLException
case class NoSuchFunction(name: FunctionName, arity: Int, position: Position) extends SoQLException("No such function `" + name + "/" + arity + "'", position) with TypecheckException
case class TypeMismatch(name: FunctionName, actual: TypeName, position: Position) extends SoQLException("Cannot pass a value of type `" + actual + "' to function `" + name + "'", position) with TypecheckException
case class AmbiguousCall(name: FunctionName, position: Position) extends SoQLException("Ambiguous call to `" + name + "'", position) with TypecheckException

