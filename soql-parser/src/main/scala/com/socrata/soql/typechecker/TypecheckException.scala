package com.socrata.soql.typechecker

import scala.util.parsing.input.Position

import com.socrata.soql.names.{TypeName, ColumnName, FunctionName}
import com.socrata.soql.SoQLException

class TypecheckException(msg: String, position: Position) extends SoQLException(msg, position)
class UnknownColumn(val col: ColumnName, p: Position) extends TypecheckException("Unknown column `" + col + "'", p)
class NoSuchFunction(val name: FunctionName, p: Position) extends TypecheckException("No such function `" + name + "'", p)
class UnknownType(val name: TypeName, p: Position) extends TypecheckException("Unknown type `" + name + "'", p)
class ImpossibleCast(val from: TypeName, to: TypeName, p: Position) extends TypecheckException("Cannot convert a value of type `"+ from + "' to `" + to + "'" , p)
class TypeMismatchError(val name: FunctionName, val actual: TypeName, position: Position) extends TypecheckException("Cannot pass a value of type `" + actual + "' to function `" + name + "'", position)
class AmbiguousCall(val name: FunctionName, position: Position) extends TypecheckException("Ambiguous call to `" + name + "'", position)
