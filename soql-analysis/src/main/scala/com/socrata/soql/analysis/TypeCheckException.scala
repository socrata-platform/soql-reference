package com.socrata.soql.analysis

import util.parsing.input.Position
import com.socrata.soql.names.{TypeName, ColumnName, FunctionName}

class TypeCheckException(msg: String, val position: Position) extends Exception(msg)
class UnknownColumn(val col: ColumnName, p: Position) extends TypeCheckException("Unknown column " + col + "\n" + p.longString, p)
class NoSuchFunction(val name: FunctionName, p: Position) extends TypeCheckException("No such function " + name + "\n" + p.longString, p)
class UnknownType(val name: TypeName, p: Position) extends TypeCheckException("Unknown type " + name + "\n" + p.longString, p)
class IncompatibleType(p: Position) extends TypeCheckException("Incompatible type\n" + p.longString, p)
