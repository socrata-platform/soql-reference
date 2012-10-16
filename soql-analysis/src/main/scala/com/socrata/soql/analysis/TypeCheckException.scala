package com.socrata.soql.analysis

import util.parsing.input.Position
import com.socrata.soql.names.FunctionName

class TypeCheckException(msg: String, val position: Position) extends Exception(msg)
class UnknownColumn(p: Position) extends TypeCheckException("Unknown column", p)
class NoSuchFunction(val name: FunctionName, p: Position) extends TypeCheckException("No such function " + name, p)
class UnknownType(p: Position) extends TypeCheckException("Unknown type", p)
class IncompatibleType(p: Position) extends TypeCheckException("Incompatible type", p)
