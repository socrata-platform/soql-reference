package com.socrata.soql.functions

import com.socrata.soql.environment.FunctionName

case class MonomorphicFunction[Type](function: Function[Type], bindings: Map[String, Type]) {
  def this(identity: String, name: FunctionName, parameters: Seq[Type], repeated: Seq[Type], result: Type, isAggregate: Boolean = false, needsWindow: Boolean = false)(documentation: String, examples: Example*) =
    this(Function(
      identity,
      name,
      Map.empty,
      parameters.map(FixedType(_)),
      repeated.map(FixedType(_)),
      FixedType(result),
      isAggregate,
      needsWindow,
      documentation,
      examples
    ), Map.empty)

  val bindingsLessWildcards = bindings.keySet.diff(function.wildcards)
  require(bindingsLessWildcards == function.typeParametersLessWildcards, "bindings do not match")

  def name: FunctionName = function.name
  lazy val parameters: Seq[Type] = function.parameters.map(bind)
  lazy val repeated = function.repeated.map(bind)
  def allParameters =
    if (repeated.isEmpty) parameters else parameters.toStream ++ Stream.continually(repeated).flatten

  def minArity = function.minArity
  def isVariadic = function.isVariadic
  def result: Type = bind(function.result)
  def isAggregate = function.isAggregate
  def needsWindow = function.needsWindow

  private def bind[B >: Type](typeLike: TypeLike[B]) = typeLike match {
    case FixedType(t) => t
    case VariableType(n) => bindings(n)
  }

  override def toString = {
    val sb = new StringBuilder(name.toString).append(" :: ")
    sb.append(parameters.mkString("", " -> ", " -> "))
    sb.append(result)
    sb.toString
  }
}
