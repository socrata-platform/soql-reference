package com.socrata.soql.analysis

import com.socrata.soql.FunctionName

case class MonomorphicFunction[Type](function: Function[Type], bindings: Map[String, Type]) {
  def this(name: FunctionName, parameters: Seq[Type], result: Type) = this(Function(name, parameters.map(FixedType(_)), FixedType(result)), Map.empty)

  require(bindings.keySet == function.parameters.collect { case VariableType(n) => n }.toSet, "bindings do not match")

  def name: FunctionName = function.name
  lazy val parameters: Seq[Type] = function.parameters.map(bind)
  def arity = function.arity
  def result: Type = bind(function.result)

  private def bind(typeLike: TypeLike[Type]) = typeLike match {
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
