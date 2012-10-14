package com.socrata.soql.analysis

import com.socrata.soql.FunctionName

sealed trait TypeLike[+Type]
case class FixedType[Type](typ: Type) extends TypeLike[Type]
case class VariableType(name: String) extends TypeLike[Nothing]

case class Function[Type](name: FunctionName, variableConstraints: Map[String, Set[Type]], parameters: Seq[TypeLike[Type]], result: TypeLike[Type]) {
  require(variableConstraints.keys.forall(k => parameters.contains(VariableType(k))), "unused constraint")

  val arity = parameters.length

  override def toString = {
    val sb = new StringBuilder(name.toString).append(" :: ")
    sb.append(variableConstraints.map { case (k, vs) => vs.mkString(k + ":{", ", ", "}") }.mkString(", "))
    if(variableConstraints.nonEmpty) sb.append(" => ")
    sb.append(parameters.map {
      case FixedType(typ) => typ
      case VariableType(name) => name
    }.mkString("", " -> ", " -> "))
    sb.append(result match {
      case FixedType(typ) => typ
      case VariableType(name) => name
    })
    sb.toString
  }
}

