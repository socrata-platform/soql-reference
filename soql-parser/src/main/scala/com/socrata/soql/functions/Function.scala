package com.socrata.soql.functions

import com.socrata.soql.names.FunctionName

sealed trait TypeLike[+Type]
case class FixedType[Type](typ: Type) extends TypeLike[Type]
case class VariableType(name: String) extends TypeLike[Nothing]

case class Function[+Type](name: FunctionName, constraints: Map[String, Any => Boolean /* ick, but without Set being covariant... */], parameters: Seq[TypeLike[Type]], repeated: Option[TypeLike[Type]], result: TypeLike[Type], isAggregate: Boolean = false) {
  val minArity = parameters.length
  def isVariadic = repeated.isDefined

  lazy val typeParameters: Set[String] =
    (parameters ++ List(result) ++ repeated).collect {
      case VariableType(typeParameter) => typeParameter
    }.toSet

  lazy val monomorphic: Option[MonomorphicFunction[Type]] =
    if(result.isInstanceOf[FixedType[_]] && parameters.forall(_.isInstanceOf[FixedType[_]]))
      Some(MonomorphicFunction(this, Map.empty))
    else
      None

  override def toString = {
    val sb = new StringBuilder(name.toString).append(" :: ")
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

