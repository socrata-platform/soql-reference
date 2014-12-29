package com.socrata.soql.functions

import com.socrata.soql.environment.FunctionName

sealed trait TypeLike[+Type]
case class FixedType[Type](typ: Type) extends TypeLike[Type]
case class VariableType(name: String) extends TypeLike[Nothing]

/**
 * @note This class has identity equality semantics even though it's a case class.  This
 *       is for performance reasons, as there should somewhere be a static list of supported
 *       functions which are re-used for all instances in the entire system.
 * @param identity A _unique_ name for this function that can be used to identify it when
 *                 serializing things that refer to it.
 */
case class Function[+Type](identity: String,
                           name: FunctionName,
                           constraints: Map[String, Any => Boolean /* ick, but without Set being covariant... */],
                           parameters: Seq[TypeLike[Type]],
                           repeated: Seq[TypeLike[Type]],
                           result: TypeLike[Type],
                           isAggregate: Boolean = false)  {

  val minArity = parameters.length
  def isVariadic = repeated.nonEmpty

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

  override final def hashCode = System.identityHashCode(this)
  override final def equals(that: Any) = this eq that.asInstanceOf[AnyRef]
}
