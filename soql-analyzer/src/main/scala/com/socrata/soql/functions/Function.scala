package com.socrata.soql.functions

import com.socrata.soql.environment.FunctionName
import com.socrata.soql.collection.CovariantSet

sealed trait TypeLike[+Type]
case class FixedType[Type](typ: Type) extends TypeLike[Type]
case class VariableType(name: String) extends TypeLike[Nothing]
class WildcardType() extends VariableType("?")

object WildcardType {
  def apply() = new WildcardType()
}

case class Example(explanation: String, query: String, tryit: String)

/**
 * @note This class has identity equality semantics even though it's a case class.  This
 *       is for performance reasons, as there should somewhere be a static list of supported
 *       functions which are re-used for all instances in the entire system.
 * @param identity A _unique_ name for this function that can be used to identify it when
 *                 serializing things that refer to it.
 */
case class Function[+Type](identity: String,
                           name: FunctionName,
                           constraints: Map[String, CovariantSet[Type]],
                           parameters: Seq[TypeLike[Type]],
                           repeated: Seq[TypeLike[Type]],
                           result: TypeLike[Type],
                           functionType: FunctionType,
                           doc: Function.Doc)  {

  val minArity = parameters.length
  def isVariadic = repeated.nonEmpty
  val varargsMultiplicity = repeated.length

  def isAggregate = functionType match {
    case FunctionType.Aggregate => true
    case _ => false
  }

  def needsWindow = functionType match {
    case FunctionType.Window(_) => true
    case _ => false
  }

  final def willAccept(n: Int): Boolean = {
    if(isVariadic && n > minArity) {
      (n - minArity) % varargsMultiplicity == 0
    } else {
      n == minArity
    }
  }

  lazy val typeParameters: Set[String] =
    (parameters ++ List(result) ++ repeated).collect {
      case VariableType(typeParameter) => typeParameter
    }.toSet

  lazy val typeParametersLessWildcards: Set[String] =
    (parameters ++ List(result) ++ repeated).collect {
      case t@VariableType(typeParameter) if !t.isInstanceOf[WildcardType] => typeParameter
    }.toSet

  lazy val monomorphic: Option[MonomorphicFunction[Type]] =
    if(result.isInstanceOf[FixedType[_]] && parameters.forall(_.isInstanceOf[FixedType[_]]))
      Some(MonomorphicFunction(this, Map.empty))
    else
      None

  lazy val wildcards = repeated.foldLeft(Set.empty[String]) { (acc, repeatedParam) =>
    repeatedParam match {
      case x: WildcardType => acc + x.name
      case _ => acc
    }
  }

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

object Function {
  case class Doc(
    description: String,
    examples: Seq[Example],
    status: Doc.Status = Doc.Normal
  )
  object Doc {
    sealed abstract class Status
    case object Normal extends Status
    case object Deprecated extends Status
    case object Hidden extends Status

    val empty = Doc("", Nil, Hidden)
  }
}
