package com.socrata.soql.functions

import scala.collection.compat.immutable.LazyList

import com.rojoma.json.v3.ast.JString

import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.environment.FunctionName
import com.socrata.soql.typechecker.FunctionInfo

case class MonomorphicFunction[+Type](function: Function[Type], bindings: Map[String, Type]) {
  def this(identity: String, name: FunctionName, parameters: Seq[Type], repeated: Seq[Type], result: Type, functionType: FunctionType)(doc: Function.Doc) =
    this(Function(
      identity,
      name,
      Map.empty,
      parameters.map(FixedType(_)),
      repeated.map(FixedType(_)),
      FixedType(result),
      functionType,
      doc
    ), Map.empty)

  val bindingsLessWildcards = bindings.keySet.diff(function.wildcards)
  require(bindingsLessWildcards == function.typeParametersLessWildcards, "bindings do not match")

  def name: FunctionName = function.name
  lazy val parameters: Seq[Type] = function.parameters.map(bind)
  lazy val repeated = function.repeated.map(bind)
  def allParameters: Seq[Type] =
    if (repeated.isEmpty) parameters else parameters.to(LazyList) ++ LazyList.continually(repeated).flatten

  def minArity = function.minArity
  def isVariadic = function.isVariadic
  def result: Type = bind(function.result)
  def functionType = function.functionType
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

object MonomorphicFunction {
  implicit def serialize[T: Writable] = new Writable[MonomorphicFunction[T]] {
    def writeTo(buffer: WriteBuffer, mf: MonomorphicFunction[T]): Unit = {
      buffer.write(mf.function.identity)
      buffer.write(mf.bindings)
    }
  }

  def deserialize[T: Readable](fi: FunctionInfo[T]) = new Readable[MonomorphicFunction[T]] {
    def readFrom(buffer: ReadBuffer): MonomorphicFunction[T] = {
      val identity = buffer.read[String]()
      fi.functionsByIdentity.get(identity) match {
        case Some(f) =>
          MonomorphicFunction(f, buffer.read[Map[String, T]]())
        case None =>
          fail("Unknown function " + JString(identity))
      }
    }
  }
}
