package com.socrata.soql.sqlizer

import scala.util.parsing.input.Position

import com.socrata.soql.ast
import com.socrata.soql.collection.OrderedSet
import com.socrata.soql.environment.{Source, TypeName, Provenance}
import com.socrata.soql.typechecker.TypeInfo2
import com.socrata.soql.types.SoQLID
import com.socrata.soql.analyzer2._

object TestTypeInfo extends TypeInfo2[TestHelper.TestMT] {
  type MT = TestHelper.TestMT

  implicit object hasType extends HasType[CV, CT] {
    def typeOf(v: CV) = v.typ
  }

  def boolType = TestBoolean

  def literalBoolean(b: Boolean, source: Source): Expr =
    LiteralValue[MT](TestBoolean(b))(new AtomicPositionInfo(source))

  def potentialExprs(l: ast.Literal, source: Option[ScopedResourceName], currentPrimaryTable: Option[Provenance]): Seq[Expr] =
    l match {
      case ast.StringLiteral(s) =>
        val asInt: Option[CV] =
          try {
            Some(TestNumber(s.toInt))
          } catch {
            case _ : NumberFormatException => None
          }
        val asCompound = tryAsCompound(s)
        val asID = tryAsID(s, currentPrimaryTable)
        Seq[Option[CV]](Some(TestText(s)), asInt, asCompound, asID).flatten.map(LiteralValue[MT](_)(new AtomicPositionInfo(Source.nonSynthetic(source, l.position))))
      case ast.NumberLiteral(n) => Seq(LiteralValue[MT](TestNumber(n.toInt))(new AtomicPositionInfo(Source.nonSynthetic(source, l.position))))
      case ast.BooleanLiteral(b) => Seq(LiteralValue[MT](TestBoolean(b))(new AtomicPositionInfo(Source.nonSynthetic(source, l.position))))
      case ast.NullLiteral() => typeParameterUniverse.iterator.map(NullLiteral[MT](_)(new AtomicPositionInfo(Source.nonSynthetic(source, l.position)))).toVector
    }

  private def tryAsID(s: String, currentPrimaryTable: Option[Provenance]): Option[CV] = {
    SoQLID.FormattedButUnobfuscatedStringRep.unapply(s).map { case SoQLID(n) =>
      val result = TestID(n)
      result.provenance = currentPrimaryTable
      result
    }
  }

  private def tryAsCompound(s: String) = {
    val result: Option[CV] = s.lastIndexOf("/") match {
      case -1 => None
      case slashPos =>
        val first = Some(s.substring(0, slashPos)).filter(_.nonEmpty)
        val second = s.substring(slashPos + 1)
        if(second.isEmpty) {
          Some(TestCompound(first, None))
        } else {
          try {
            Some(TestCompound(first, Some(second.toDouble)))
          } catch {
            case _: NumberFormatException =>
              None
          }
        }
    }
    result match {
      case Some(TestCompound(None, None)) => None
      case other => other
    }
  }

  def updateProvenance(v: CV)(f: Provenance => Provenance): CV = {
    v match {
      case id: TestID =>
        val newId = id.copy()
        newId.provenance = id.provenance.map(f)
        newId
      case _ =>
        v
    }
  }

  def typeParameterUniverse: OrderedSet[TestType] = OrderedSet(
    TestText,
    TestNumber,
    TestBoolean,
    TestCompound,
    TestID
  )
  def isBoolean(typ: TestType): Boolean = typ == TestBoolean
  def isGroupable(typ: TestType): Boolean = true
  def isEquatable(typ: TestType): Boolean = true
  def isOrdered(typ: TestType): Boolean = typ.isOrdered
  def typeFor(name: TypeName): Option[TestType] = TestType.typesByName.get(name)
  def typeNameFor(typ: TestType): TypeName = typ.name
  def typeOf(value: TestValue): TestType = value.typ
}
