package com.socrata.soql.analyzer2.mocktablefinder

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.codec.{JsonDecode, FieldDecode, JsonEncode, FieldEncode}
import com.rojoma.json.v3.util.{AutomaticJsonDecodeBuilder, AutomaticJsonEncodeBuilder, SimpleHierarchyDecodeBuilder, SimpleHierarchyEncodeBuilder, InternalTag}

import com.socrata.soql.BinaryTree
import com.socrata.soql.ast.Select
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, ResourceName, HoleName}
import com.socrata.soql.parsing.standalone_exceptions.LexerParserException
import com.socrata.soql.parsing.{StandaloneParser, AbstractParser}
import com.socrata.soql.analyzer2.{TableFinder, DatabaseTableName, ParsedTableDescription, CanonicalName, TableMap}

sealed abstract class Thing[+RNS, +CT]
case class D[+CT](schema: (String, CT)*) extends Thing[Nothing, CT]
case class Q[+RNS, +CT](scope: RNS, parent: String, soql: String, params: (String, CT)*) extends Thing[RNS, CT]
case class U[+RNS, CT](scope: RNS, soql: String, params: (String, CT)*) extends Thing[RNS, CT]

object MockTableFinder {
  def empty[RNS, CT] = new MockTableFinder[RNS, CT](Map.empty)
  def apply[RNS, CT](items: ((RNS, String), Thing[RNS, CT])*) = new MockTableFinder[RNS, CT](items.toMap)

  private sealed abstract class JThing[+RNS, +CT]
  private case class JD[+CT](schema: Seq[(String, CT)]) extends JThing[Nothing, CT]
  private case class JQ[+RNS,+CT](scope: Option[RNS], parent: String, soql: String, params: Map[String, CT]) extends JThing[RNS, CT]
  private case class JU[+RNS,+CT](scope: Option[RNS], soql: String, params: Seq[(String, CT)]) extends JThing[RNS, CT]

  implicit def jDecode[RNS: FieldDecode: JsonDecode, CT: JsonDecode]: JsonDecode[MockTableFinder[RNS, CT]] =
    new JsonDecode[MockTableFinder[RNS, CT]] {
      private implicit val jdDecode = AutomaticJsonDecodeBuilder[JD[CT]]
      private implicit val jqDecode = AutomaticJsonDecodeBuilder[JQ[RNS, CT]]
      private implicit val juDecode = AutomaticJsonDecodeBuilder[JU[RNS, CT]]

      private implicit val jDecode =
        SimpleHierarchyDecodeBuilder[JThing[RNS, CT]](InternalTag("type")).
          branch[JD[CT]]("dataset").
          branch[JQ[RNS, CT]]("saved query").
          branch[JU[RNS, CT]]("udf").
          build

      def decode(x: JValue) =
        JsonDecode.fromJValue[Map[RNS, Map[String, JThing[RNS, CT]]]](x).map { m =>
          new MockTableFinder(
            m.iterator.flatMap { case (rns, jthings) =>
              jthings.map { case (name, jthing) =>
                val thing = jthing match {
                  case JD(schema) => D(schema : _*)
                  case JQ(scopeOpt, parent, soql, params) => Q(scopeOpt.getOrElse(rns), parent, soql, params.toSeq : _*)
                  case JU(scopeOpt, soql, params) => U(scopeOpt.getOrElse(rns), soql, params : _*)
                }

                (rns, name) -> thing
              }
            }.toMap
          )
        }
    }

  implicit def jEncode[RNS: FieldEncode: JsonEncode, CT: JsonEncode]: JsonEncode[MockTableFinder[RNS, CT]] =
    new JsonEncode[MockTableFinder[RNS, CT]] {
      private implicit val jdEncode = AutomaticJsonEncodeBuilder[JD[CT]]
      private implicit val jqEncode = AutomaticJsonEncodeBuilder[JQ[RNS, CT]]
      private implicit val juEncode = AutomaticJsonEncodeBuilder[JU[RNS, CT]]

      private implicit val jEncode =
        SimpleHierarchyEncodeBuilder[JThing[RNS, CT]](InternalTag("type")).
          branch[JD[CT]]("dataset").
          branch[JQ[RNS, CT]]("saved query").
          branch[JU[RNS, CT]]("udf").
          build

      def encode(mtf: MockTableFinder[RNS, CT]) = {
        val rearranged: Map[RNS, Map[String, JThing[RNS, CT]]] =
          mtf.raw.toSeq.
            groupBy { case ((rns, name), thing) => rns }.
            map { case (rns, things) =>
              val jThings =
                things.map { case ((_rns, name), thing) =>
                  val jThing = thing match {
                    case D(schema @ _*) => JD(schema)
                    case Q(scope, parent, soql, params@_*) => JQ(Some(scope).filter(_ != rns), parent, soql, params.toMap)
                    case U(scope, soql, params@_*) => JU(Some(scope).filter(_ != rns), soql, params)
                  }
                  name -> jThing
                }.toMap
              rns -> jThings
            }.toMap
        JsonEncode.toJValue(rearranged)
      }
    }
}

class MockTableFinder[RNS, CT](private val raw: Map[(RNS, String), Thing[RNS, CT]]) extends TableFinder {
  private val tables = raw.iterator.map { case ((scope, rawResourceName), thing) =>
    val converted = thing match {
      case D(rawSchema @ _*) =>
        Dataset(
          DatabaseTableName(rawResourceName),
          OrderedMap() ++ rawSchema.iterator.map {case (rawColumnName, ct) =>
            ColumnName(rawColumnName) -> ct
          }
        )
      case Q(scope, parent, soql, params @ _*) =>
        Query(scope, CanonicalName(rawResourceName), ResourceName(parent), soql, params.iterator.map { case (k, v) => HoleName(k) -> v }.toMap)
      case U(scope, soql, params @ _*) =>
        TableFunction(scope, CanonicalName(rawResourceName), soql, OrderedMap() ++ params.iterator.map { case (k,v) => HoleName(k) -> v })
    }
      (scope, ResourceName(rawResourceName)) -> converted
  }.toMap

  type ResourceNameScope = RNS
  type ParseError = LexerParserException
  type ColumnType = CT

  protected def lookup(scope: RNS, name: ResourceName): Either[LookupError, TableDescription] = {
    tables.get((scope, name)) match {
      case Some(schema) =>
        Right(schema)
      case None =>
        Left(LookupError.NotFound)
    }
  }

  protected def parse(soql: String, udfParamsAllowed: Boolean): Either[ParseError, BinaryTree[Select]] = {
    try {
      Right(
        new StandaloneParser(AbstractParser.defaultParameters.copy(allowHoles = udfParamsAllowed)).
          binaryTreeSelect(soql)
      )
    } catch {
      case e: ParseError =>
        Left(e)
    }
  }

  private def parsed(thing: TableDescription) = {
    thing match {
      case ds: Dataset => ds.toParsed
      case Query(scope, canonicalName, parent, soql, params) => ParsedTableDescription.Query(scope, canonicalName, parent, parse(soql, false).getOrElse(throw new Exception("broken soql fixture 1")), soql, params)
      case TableFunction(scope, canonicalName, soql, params) => ParsedTableDescription.TableFunction(scope, canonicalName, parse(soql, false).getOrElse(throw new Exception("broken soql fixture 2")), soql, params)
    }
  }

  def apply(names: (RNS, String)*): Success[TableMap] = {
    val r = names.foldLeft(TableMap.empty[RNS, CT]) { (tableMap, scopeName) =>
      val (scope, n) = scopeName
      val name = ResourceName(n)
      tableMap + ((scope, name) -> parsed(tables((scope, name))))
    }

    if(r.size != names.length) {
      throw new Exception("Malformed table list")
    }

    Success(r)
  }

  def notFound(scope: RNS, name: String) =
    Error.NotFound((scope, ResourceName(name)))
}
