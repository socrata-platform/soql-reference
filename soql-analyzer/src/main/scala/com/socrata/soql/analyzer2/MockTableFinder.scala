package com.socrata.soql.analyzer2.mocktablefinder

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.codec.{JsonDecode, FieldDecode, JsonEncode, FieldEncode}
import com.rojoma.json.v3.util.{AutomaticJsonDecodeBuilder, AutomaticJsonEncodeBuilder, SimpleHierarchyDecodeBuilder, SimpleHierarchyEncodeBuilder, InternalTag}

import com.socrata.soql.BinaryTree
import com.socrata.soql.ast.Select
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, ResourceName, HoleName}
import com.socrata.soql.parsing.standalone_exceptions.LexerParserException
import com.socrata.soql.parsing.StandaloneParser
import com.socrata.soql.analyzer2.{TableFinder, DatabaseTableName, TableDescription, CanonicalName, TableMap, ParserUtil, FoundTables, UnparsedFoundTables, UnparsedTableMap, ScopedResourceName}

sealed abstract class Thing[+RNS, +CT]
case class D[+CT](schema: (String, CT)*) extends Thing[Nothing, CT] {
  private var orderings_ : List[(String, Boolean)] = Nil
  private var hiddenColumns_ : Set[ColumnName] = Set.empty

  def withOrdering(column: String, ascending: Boolean = true): D[CT] = {
    val result = D(schema : _*)
    result.orderings_ = (column, ascending) :: orderings_
    result.hiddenColumns_ = hiddenColumns_
    result
  }
  def orderings: Seq[(String, Boolean)] = orderings_.reverse

  def withHiddenColumns(column: String*): D[CT] = {
    val result = D(schema : _*)
    result.orderings_ = orderings_
    result.hiddenColumns_ = hiddenColumns_ ++ column.iterator.map(ColumnName(_))
    result
  }
  def hiddenColumns = hiddenColumns_
}
case class Q[+RNS, +CT](scope: RNS, parent: String, soql: String, params: (String, CT)*) extends Thing[RNS, CT] {
  private var canonicalName_ : Option[String] = None
  private var hiddenColumns_ : Set[ColumnName] = Set.empty

  def withCanonicalName(name: String): Q[RNS, CT] = {
    val result = Q(scope, parent, soql, params : _*)
    result.canonicalName_ = Some(name)
    result.hiddenColumns_ = hiddenColumns_
    result
  }
  def canonicalName = canonicalName_

  def withHiddenColumns(column: String*): Q[RNS, CT] = {
    val result = Q(scope, parent, soql, params : _*)
    result.canonicalName_ = canonicalName_
    result.hiddenColumns_ = hiddenColumns_ ++ column.iterator.map(ColumnName(_))
    result
  }
  def hiddenColumns = hiddenColumns_
}
case class U[+RNS, CT](scope: RNS, soql: String, params: (String, CT)*) extends Thing[RNS, CT] {
  private var canonicalName_ : Option[String] = None
  private var hiddenColumns_ : Set[ColumnName] = Set.empty

  def withCanonicalName(name: String): U[RNS, CT] = {
    val result = U(scope, soql, params : _*)
    result.canonicalName_ = Some(name)
    result.hiddenColumns_ = hiddenColumns_
    result
  }
  def canonicalName = canonicalName_

  def withHiddenColumns(column: String*): U[RNS, CT] = {
    val result = U(scope, soql, params : _*)
    result.canonicalName_ = canonicalName_
    result.hiddenColumns_ = hiddenColumns_ ++ column.iterator.map(ColumnName(_))
    result
  }
  def hiddenColumns = hiddenColumns_
}

object MockTableFinder {
  def empty[RNS, CT] = new MockTableFinder[RNS, CT](Map.empty)
  def apply[RNS, CT](items: ((RNS, String), Thing[RNS, CT])*) = new MockTableFinder[RNS, CT](items.toMap)
  def apply[RNS, CT](items: FoundTables[RNS, CT]): MockTableFinder[RNS, CT] = this(items.asUnparsedFoundTables)
  def apply[RNS, CT](items: UnparsedFoundTables[RNS, CT]) = UnparsedTableMap.asMockTableFinder(items.tableMap)

  private sealed abstract class JThing[+RNS, +CT]
  private case class JD[+CT](schema: Seq[(String, CT)], orderings: Option[Seq[(String, Boolean)]], hiddenColumns: Option[Seq[String]]) extends JThing[Nothing, CT]
  private case class JQ[+RNS,+CT](scope: Option[RNS], parent: String, soql: String, params: Map[String, CT], canonicalName: Option[String], hiddenColumns: Option[Seq[String]]) extends JThing[RNS, CT]
  private case class JU[+RNS,+CT](scope: Option[RNS], soql: String, params: Seq[(String, CT)], canonicalName: Option[String], hiddenColumns: Option[Seq[String]]) extends JThing[RNS, CT]

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
                  case JD(schema, orderings, hiddenColumns) =>
                    orderings.getOrElse(Nil).foldLeft(D(schema : _*)) { (d, orderDir) =>
                      d.withOrdering(orderDir._1, orderDir._2)
                    }.withHiddenColumns(hiddenColumns.getOrElse(Nil) : _*)
                  case JQ(scopeOpt, parent, soql, params, cname, hiddenColumns) =>
                    val result = Q(scopeOpt.getOrElse(rns), parent, soql, params.toSeq : _*)
                    cname.fold(result)(result.withCanonicalName).withHiddenColumns(hiddenColumns.getOrElse(Nil) : _*)
                  case JU(scopeOpt, soql, params, cname, hiddenColumns) =>
                    val result = U(scopeOpt.getOrElse(rns), soql, params : _*)
                    cname.fold(result)(result.withCanonicalName).withHiddenColumns(hiddenColumns.getOrElse(Nil) : _*)
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
                    case d@D(schema @ _*) =>
                      JD(
                        schema,
                        Some(d.orderings).filter(_.nonEmpty),
                        Some(d.hiddenColumns.map(_.name).toSeq).filter(_.nonEmpty)
                      )
                    case q@Q(scope, parent, soql, params@_*) =>
                      JQ(
                        Some(scope).filter(_ != rns),
                        parent,
                        soql,
                        params.toMap,
                        q.canonicalName.filter(_ != name),
                        Some(q.hiddenColumns.map(_.name).toSeq).filter(_.nonEmpty)
                      )
                    case u@U(scope, soql, params@_*) =>
                      JU(
                        Some(scope).filter(_ != rns),
                        soql,
                        params,
                        u.canonicalName.filter(_ != name),
                        Some(u.hiddenColumns.map(_.name).toSeq).filter(_.nonEmpty)
                      )
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
  private val tables: Map[ScopedResourceName, FinderTableDescription] = raw.iterator.map { case ((scope, rawResourceName), thing) =>
    val converted = thing match {
      case d@D(rawSchema @ _*) =>
        Dataset(
          DatabaseTableName(rawResourceName),
          CanonicalName(rawResourceName),
          OrderedMap() ++ rawSchema.iterator.map {case (rawColumnName, ct) =>
            val cn = ColumnName(rawColumnName)
            cn -> DatasetColumnInfo(ct, hidden = d.hiddenColumns(cn))
          },
          d.orderings.map { case (col, asc) => Ordering(ColumnName(col), asc) }
        )
      case q@Q(scope, parent, soql, params @ _*) =>
        Query(
          scope,
          CanonicalName(q.canonicalName.getOrElse(rawResourceName)),
          ResourceName(parent),
          soql,
          params.iterator.map { case (k, v) => HoleName(k) -> v }.toMap,
          q.hiddenColumns
        )
      case u@U(scope, soql, params @ _*) =>
        TableFunction(
          scope,
          CanonicalName(u.canonicalName.getOrElse(rawResourceName)),
          soql,
          OrderedMap() ++ params.iterator.map { case (k,v) => HoleName(k) -> v },
          u.hiddenColumns
        )
    }
    ScopedResourceName(scope, ResourceName(rawResourceName)) -> converted
  }.toMap

  type ResourceNameScope = RNS
  type ColumnType = CT

  protected def lookup(name: ScopedResourceName): Either[LookupError, FinderTableDescription] = {
    tables.get(name) match {
      case Some(schema) =>
        Right(schema)
      case None =>
        Left(LookupError.NotFound)
    }
  }

  private def parsed(thing: FinderTableDescription) = {
    thing match {
      case ds: Dataset => ds.toParsed
      case Query(scope, canonicalName, parent, soql, params, hiddenColumns) =>
        TableDescription.Query(
          scope, canonicalName, parent,
          ParserUtil.parseWithoutContext(soql, parserParameters.copy(allowHoles = false)).getOrElse(throw new Exception("broken soql fixture 1")),
          soql, params, hiddenColumns)
      case TableFunction(scope, canonicalName, soql, params, hiddenColumns) =>
        TableDescription.TableFunction(
          scope, canonicalName,
          ParserUtil.parseWithoutContext(soql, parserParameters.copy(allowHoles = true)).getOrElse(throw new Exception("broken soql fixture 2")),
          soql, params, hiddenColumns
        )
    }
  }

  def apply(names: (RNS, String)*): Result[TableMap] = {
    val r = names.foldLeft(TableMap.empty[RNS, CT]) { (tableMap, scopeName) =>
      val (scope, n) = scopeName
      val name = ResourceName(n)
      tableMap + (ScopedResourceName(scope, name) -> parsed(tables(ScopedResourceName(scope, name))))
    }

    if(r.size != names.length) {
      throw new Exception("Malformed table list")
    }

    Right(r)
  }
}
