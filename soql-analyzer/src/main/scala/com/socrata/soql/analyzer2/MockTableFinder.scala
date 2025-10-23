package com.socrata.soql.analyzer2.mocktablefinder

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.codec.{JsonDecode, FieldDecode, JsonEncode, FieldEncode}
import com.rojoma.json.v3.util.{AutomaticJsonDecodeBuilder, AutomaticJsonEncodeBuilder, SimpleHierarchyDecodeBuilder, SimpleHierarchyEncodeBuilder, InternalTag}

import com.socrata.soql.BinaryTree
import com.socrata.soql.ast.Select
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, ResourceName, ScopedResourceName, HoleName}
import com.socrata.soql.parsing.standalone_exceptions.LexerParserException
import com.socrata.soql.parsing.StandaloneParser
import com.socrata.soql.analyzer2.{TableFinder, DatabaseTableName, DatabaseColumnName, TableDescription, CanonicalName, TableMap, ParserUtil, FoundTables, UnparsedFoundTables, UnparsedTableMap, MetaTypes, MetaTypeHelper}

sealed abstract class OptionalBoolean {
  def orElse(b: Boolean): Boolean
}
object OptionalBoolean {
  implicit class Provided(value: Boolean) extends OptionalBoolean {
    def orElse(b: Boolean) = value
  }
  implicit object Unprovided extends OptionalBoolean {
    def orElse(b: Boolean) = b
  }
}

sealed abstract class Thing[+RNS, +CT] {
  def canonicalName: Option[String]
}
case class D[+CT](schema: (String, CT)*) extends Thing[Nothing, CT] {
  private var canonicalName_ : Option[String] = None
  private var orderings_ : List[(String, Boolean, Boolean)] = Nil
  private var hiddenColumns_ : Set[String] = Set.empty
  private var primaryKeys_ : List[Seq[String]] = Nil
  private var outputColumnHints_ : Map[ColumnName, JValue] = Map.empty

  private def copyVars[CT2 >: CT](that: D[CT2]): Unit = {
    this.canonicalName_ = that.canonicalName_
    this.orderings_ = that.orderings_
    this.hiddenColumns_ = that.hiddenColumns_
    this.primaryKeys_ = that.primaryKeys_
    this.outputColumnHints_ = that.outputColumnHints_
  }

  def withCanonicalName(name: String): D[CT] = {
    val result = D(schema: _*)
    result.copyVars(this)
    result.canonicalName_ = Some(name)
    result
  }
  def canonicalName = canonicalName_

  def withOrdering(column: String, ascending: Boolean = true, nullLast: OptionalBoolean = OptionalBoolean.Unprovided): D[CT] = {
    val result = D(schema : _*)
    result.copyVars(this)
    result.orderings_ = (column, ascending, nullLast.orElse(ascending)) :: orderings_
    result
  }
  def orderings: Seq[(String, Boolean, Boolean)] = orderings_.reverse

  def withHiddenColumns(column: String*): D[CT] = {
    val result = D(schema : _*)
    result.copyVars(this)
    result.hiddenColumns_ = hiddenColumns_ ++ column
    result
  }
  def hiddenColumns = hiddenColumns_

  def withPrimaryKey(column: String*): D[CT] = {
    val result = D(schema : _*)
    result.copyVars(this)
    result.primaryKeys_ = column :: primaryKeys_
    result
  }
  def primaryKeys = primaryKeys_.reverse

  def withOutputColumnHints(hints: (String, JValue)*): D[CT] = {
    val result = D(schema : _*)
    result.copyVars(this)
    result.outputColumnHints_ = outputColumnHints_ ++ hints.map { case (c, v) => ColumnName(c) -> v }
    result
  }
  def outputColumnHints = outputColumnHints_
}
case class Q[+RNS, +CT](scope: RNS, parent: String, soql: String, params: (String, CT)*) extends Thing[RNS, CT] {
  private var canonicalName_ : Option[String] = None
  private var hiddenColumns_ : Set[ColumnName] = Set.empty
  private var outputColumnHints_ : Map[ColumnName, JValue] = Map.empty

  private def copyVars[RNS2 >: RNS, CT2 >: CT](that: Q[RNS2, CT2]): Unit = {
    this.canonicalName_ = that.canonicalName_
    this.hiddenColumns_ = that.hiddenColumns_
    this.outputColumnHints_ = that.outputColumnHints_
  }

  def withCanonicalName(name: String): Q[RNS, CT] = {
    val result = Q(scope, parent, soql, params : _*)
    result.copyVars(this)
    result.canonicalName_ = Some(name)
    result
  }
  def canonicalName = canonicalName_

  def withHiddenColumns(column: String*): Q[RNS, CT] = {
    val result = Q(scope, parent, soql, params : _*)
    result.copyVars(this)
    result.hiddenColumns_ = hiddenColumns_ ++ column.iterator.map(ColumnName(_))
    result
  }
  def hiddenColumns = hiddenColumns_

  def withOutputColumnHints(hints: (String, JValue)*): Q[RNS, CT] = {
    val result = Q(scope, parent, soql, params : _*)
    result.copyVars(this)
    result.outputColumnHints_ = outputColumnHints_ ++ hints.map { case (c, v) => ColumnName(c) -> v }
    result
  }
  def outputColumnHints = outputColumnHints_
}
case class U[+RNS, CT](scope: RNS, soql: String, params: (String, CT)*) extends Thing[RNS, CT] {
  private var canonicalName_ : Option[String] = None
  private var hiddenColumns_ : Set[ColumnName] = Set.empty

  private def copyVars[RNS2 >: RNS, CT2 >: CT](that: U[RNS2, CT]): Unit = {
    this.canonicalName_ = that.canonicalName_
    this.hiddenColumns_ = that.hiddenColumns_
  }

  def withCanonicalName(name: String): U[RNS, CT] = {
    val result = U(scope, soql, params : _*)
    result.copyVars(this)
    result.canonicalName_ = Some(name)
    result
  }
  def canonicalName = canonicalName_

  def withHiddenColumns(column: String*): U[RNS, CT] = {
    val result = U(scope, soql, params : _*)
    result.copyVars(this)
    result.hiddenColumns_ = hiddenColumns_ ++ column.iterator.map(ColumnName(_))
    result
  }
  def hiddenColumns = hiddenColumns_
}

object MockTableFinder {
  def empty[MT <: MetaTypes](implicit dtnIsString: String =:= MT#DatabaseTableNameImpl, dcnIsString: String =:= MT#DatabaseColumnNameImpl) = new MockTableFinder[MT](OrderedMap.empty)
  def apply[MT <: MetaTypes](items: ((MT#ResourceNameScope, String), Thing[MT#ResourceNameScope, MT#ColumnType])*)(implicit dtnIsString: String =:= MT#DatabaseTableNameImpl, dcnIsString: String =:= MT#DatabaseColumnNameImpl) = new MockTableFinder[MT](OrderedMap() ++ items)
  def apply[MT <: MetaTypes](items: UnparsedFoundTables[MT])(implicit dtnIsString: String =:= MT#DatabaseTableNameImpl, dcnIsString: String =:= MT#DatabaseColumnNameImpl) = UnparsedTableMap.asMockTableFinder(items.tableMap)
  def apply[MT <: MetaTypes](items: FoundTables[MT])(implicit dtnIsString: String =:= MT#DatabaseTableNameImpl, dcnIsString: String =:= MT#DatabaseColumnNameImpl): MockTableFinder[MT] = this(items.asUnparsedFoundTables)

  private sealed abstract class JThing[+RNS, +CT]
  private case class JD[+CT](schema: Seq[(String, CT)], canonicalName: Option[String], orderings: Option[Seq[(String, Boolean, Boolean)]], hiddenColumns: Option[Seq[String]], primaryKeys: Option[Seq[Seq[String]]]) extends JThing[Nothing, CT]
  private case class JQ[+RNS,+CT](scope: Option[RNS], parent: String, soql: String, params: Map[String, CT], canonicalName: Option[String], hiddenColumns: Option[Seq[String]]) extends JThing[RNS, CT]
  private case class JU[+RNS,+CT](scope: Option[RNS], soql: String, params: Seq[(String, CT)], canonicalName: Option[String], hiddenColumns: Option[Seq[String]]) extends JThing[RNS, CT]

  implicit def jDecode[MT <: MetaTypes](implicit rnsFieldDecode: FieldDecode[MT#ResourceNameScope], rnsDecode: JsonDecode[MT#ResourceNameScope], ctDecode: JsonDecode[MT#ColumnType], dtnIsString: String =:= MT#DatabaseTableNameImpl, dcnIsString: String =:= MT#DatabaseColumnNameImpl): JsonDecode[MockTableFinder[MT]] =
    new JsonDecode[MockTableFinder[MT]] with MetaTypeHelper[MT] {
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
            OrderedMap() ++ m.iterator.flatMap { case (rns, jthings) =>
              jthings.map { case (name, jthing) =>
                val thing = jthing match {
                  case JD(schema, canonicalName, orderings, hiddenColumns, pks) =>
                    pks.getOrElse(Nil).foldLeft(
                      orderings.getOrElse(Nil).foldLeft(
                        canonicalName.foldLeft(D(schema : _*)) { (d, cname) =>
                          d.withCanonicalName(cname)
                        }
                      ) { (d, orderDir) =>
                        d.withOrdering(orderDir._1, orderDir._2)
                      }.withHiddenColumns(hiddenColumns.getOrElse(Nil) : _*)
                    ) { (dataset, pk) => dataset.withPrimaryKey(pk: _*) }
                  case JQ(scopeOpt, parent, soql, params, cname, hiddenColumns) =>
                    val result = Q(scopeOpt.getOrElse(rns), parent, soql, params.toSeq : _*)
                    cname.fold(result)(result.withCanonicalName).withHiddenColumns(hiddenColumns.getOrElse(Nil) : _*)
                  case JU(scopeOpt, soql, params, cname, hiddenColumns) =>
                    val result = U(scopeOpt.getOrElse(rns), soql, params : _*)
                    cname.fold(result)(result.withCanonicalName).withHiddenColumns(hiddenColumns.getOrElse(Nil) : _*)
                }

                (rns, name) -> thing
              }
            }
          )
        }
    }

  implicit def jEncode[MT <: MetaTypes](implicit rnsFieldEncode: FieldEncode[MT#ResourceNameScope], rnsEncode: JsonEncode[MT#ResourceNameScope], ctEncode: JsonEncode[MT#ColumnType]): JsonEncode[MockTableFinder[MT]] =
    new JsonEncode[MockTableFinder[MT]] with MetaTypeHelper[MT] {
      private implicit val jdEncode = AutomaticJsonEncodeBuilder[JD[CT]]
      private implicit val jqEncode = AutomaticJsonEncodeBuilder[JQ[RNS, CT]]
      private implicit val juEncode = AutomaticJsonEncodeBuilder[JU[RNS, CT]]

      private implicit val jEncode =
        SimpleHierarchyEncodeBuilder[JThing[RNS, CT]](InternalTag("type")).
          branch[JD[CT]]("dataset").
          branch[JQ[RNS, CT]]("saved query").
          branch[JU[RNS, CT]]("udf").
          build

      def encode(mtf: MockTableFinder[MT]) = {
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
                        d.canonicalName,
                        Some(d.orderings).filter(_.nonEmpty),
                        Some(d.hiddenColumns.toSeq).filter(_.nonEmpty),
                        Some(d.primaryKeys).filter(_.nonEmpty)
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

class MockTableFinder[MT <: MetaTypes](private val raw: OrderedMap[(MT#ResourceNameScope, String), Thing[MT#ResourceNameScope, MT#ColumnType]])(implicit dtnIsString: String =:= MT#DatabaseTableNameImpl, dcnIsString: String =:= MT#DatabaseColumnNameImpl) extends TableFinder[MT] {
  private val tables: Map[ScopedResourceName, FinderTableDescription] = locally {
    // canonical names need to be unique within the whole tablefinder
    // (not just the scope) so unless the user has specified a
    // particular CN, generate one from the resource name, ensuring it
    // doesn't collide with any other CN.
    var knownCanonicalNames = raw.iterator.flatMap { case (_, thing) => thing.canonicalName }.toSet
    def canonicalNameForResourceName(rn: String) = {
      val r = (Iterator.single(rn) ++ Iterator.from(1).map { i => s"${rn}_${i}" })
        .dropWhile(knownCanonicalNames(_))
        .next()
      knownCanonicalNames += r
      r
    }

    raw.iterator.map { case ((scope, rawResourceName), thing) =>
      val converted = thing match {
        case d@D(rawSchema @ _*) =>
          val hiddenColumns = d.hiddenColumns.map(ColumnName(_))
          val outputColumnHints = d.outputColumnHints
          Dataset(
            DatabaseTableName(rawResourceName),
            CanonicalName(d.canonicalName.getOrElse(canonicalNameForResourceName(rawResourceName))),
            OrderedMap() ++ rawSchema.iterator.map { case (rawColumnName, ct) =>
              val cn = ColumnName(rawColumnName)
              DatabaseColumnName[MT#DatabaseColumnNameImpl](cn.caseFolded) -> DatasetColumnInfo(cn, ct, hidden = hiddenColumns(cn), hint = outputColumnHints.get(cn))
            },
            d.orderings.map { case (col, asc, nullLast) => Ordering(DatabaseColumnName(ColumnName(col).caseFolded), asc, nullLast) },
            d.primaryKeys.map(_.map { col => DatabaseColumnName[MT#DatabaseColumnNameImpl](ColumnName(col).caseFolded) })
          )
        case q@Q(scope, parent, soql, params @ _*) =>
          Query(
            scope,
            CanonicalName(q.canonicalName.getOrElse(canonicalNameForResourceName(rawResourceName))),
            ResourceName(parent),
            soql,
            params.iterator.map { case (k, v) => HoleName(k) -> v }.toMap,
            q.hiddenColumns,
            q.outputColumnHints
          )
        case u@U(scope, soql, params @ _*) =>
          TableFunction(
            scope,
            CanonicalName(u.canonicalName.getOrElse(canonicalNameForResourceName(rawResourceName))),
            soql,
            OrderedMap() ++ params.iterator.map { case (k,v) => HoleName(k) -> v },
            u.hiddenColumns
          )
      }
      ScopedResourceName(scope, ResourceName(rawResourceName)) -> converted
    }.toMap
  }

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
      case Query(scope, canonicalName, parent, soql, params, hiddenColumns, outputColumnHints) =>
        TableDescription.Query[MT](
          scope, canonicalName, parent,
          ParserUtil.parseWithoutContext(soql, parserParameters.copy(allowHoles = false)).getOrElse(throw new Exception("broken soql fixture 1")),
          soql, params, hiddenColumns, outputColumnHints)
      case TableFunction(scope, canonicalName, soql, params, hiddenColumns) =>
        TableDescription.TableFunction[MT](
          scope, canonicalName,
          ParserUtil.parseWithoutContext(soql, parserParameters.copy(allowHoles = true)).getOrElse(throw new Exception("broken soql fixture 2")),
          soql, params, hiddenColumns
        )
    }
  }

  def apply(names: (MT#ResourceNameScope, String)*): Result[TableMap] = {
    val r = names.foldLeft(TableMap.empty[MT]) { (tableMap, scopeName) =>
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
