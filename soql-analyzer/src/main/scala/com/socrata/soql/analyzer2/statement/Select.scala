package com.socrata.soql.analyzer2.statement

import scala.util.parsing.input.{Position, NoPosition}
import scala.collection.compat._
import scala.collection.compat.immutable.LazyList

import com.rojoma.json.v3.ast.JString
import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.collection._
import com.socrata.soql.environment.{ResourceName, ColumnName}
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.HasDoc
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}

import DocUtils._

trait SelectImpl[+RNS, +CT, +CV] { this: Select[RNS, CT, CV] =>
  type Self[+RNS, +CT, +CV] = Select[RNS, CT, CV]
  def asSelf = this

  private[analyzer2] def columnReferences: Map[TableLabel, Set[ColumnLabel]] = {
    var refs = distinctiveness.columnReferences
    for(e <- selectList.values) {
      refs = refs.mergeWith(e.expr.columnReferences)(_ ++ _)
    }
    refs = refs.mergeWith(from.columnReferences)(_ ++ _)
    for(w <- where) {
      refs = refs.mergeWith(w.columnReferences)(_ ++ _)
    }
    for(g <- groupBy) {
      refs = refs.mergeWith(g.columnReferences)(_ ++ _)
    }
    for(h <- having) {
      refs = refs.mergeWith(h.columnReferences)(_ ++ _)
    }
    for(o <- orderBy) {
      refs = refs.mergeWith(o.expr.columnReferences)(_ ++ _)
    }
    refs
  }

  final def directlyFind(predicate: Expr[CT, CV] => Boolean): Option[Expr[CT, CV]] = {
    // "directly" means "in _this_ query, not any non-lateral subqueries"
    selectList.valuesIterator.flatMap(_.expr.find(predicate)).nextOption().orElse {
      from.reduce[Option[Expr[CT, CV]]](
        Function.const(None),
        { (a, join) =>
          a.orElse {
            join.on.find(predicate)
          }.orElse {
            if(join.lateral) join.right.find(predicate) else None
          }
        })
    }.orElse {
      where.flatMap(_.find(predicate))
    }.orElse {
      groupBy.iterator.flatMap(_.find(predicate)).nextOption()
    }.orElse {
      having.flatMap(_.find(predicate))
    }.orElse {
      orderBy.iterator.flatMap(_.expr.find(predicate)).nextOption()
    }.orElse {
      distinctiveness match {
        case Distinctiveness.Indistinct => None
        case Distinctiveness.FullyDistinct => None
        case Distinctiveness.On(exprs) => exprs.iterator.flatMap(_.find(predicate)).nextOption()
      }
    }
  }

  def directlyContains[CT2 >: CT, CV2 >: CV](e: Expr[CT2, CV2]): Boolean =
    directlyFind(_ == e).isDefined

  def find(predicate: Expr[CT, CV] => Boolean): Option[Expr[CT, CV]] =
    directlyFind(predicate).orElse { // this checks everything except the non-lateral AtomicFroms
      from.reduce[Option[Expr[CT, CV]]]( // ..so that's what this does
        _.find(predicate),
        { (a, join) => a.orElse { if(join.lateral) None else join.right.find(predicate) } }
      )
    }

  def contains[CT2 >: CT, CV2 >: CV](e: Expr[CT2, CV2]): Boolean =
    find(_ == e).isDefined

  val schema = selectList.withValuesMapped { case NamedExpr(expr, name) => NameEntry(name, expr.typ) }
  def getColumn(cl: ColumnLabel) = cl match {
    case acl: AutoColumnLabel => schema.get(acl)
    case _ => None
  }
  lazy val selectedExprs = selectList.withValuesMapped(_.expr)

  private def freshName(base: String) = {
    val names = selectList.valuesIterator.map(_.name).toSet
    Iterator.from(1).map { i => ColumnName(base + "_" + i) }.find { n =>
      !names.contains(n)
    }.get
  }

  def isAggregated =
    groupBy.nonEmpty ||
      having.nonEmpty ||
      selectList.valuesIterator.exists(_.expr.isAggregated) ||
      orderBy.iterator.exists(_.expr.isAggregated)

  def isWindowed =
    selectList.valuesIterator.exists(_.expr.isWindowed)

  lazy val unique: LazyList[Seq[AutoColumnLabel]] = {
    val selectedColumns = selectList.iterator.collect { case (columnLabel, NamedExpr(Column(table, col, typ), _name)) =>
      Column(table, col, typ)(AtomicPositionInfo.None) -> columnLabel
    }.toMap

    if(isAggregated) {
      // if we've selected all our grouping-exprs, then those columns
      // effectively represent a primary key.
      val selectedColumns = selectList.iterator.map { case (columnLabel, NamedExpr(expr, _name)) =>
        expr -> columnLabel
      }.toMap
      val synthetic =
        if(groupBy.forall(selectedColumns.contains(_))) {
          LazyList(groupBy.map(selectedColumns))
        } else {
          LazyList.empty
        }

      // A bit of a last-gasp hope - if we've grouped by all our
      // parents' primary keys and selected them, then this group by
      // was kind of pointless but we can use that to act as our
      // primary key too.
      val groupByExprs = groupBy.to(Set)
      val inherited =
        from.unique.flatMap { cols =>
          if(cols.forall { col => groupByExprs(col) && selectedColumns.contains(col) }) {
            Some(cols.map(selectedColumns))
          } else {
            None
          }
        }

      synthetic ++ inherited
    } else {
      from.unique.flatMap { columns =>
        if(columns.forall(selectedColumns.contains(_))) {
          Some(columns.map(selectedColumns))
        } else {
          None
        }
      }
    }
  }

  private[analyzer2] def realTables = from.realTables

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) = {
    Select(
      distinctiveness = distinctiveness.doRewriteDatabaseNames(state),
      selectList = selectList.withValuesMapped(_.doRewriteDatabaseNames(state)),
      from = from.doRewriteDatabaseNames(state),
      where = where.map(_.doRewriteDatabaseNames(state)),
      groupBy = groupBy.map(_.doRewriteDatabaseNames(state)),
      having = having.map(_.doRewriteDatabaseNames(state)),
      orderBy = orderBy.map(_.doRewriteDatabaseNames(state)),
      limit = limit,
      offset = offset,
      search = search,
      hint = hint
    )
  }

  private[analyzer2] def doRelabel(state: RelabelState) =
    Select(
      distinctiveness = distinctiveness.doRelabel(state),
      selectList = OrderedMap() ++ selectList.iterator.map { case (k, v) => state.convert(k) -> v.doRelabel(state) },
      from = from.doRelabel(state),
      where = where.map(_.doRelabel(state)),
      groupBy = groupBy.map(_.doRelabel(state)),
      having = having.map(_.doRelabel(state)),
      orderBy = orderBy.map(_.doRelabel(state)),
      limit = limit,
      offset = offset,
      search = search,
      hint = hint
    )

  def mapAlias(f: Option[ResourceName] => Option[ResourceName]): Self[RNS, CT, CV] =
    copy(from = from.mapAlias(f))

  private[analyzer2] def doLabelMap[RNS2 >: RNS](state: LabelMapState[RNS2]): Unit = {
    from.doLabelMap(state)
  }

  private[analyzer2] def findIsomorphism[RNS2 >: RNS, CT2 >: CT, CV2 >: CV](
    state: IsomorphismState,
    thisCurrentTableLabel: Option[TableLabel],
    thatCurrentTableLabel: Option[TableLabel],
    that: Statement[RNS2, CT2, CV2]
  ): Boolean =
    that match {
      case Select(
        thatDistinctiveness,
        thatSelectList,
        thatFrom,
        thatWhere,
        thatGroupBy,
        thatHaving,
        thatOrderBy,
        thatLimit,
        thatOffset,
        thatSearch,
        thatHint
      ) =>
        this.distinctiveness.findIsomorphism(state, thatDistinctiveness) &&
          this.selectList.size == thatSelectList.size &&
          this.selectList.iterator.zip(thatSelectList.iterator).forall { case ((thisColLabel, thisNamedExpr), (thatColLabel, thatNamedExpr)) =>
            state.tryAssociate(thisCurrentTableLabel, thisColLabel, thatCurrentTableLabel, thatColLabel) &&
              thisNamedExpr.expr.findIsomorphism(state, thatNamedExpr.expr)
            // do we care about the name?
          } &&
          this.from.findIsomorphism(state, thatFrom) &&
          this.where.isDefined == thatWhere.isDefined &&
          this.where.zip(thatWhere).forall { case (a, b) => a.findIsomorphism(state, b) } &&
          this.groupBy.length == thatGroupBy.length &&
          this.groupBy.zip(thatGroupBy).forall { case (a, b) => a.findIsomorphism(state, b) } &&
          this.having.isDefined == thatHaving.isDefined &&
          this.having.zip(thatHaving).forall { case (a, b) => a.findIsomorphism(state, b) } &&
          this.orderBy.length == thatOrderBy.length &&
          this.orderBy.zip(thatOrderBy).forall { case (a, b) => a.findIsomorphism(state, b) } &&
          this.limit == thatLimit &&
          this.offset == thatOffset &&
          this.search == thatSearch &&
          this.hint == thatHint
      case _ =>
        false
    }

  override def debugDoc(implicit ev: HasDoc[CV]) =
    Seq[Option[Doc[Annotation[RNS, CT]]]](
      Some(
        (Seq(Some(d"SELECT"), distinctiveness.debugDoc).flatten.hsep +:
          selectList.toSeq.zipWithIndex.map { case ((columnLabel, NamedExpr(expr, columnName)), idx) =>
            expr.debugDoc.annotate(Annotation.SelectListDefinition(idx+1)) ++ Doc.softlineSep ++ d"AS" +#+ columnLabel.debugDoc.annotate(Annotation.ColumnAliasDefinition(columnName, columnLabel))
          }.punctuate(d",")).sep.nest(2)
      ),
      Some((d"FROM" +#+ from.debugDoc).nest(2)),
      where.map { w => Seq(d"WHERE", w.debugDoc).sep.nest(2) },
      if(groupBy.nonEmpty) {
        Some((d"GROUP BY" +: groupBy.map(_.debugDoc).punctuate(d",")).sep.nest(2))
      } else {
        None
      },
      having.map { h => Seq(d"HAVING", h.debugDoc).sep.nest(2) },
      if(orderBy.nonEmpty) {
        Some((d"ORDER BY" +: orderBy.map(_.debugDoc).punctuate(d",")).sep.nest(2))
      } else {
        None
      },
      limit.map { l => d"LIMIT $l" },
      offset.map { o => d"OFFSET $o" },
      search.map { s => Seq(d"SEARCH", Doc(JString(s).toString)).sep }
    ).flatten.sep
}

trait OSelectImpl { this: Select.type =>
  implicit def serialize[RNS: Writable, CT: Writable, CV](implicit ev: Writable[Expr[CT, CV]]) = new Writable[Select[RNS, CT, CV]] {
    def writeTo(buffer: WriteBuffer, select: Select[RNS, CT, CV]): Unit = {
      val Select(
        distinctiveness: Distinctiveness[CT, CV],
        selectList: OrderedMap[AutoColumnLabel, NamedExpr[CT, CV]],
        from: From[RNS, CT, CV],
        where: Option[Expr[CT, CV]],
        groupBy: Seq[Expr[CT, CV]],
        having: Option[Expr[CT, CV]],
        orderBy: Seq[OrderBy[CT, CV]],
        limit: Option[BigInt],
        offset: Option[BigInt],
        search: Option[String],
        hint: Set[SelectHint]
      ) = select

      buffer.write(distinctiveness)
      buffer.write(selectList)
      buffer.write(from)
      buffer.write(where)
      buffer.write(groupBy)
      buffer.write(having)
      buffer.write(orderBy)
      buffer.write(limit)
      buffer.write(offset)
      buffer.write(search)
      buffer.write(hint)
    }
  }

  implicit def deserialize[RNS: Readable, CT: Readable, CV](implicit ev: Readable[Expr[CT, CV]]) = new Readable[Select[RNS, CT, CV]] {
    def readFrom(buffer: ReadBuffer): Select[RNS, CT, CV] = {
      Select(
        distinctiveness = buffer.read[Distinctiveness[CT, CV]](),
        selectList = buffer.read[OrderedMap[AutoColumnLabel, NamedExpr[CT, CV]]](),
        from = buffer.read[From[RNS, CT, CV]](),
        where = buffer.read[Option[Expr[CT, CV]]](),
        groupBy = buffer.read[Seq[Expr[CT, CV]]](),
        having = buffer.read[Option[Expr[CT, CV]]](),
        orderBy = buffer.read[Seq[OrderBy[CT, CV]]](),
        limit = buffer.read[Option[BigInt]](),
        offset = buffer.read[Option[BigInt]](),
        search = buffer.read[Option[String]](),
        hint = buffer.read[Set[SelectHint]]()
      )
    }
  }
}
