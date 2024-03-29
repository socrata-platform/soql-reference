package com.socrata.soql.analyzer2.statement

import scala.util.parsing.input.{Position, NoPosition}
import scala.collection.compat._
import scala.collection.compat.immutable.LazyList

import com.rojoma.json.v3.ast.{JString, JValue}
import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.collection._
import com.socrata.soql.environment.{ResourceName, ColumnName}
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer}

import DocUtils._

trait SelectImpl[MT <: MetaTypes] { this: Select[MT] =>
  type Self[MT <: MetaTypes] = Select[MT]
  def asSelf = this

  private[analyzer2] def columnReferences: Map[AutoTableLabel, Set[ColumnLabel]] = {
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
    for(s <- search) {
      // Add all our input columns
      val allInputColumns = from.schema.
        foldLeft(Map.empty[AutoTableLabel, Set[ColumnLabel]]) { (acc, schemaEnt) =>
          acc + (schemaEnt.table -> (acc.getOrElse(schemaEnt.table, Set.empty) + schemaEnt.column))
        }
      refs = refs.mergeWith(allInputColumns)(_ ++ _)
    }
    refs
  }

  final def directlyFind(predicate: Expr[MT] => Boolean): Option[Expr[MT]] = {
    // "directly" means "in _this_ query, not any non-lateral subqueries"
    selectList.valuesIterator.flatMap(_.expr.find(predicate)).nextOption().orElse {
      from.reduce[Option[Expr[MT]]](
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
        case Distinctiveness.Indistinct() => None
        case Distinctiveness.FullyDistinct() => None
        case Distinctiveness.On(exprs) => exprs.iterator.flatMap(_.find(predicate)).nextOption()
      }
    }
  }

  def directlyContains(e: Expr[MT]): Boolean =
    directlyFind(_ == e).isDefined

  def find(predicate: Expr[MT] => Boolean): Option[Expr[MT]] =
    directlyFind(predicate).orElse { // this checks everything except the non-lateral AtomicFroms
      from.reduce[Option[Expr[MT]]]( // ..so that's what this does
        _.find(predicate),
        { (a, join) => a.orElse { if(join.lateral) None else join.right.find(predicate) } }
      )
    }

  def contains(e: Expr[MT]): Boolean =
    find(_ == e).isDefined

  lazy val schema = locally {
    selectList.withValuesMapped { case NamedExpr(expr, name, hint, isSynthetic) =>
      def inheritedHint: Option[JValue] =
        expr match {
          // Why the .get here?  Because just because we have a column
          // ref, it does not _necessarily_ come from a table in our
          // FROM - it could come from a sibling table via a lateral
          // join.  In that case we just won't inherit.
          case c: Column[MT] => from.schemaByTableColumn.get((c.table, c.column)).flatMap(_.hint)
          case _ => None
        }

      Statement.SchemaEntry[MT](name, expr.typ, hint.orElse(inheritedHint), isSynthetic = isSynthetic)
    }
  }

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
    selectList.valuesIterator.exists(_.expr.isWindowed) ||
      orderBy.iterator.exists(_.expr.isWindowed)

  lazy val unique: LazyList[Seq[AutoColumnLabel]] = {
    val selectedColumns = selectList.iterator.collect {
      case (columnLabel, NamedExpr(PhysicalColumn(table, tableName, col, typ), _name, _hint, _isSynthetic)) =>
        PhysicalColumn(table, tableName, col, typ)(AtomicPositionInfo.Synthetic) -> columnLabel
      case (columnLabel, NamedExpr(VirtualColumn(table, col, typ), _name, _hint, _isSynthetic)) =>
        VirtualColumn(table, col, typ)(AtomicPositionInfo.Synthetic) -> columnLabel
    }.toMap[Column[MT], AutoColumnLabel]

    if(isAggregated) {
      // if we've selected all our grouping-exprs, then those columns
      // effectively represent a primary key.
      val selectedColumns = selectList.iterator.map { case (columnLabel, NamedExpr(expr, _name, _hint, _isSynthetic)) =>
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

  private[analyzer2] def doAllTables(set: Set[DatabaseTableName]): Set[DatabaseTableName] =
    from.doAllTables(set)

  private[analyzer2] def doRewriteDatabaseNames[MT2 <: MetaTypes](state: RewriteDatabaseNamesState[MT2]) = {
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

  def mapAlias(f: Option[ResourceName] => Option[ResourceName]): Self[MT] =
    copy(from = from.mapAlias(f))

  private[analyzer2] def doLabelMap(state: LabelMapState[MT]): Unit = {
    from.doLabelMap(state)
  }

  private[analyzer2] def findIsomorphismish(
    state: IsomorphismState,
    thisCurrentTableLabel: Option[AutoTableLabel],
    thatCurrentTableLabel: Option[AutoTableLabel],
    that: Statement[MT],
    recurseStmt: (Statement[MT], IsomorphismState, Option[AutoTableLabel], Option[AutoTableLabel], Statement[MT]) => Boolean,
    recurseFrom: (From[MT], IsomorphismState, From[MT]) => Boolean,
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
        recurseFrom(this.from, state, thatFrom) &&
          this.distinctiveness.findIsomorphism(state, thatDistinctiveness) &&
          this.selectList.size == thatSelectList.size &&
          this.selectList.iterator.zip(thatSelectList.iterator).forall { case ((thisColLabel, thisNamedExpr), (thatColLabel, thatNamedExpr)) =>
            state.tryAssociate(thisCurrentTableLabel, thisColLabel, thatCurrentTableLabel, thatColLabel) &&
              thisNamedExpr.expr.findIsomorphism(state, thatNamedExpr.expr)
            // do we care about the name or syntheticness or hints?
          } &&
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

  private[analyzer2] def findVerticalSlice(
    state: IsomorphismState,
    thisCurrentTableLabel: Option[AutoTableLabel],
    thatCurrentTableLabel: Option[AutoTableLabel],
    that: Statement[MT]
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
      ) if state.attempt(this.from.findVerticalSlice(_, thatFrom)) =>
        (this.distinctiveness, thatDistinctiveness) match {
          case (Distinctiveness.Indistinct(), Distinctiveness.Indistinct()) =>
            // ok
          case (Distinctiveness.On(as), Distinctiveness.On(bs)) if as.length == bs.length && as.lazyZip(bs).forall(_.findIsomorphism(state, _)) =>
            // ok
          case _ =>
            // This is a weird case; we need the same schema here but
            // we don't actually care if our parent tables are exactly
            // the same...
            return this.findIsomorphismish(
              state,
              thisCurrentTableLabel,
              thatCurrentTableLabel,
              that,
              (stmt, isostate, thisLbl, thatLbl, that) => stmt.findVerticalSlice(isostate, thisLbl, thatLbl, that),
              (from, isostate, that) => from.findVerticalSlice(isostate, that)
            )
        }
        if(!(this.where.isDefined == thatWhere.isDefined && this.where.lazyZip(thatWhere).forall(_.findIsomorphism(state, _)))) {
          return false
        }
        if(!(this.groupBy.length == thatGroupBy.length && this.groupBy.lazyZip(thatGroupBy).forall(_.findIsomorphism(state, _)))) {
          return false
        }
        if(!(this.having.isDefined == thatHaving.isDefined && this.having.lazyZip(thatHaving).forall(_.findIsomorphism(state, _)))) {
          return false
        }
        // "<=" because if "that" is more constrained than "this" in
        // ordering then we can actually get a valid vertical slice
        // out of it, since that's ordering satisfies this anyway.
        if(!(this.orderBy.length <= thatOrderBy.length && this.orderBy.lazyZip(thatOrderBy).forall(_.findIsomorphism(state, _)))) {
          return false
        }
        if(this.limit != thatLimit) {
          return false
        }
        if(this.offset != thatOffset) {
          return false
        }
        if(this.search != thatSearch) {
          return false
        }
        if(this.hint != thatHint) {
          return false
        }

        this.selectList.iterator.forall { case (thisLabel, thisNamedExpr) =>
          thatSelectList.iterator.exists { case (thatLabel, thatNamedExpr) =>
            thisNamedExpr.expr.findIsomorphism(state, thatNamedExpr.expr) &&
              state.tryAssociate(thisCurrentTableLabel, thisLabel, thatCurrentTableLabel, thatLabel)
          }
        }

      case _ =>
        this.findIsomorphismish(
          state,
          thisCurrentTableLabel,
          thatCurrentTableLabel,
          that,
          (stmt, isostate, thisLbl, thatLbl, that) => stmt.findVerticalSlice(isostate, thisLbl, thatLbl, that),
          (from, isostate, that) => from.findVerticalSlice(isostate, that)
        )
    }

  private[analyzer2] override def doDebugDoc(implicit ev: StatementDocProvider[MT]) =
    Seq[Option[Doc[Annotation[MT]]]](
      Some(
        (Seq(Some(d"SELECT"), distinctiveness.debugDoc(ev)).flatten.hsep +:
          selectList.toSeq.zipWithIndex.map { case ((columnLabel, NamedExpr(expr, columnName, _hint, _isSynthetic)), idx) =>
            expr.debugDoc(ev).annotate(Annotation.SelectListDefinition[MT](idx+1)) ++ Doc.softlineSep ++ d"AS" +#+ columnLabel.debugDoc.annotate(Annotation.ColumnAliasDefinition[MT](columnName, columnLabel))
          }.punctuate(d",")).sep.nest(2)
      ),
      Some((d"FROM" +#+ from.doDebugDoc).nest(2)),
      where.map { w => Seq(d"WHERE", w.debugDoc(ev)).sep.nest(2) },
      if(groupBy.nonEmpty) {
        Some((d"GROUP BY" +: groupBy.map(_.debugDoc(ev)).punctuate(d",")).sep.nest(2))
      } else {
        None
      },
      having.map { h => Seq(d"HAVING", h.debugDoc(ev)).sep.nest(2) },
      if(orderBy.nonEmpty) {
        Some((d"ORDER BY" +: orderBy.map(_.debugDoc(ev)).punctuate(d",")).sep.nest(2))
      } else {
        None
      },
      limit.map { l => d"LIMIT $l" },
      offset.map { o => d"OFFSET $o" },
      search.map { s => Seq(d"SEARCH", Doc(JString(s).toString)).sep }
    ).flatten.sep
}

trait OSelectImpl { this: Select.type =>
  implicit def serialize[MT <: MetaTypes](implicit rnsWritable: Writable[MT#ResourceNameScope], ctWritable: Writable[MT#ColumnType], exprWritable: Writable[Expr[MT]], dtnWritable: Writable[MT#DatabaseTableNameImpl], dcnWritable: Writable[MT#DatabaseColumnNameImpl]) = new Writable[Select[MT]] with MetaTypeHelper[MT] {
    def writeTo(buffer: WriteBuffer, select: Select[MT]): Unit = {
      val Select(
        distinctiveness: Distinctiveness[MT],
        selectList: OrderedMap[AutoColumnLabel, NamedExpr[MT]],
        from: From[MT],
        where: Option[Expr[MT]],
        groupBy: Seq[Expr[MT]],
        having: Option[Expr[MT]],
        orderBy: Seq[OrderBy[MT]],
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

  implicit def deserialize[MT <: MetaTypes](implicit rnsReadable: Readable[MT#ResourceNameScope], ctReadable: Readable[MT#ColumnType], exprReadable: Readable[Expr[MT]], dtnReadable: Readable[MT#DatabaseTableNameImpl], dcnReadable: Readable[MT#DatabaseColumnNameImpl]) = new Readable[Select[MT]] with MetaTypeHelper[MT] {
    def readFrom(buffer: ReadBuffer): Select[MT] = {
      Select(
        distinctiveness = buffer.read[Distinctiveness[MT]](),
        selectList = buffer.read[OrderedMap[AutoColumnLabel, NamedExpr[MT]]](),
        from = buffer.read[From[MT]](),
        where = buffer.read[Option[Expr[MT]]](),
        groupBy = buffer.read[Seq[Expr[MT]]](),
        having = buffer.read[Option[Expr[MT]]](),
        orderBy = buffer.read[Seq[OrderBy[MT]]](),
        limit = buffer.read[Option[BigInt]](),
        offset = buffer.read[Option[BigInt]](),
        search = buffer.read[Option[String]](),
        hint = buffer.read[Set[SelectHint]]()
      )
    }
  }
}
