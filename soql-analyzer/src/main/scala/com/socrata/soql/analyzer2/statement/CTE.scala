package com.socrata.soql.analyzer2.statement

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.collection._
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.functions.MonomorphicFunction

import DocUtils._

trait CTEImpl[MT <: MetaTypes] { this: CTE[MT] =>
  import CTE.Definition

  type Self[MT <: MetaTypes] = CTE[MT]
  def asSelf = this

  val schema = useQuery.schema

  def unique = useQuery.unique

  def find(predicate: Expr[MT] => Boolean): Option[Expr[MT]] =
    definitions.valuesIterator.flatMap(_.query.find(predicate)).to(LazyList).headOption.orElse(useQuery.find(predicate))

  def contains(e: Expr[MT]): Boolean =
    definitions.valuesIterator.exists(_.query.contains(e)) || useQuery.contains(e)

  private[analyzer2] def doAllTables(set: Set[DatabaseTableName]): Set[DatabaseTableName] =
    useQuery.doAllTables(definitions.valuesIterator.foldLeft(set) { (acc, definition) => definition.query.doAllTables(acc) })

  private[analyzer2] def realTables: Map[AutoTableLabel, DatabaseTableName] =
    definitions.valuesIterator.map(_.query.realTables).foldLeft(useQuery.realTables) { (acc, defTables) =>
      acc ++ defTables
    }

  private[analyzer2] def columnReferences: Map[AutoTableLabel, Set[ColumnLabel]] =
    definitions.valuesIterator.foldLeft(useQuery.columnReferences) { (acc, definition) =>
      definition.query.columnReferences.mergeWith(acc)(_ ++ _)
    }

  private[analyzer2] def doRewriteDatabaseNames[MT2 <: MetaTypes](state: RewriteDatabaseNamesState[MT2]) =
    copy(
      definitions = OrderedMap() ++ definitions.iterator.map { case (label, defn) =>
        label -> defn.copy(query = defn.query.doRewriteDatabaseNames(state))
      },
      useQuery = useQuery.doRewriteDatabaseNames(state)
    )

  private[analyzer2] def doRelabel(state: RelabelState): Self[MT] = {
    val newDefinitions = OrderedMap() ++ definitions.iterator.map { case (label, definition) =>
      state.convert(label) -> definition.copy(query = definition.query.doRelabel(state))
    }
    CTE(definitions = newDefinitions, useQuery = useQuery.doRelabel(state))
  }

  private[analyzer2] def findIsomorphismish(
    state: IsomorphismState,
    thisCurrentTableLabel: Option[AutoTableLabel],
    thatCurrentTableLabel: Option[AutoTableLabel],
    that: Statement[MT],
    recurseStmt: (Statement[MT], IsomorphismState, Option[AutoTableLabel], Option[AutoTableLabel], Statement[MT]) => Boolean,
    recurseFrom: (From[MT], IsomorphismState, From[MT]) => Boolean
  ): Boolean =
    that match {
      case CTE(thatDefinitions, thatUseQuery) =>
        this.definitions.size == thatDefinitions.size &&
          this.definitions.iterator.zip(thatDefinitions.iterator).forall { case ((thisLabel, Definition(thisAlias, thisQuery, thisHint)), (thatLabel, Definition(thatAlias, thatQuery, thatHint))) =>
            state.tryAssociate(thisLabel, thatLabel) &&
              recurseStmt(thisQuery, state, Some(thisLabel), Some(thatLabel), thatQuery) &&
              thisHint == thatHint
          }
          this.useQuery.findIsomorphismish(state, thisCurrentTableLabel, thatCurrentTableLabel, thatUseQuery, recurseStmt, recurseFrom)
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
      case CTE(thatDefinitions, thatUseQuery) =>
        this.definitions.size == thatDefinitions.size &&
          this.definitions.iterator.zip(thatDefinitions.iterator).forall { case ((thisLabel, Definition(thisAlias, thisQuery, thisHint)), (thatLabel, Definition(thatAlias, thatQuery, thatHint))) =>
            state.tryAssociate(thisLabel, thatLabel) &&
              thisQuery.findVerticalSlice(state, Some(thisLabel), Some(thatLabel), thatQuery) &&
              thisHint == thatHint
          }
          this.useQuery.findVerticalSlice(state, thisCurrentTableLabel, thatCurrentTableLabel, thatUseQuery)
      case _ =>
        false
    }

  def mapAlias(f: Option[ResourceName] => Option[ResourceName]): Self[MT] =
    copy(
      definitions = OrderedMap() ++ definitions.iterator.map { case (label, Definition(alias, query, hint)) =>
        label -> Definition(f(alias), query.mapAlias(f), hint)
      },
      useQuery = useQuery.mapAlias(f)
    )

  private[analyzer2] override def doDebugDoc(implicit ev: StatementDocProvider[MT]): Doc[Annotation[MT]] = {
    val defDocs = definitions.iterator.map { case (label, definition) =>
      Seq(
        Some(label.debugDoc +#+ d"AS"),
        definition.hint.debugDoc,
        Some(definition.query.doDebugDoc.encloseNesting(d"(", d")"))
      ).flatten.hsep
    }.toVector.concatWith(_ ++ d"," ++ Doc.lineSep ++ _)

    (d"WITH" ++ Doc.lineSep ++ defDocs).nest(2) ++ Doc.lineSep ++ useQuery.doDebugDoc
  }

  private[analyzer2] def doLabelMap(state: LabelMapState[MT]): Unit = {
    for((label, defn) <- definitions) {
      defn.query.doLabelMap(state)
      val tr = LabelMap.TableReference(None, None)
      state.tableMap += label -> tr
      for((columnLabel, Statement.SchemaEntry(name, _typ, _isSynthetic, _hint)) <- defn.query.schema) {
        state.columnMap += (label, columnLabel) -> (tr, name)
      }
    }
    useQuery.doLabelMap(state)
  }

  override def nonlocalColumnReferences =
    definitions.valuesIterator.foldLeft(useQuery.nonlocalColumnReferences -- definitions.keys) { (acc, defn) =>
      Util.mergeColumnSet(acc, defn.query.nonlocalColumnReferences)
    }
}

trait OCTEImpl { this: CTE.type =>
  case class Definition[MT <: MetaTypes](
    alias: Option[ResourceName], // can this ever be not-none?
    query: Statement[MT],
    hint: MaterializedHint
  )

  object Definition {
    implicit def serialize[MT <: MetaTypes](implicit rnsWritable: Writable[MT#ResourceNameScope], ctWritable: Writable[MT#ColumnType], exprWritable: Writable[Expr[MT]], dtnWritable: Writable[MT#DatabaseTableNameImpl], dcnWritable: Writable[MT#DatabaseColumnNameImpl]): Writable[Definition[MT]] =
      new Writable[Definition[MT]] {
        def writeTo(buffer: WriteBuffer, defn: Definition[MT]): Unit = {
          buffer.write(defn.alias)
          buffer.write(defn.query)
          buffer.write(defn.hint)
        }
      }

    implicit def deserialize[MT <: MetaTypes](implicit rnsReadable: Readable[MT#ResourceNameScope], ctReadable: Readable[MT#ColumnType], exprReadable: Readable[Expr[MT]], dtnReadable: Readable[MT#DatabaseTableNameImpl], dcnReadable: Readable[MT#DatabaseColumnNameImpl]): Readable[Definition[MT]] =
      new Readable[Definition[MT]] {
        def readFrom(buffer: ReadBuffer): Definition[MT] = {
          Definition(
            alias = buffer.read[Option[ResourceName]](),
            query = buffer.read[Statement[MT]](),
            hint = buffer.read[MaterializedHint]()
          )
        }
      }
  }

  implicit def serialize[MT <: MetaTypes](implicit rnsWritable: Writable[MT#ResourceNameScope], ctWritable: Writable[MT#ColumnType], exprWritable: Writable[Expr[MT]], dtnWritable: Writable[MT#DatabaseTableNameImpl], dcnWritable: Writable[MT#DatabaseColumnNameImpl]): Writable[CTE[MT]] =
    new Writable[CTE[MT]] {
      def writeTo(buffer: WriteBuffer, ct: CTE[MT]): Unit = {
        buffer.write(ct.definitions)
        buffer.write(ct.useQuery)
      }
    }

  implicit def deserialize[MT <: MetaTypes](implicit rnsReadable: Readable[MT#ResourceNameScope], ctReadable: Readable[MT#ColumnType], exprReadable: Readable[Expr[MT]], dtnReadable: Readable[MT#DatabaseTableNameImpl], dcnReadable: Readable[MT#DatabaseColumnNameImpl]): Readable[CTE[MT]] =
    new Readable[CTE[MT]] {
      def readFrom(buffer: ReadBuffer): CTE[MT] = {
        CTE(
          definitions = buffer.read[OrderedMap[AutoTableLabel, Definition[MT]]](),
          useQuery = buffer.read[Statement[MT]]()
        )
      }
    }
}
