package com.socrata.soql.analyzer2.statement

import scala.collection.compat.immutable.LazyList

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

  lazy val referencedCTEs = definitions.valuesIterator.foldLeft(useQuery.referencedCTEs) { (acc, defn) =>
    acc ++ defn.query.referencedCTEs
  } -- definitions.keysIterator

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
      definitions = definitions.withValuesMapped { defn =>
        defn.copy(query = defn.query.doRewriteDatabaseNames(state))
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
            // This isn't great, but we're not going to check the
            // queries for isomorphism here, because we do that in
            // "virtual table space", and we don't have a full virtual
            // table here, just a potential one.  Instead, we'll do
            // that at the point of use, in FromCTE.
            state.tryAssociate(thisLabel, thatLabel) &&
              thisHint == thatHint
          } &&
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
            // similarly to findIsomorphismish, we'll associate the
            // CTE labels here but defer the "is vertical slice?"
            // checking to point of use.
            state.tryAssociate(thisLabel, thatLabel) &&
              thisHint == thatHint
          } &&
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
    }
    useQuery.doLabelMap(state)
  }

  override def nonlocalColumnReferences =
    definitions.valuesIterator.foldLeft(useQuery.nonlocalColumnReferences) { (acc, defn) =>
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
        val validator = new CTEValidator[MT]()
        val result =
          // Need to reseat FromCTEs' basedOn references to point here
          // as necessary...
          validator.fixupSpecificallyCTEReferences(
            AvailableCTEs.empty,
            CTE.unvalidated(
              definitions = buffer.read[OrderedMap[AutoCTELabel, Definition[MT]]](),
              useQuery = buffer.read[Statement[MT]]()
            )
          )
        validator.validate(AvailableCTEs.empty[MT, Unit], result)
        result
      }
    }

  private class CTEValidator[MT <: MetaTypes]() extends StatementUniverse[MT] {
    type ACTEs = AvailableCTEs[Unit]

    def validate(availableCTEs: ACTEs, s: Statement): Unit = {
      if(s.referencedCTEs.intersect(availableCTEs.ctes.keySet).isEmpty) {
        // no referenced CTES in the CTEs we know about == it is valid
        return
      }
      s match {
        case CombinedTables(_op, left, right) =>
          validate(availableCTEs, left)
          validate(availableCTEs, right)
        case v : Values =>
          // ok
        case CTE(defns, useQuery) =>
          val (newAvailableCTEs, newDefns) = availableCTEs.collect(defns) { (aCTEs, query) =>
            validate(aCTEs, query)

            ((), query)
          }
          validate(newAvailableCTEs, useQuery)
        case sel: Select =>
          validate(availableCTEs, sel.from)
      }
    }

    def validate(availableCTEs: ACTEs, f: From): Unit =
      f.reduce[Unit](
        validateAtomic(availableCTEs, _),
        { case ((), j) => validateAtomic(availableCTEs, j.right) }
      )

    def validateAtomic(availableCTEs: ACTEs, f: AtomicFrom): Unit =
      f match {
        case fc: FromCTE =>
          for(defQuery <- availableCTEs.ctes.get(fc.cteLabel)) {
            fc.basedOn eq defQuery.stmt
          }
        case fs: FromStatement =>
          validate(availableCTEs, fs.statement)
        case _ =>
          // ok
      }


    def fixupCTEReferences(availableCTEs: ACTEs, s: Statement): Statement = {
      if(s.referencedCTEs.intersect(availableCTEs.ctes.keySet).isEmpty) {
        // no referenced CTES in the CTEs we know about == it is valid
        return s
      }

      s match {
        case CombinedTables(op, left, right) =>
          CombinedTables(
            op,
            fixupCTEReferences(availableCTEs, left),
            fixupCTEReferences(availableCTEs, right)
          )

        case v : Values =>
          v

        case cte: CTE =>
          fixupSpecificallyCTEReferences(availableCTEs, cte)

        case sel: Select =>
          sel.copy(from = fixupCTEReferences(availableCTEs, sel.from))
      }
    }

    def fixupSpecificallyCTEReferences(availableCTEs: ACTEs, cte: CTE): CTE = {
      val CTE(defns, useQuery) = cte
      val (newAvailableCTEs, newDefns) = availableCTEs.collect(defns) { (aCTEs, query) =>
        fixupCTEReferences(aCTEs, query)

        ((), query)
      }
      val newUseQuery = fixupCTEReferences(newAvailableCTEs, useQuery)
      CTE.unvalidated(newDefns, newUseQuery)
    }

    def fixupCTEReferences(availableCTEs: ACTEs, f: From): From =
      f.map[MT](
        fixupCTEReferencesAtomic(availableCTEs, _),
        { case (joinType, lat, left, right, on) =>
          Join(joinType, lat, left, fixupCTEReferencesAtomic(availableCTEs, right), on)
        }
      )

    def fixupCTEReferencesAtomic(availableCTEs: ACTEs, f: AtomicFrom): AtomicFrom =
      f match {
        case fc: FromCTE =>
          availableCTEs.ctes.get(fc.cteLabel) match {
            case None =>
              // ok, it must just refer to a CTE defined at a higher level
              fc
            case Some(AvailableCTE(defQuery, ())) if defQuery eq fc.basedOn =>
              // ok, we're already fixed up
              fc
            case Some(AvailableCTE(defQuery, ())) =>
              fc.copy(basedOn = defQuery)
          }
        case fs: FromStatement =>
          fs.copy(statement = fixupCTEReferences(availableCTEs, fs.statement))
        case other =>
          other
      }
  }

  def validateBasedOnIdentity[MT <: MetaTypes](cte: CTE[MT]): Unit = {
    new CTEValidator[MT]().validate(AvailableCTEs.empty[MT, Unit], cte)
  }
}
