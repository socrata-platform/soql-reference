package com.socrata.soql.analyzer2

import com.socrata.soql.functions.MonomorphicFunction

import com.socrata.soql.serialize.{ReadBuffer, WriteBuffer, Readable, Writable}

class SoQLAnalysis[MT <: MetaTypes] private (
  val labelProvider: LabelProvider,
  val statement: Statement[MT],
  private val usesSelectListReferences: Boolean
) extends MetaTypeHelper[MT] {
  private[analyzer2] def this(labelProvider: LabelProvider, statement: Statement[MT]) =
    this(labelProvider, statement, false)

  /** For rewrite trivial table parameters ("trivial" means "column
    * references and literals") so that they're inlined into the table
    * query rather than being held out-of-line in a temporary VALUES
    * form.  If all such table parameters are eliminated, this also
    * removes an intermediate LATERAL JOIN.
    *
    * Technically, this does this inlining for anything that has the
    * same shape as an expanded table-function, but that's still a
    * valid transformation. */
  def inlineTrivialParameters(isLiteralTrue: Expr[MT] => Boolean): SoQLAnalysis[MT] = {
    withoutSelectListReferences { self =>
      val nlp = self.labelProvider.clone()
      self.copy(
        labelProvider = nlp,
        statement = rewrite.InlineTrivialParameters(isLiteralTrue, self.statement)
      )
    }
  }

  /** Rewrite the analysis plumbing through enough information to
    * preserve table-ordering (except across joins and aggregates,
    * which of course destroy ordering). */
  def preserveOrdering: SoQLAnalysis[MT] = {
    withoutSelectListReferences { self =>
      val nlp = self.labelProvider.clone()
      self.copy(
        labelProvider = nlp,
        statement = rewrite.PreserveOrdering(nlp, self.statement)
      )
    }
  }

  /** Eliminate simple passthrough selects that don't have any logic
    * associated with them. */
  def removeTrivialSelects: SoQLAnalysis[MT] = {
    // No need to remove select list references here because the
    // things we're removing don't have the parts that would include
    // them
    copy(
      labelProvider = labelProvider.clone(),
      statement = rewrite.RemoveTrivialSelects(statement)
    )
  }

  /** Attempt to impose an arbitrary total order on this query.  Will
    * preserve any ordering that exists, but also use the statement's
    * `unique` columns to impose an ordering if possible, or add
    * trailing ORDER BY clauses for each orderable column in the
    * select list which are not already present.
    */
  def imposeOrdering(isOrderable: CT => Boolean): SoQLAnalysis[MT] = {
    withoutSelectListReferences { self =>
      val nlp = self.labelProvider.clone()
      self.copy(
        labelProvider = nlp,
        statement = rewrite.ImposeOrdering(nlp, isOrderable, rewrite.PreserveUnique(nlp, self.statement))
      )
    }
  }

  /** Simplify subselects on a best-effort basis. */
  def merge(and: MonomorphicFunction[CT]): SoQLAnalysis[MT] =
    copy(statement = new rewrite.Merger(and).merge(statement))

  /** Remove columns not actually used by the query */
  def removeUnusedColumns: SoQLAnalysis[MT] =
    withoutSelectListReferences { self =>
      self.copy(statement = rewrite.RemoveUnusedColumns(self.statement))
    }

  def removeUnusedOrderBy: SoQLAnalysis[MT] =
    copy(statement = rewrite.RemoveUnusedOrderBy(statement))

  /** Rewrite expressions in group/order/distinct clauses which are
    * identical to expressions in the select list to use select-list
    * indexes instead. */
  def useSelectListReferences =
    if(usesSelectListReferences) this
    else copy(statement = rewrite.SelectListReferences.use(statement), usesSelectListReferences = true)

  /** Update limit/offset for paging purposes.  You might want to
    * imposeOrdering before doing this to ensure the paging is
    * meaningful.  Pages are zero-based (i.e., they're an offset
    * rather than a page number). */
  def page(pageSize: BigInt, pageOffset: BigInt) = {
    addLimitOffset(Some(pageSize), Some(pageOffset * pageSize))
  }

  /** Update limit/offset.  You might want to imposeOrdering before
    * doing this to ensure the bounds are meaningful. */
  def addLimitOffset(limit: Option[BigInt], offset: Option[BigInt]) = {
    val nlp = labelProvider.clone()
    copy(
      labelProvider = nlp,
      statement = rewrite.AddLimitOffset(nlp, statement, limit, offset)
    )
  }

  private def copy[MT2 <: MetaTypes](
    labelProvider: LabelProvider = this.labelProvider,
    statement: Statement[MT2] = this.statement,
    usesSelectListReferences: Boolean = this.usesSelectListReferences
  ) =
    new SoQLAnalysis[MT2](labelProvider, statement, usesSelectListReferences)

  private def withoutSelectListReferences(f: SoQLAnalysis[MT] => SoQLAnalysis[MT]) =
    if(usesSelectListReferences) {
      f(this.copy(statement = rewrite.SelectListReferences.unuse(statement))).useSelectListReferences
    } else {
      f(this)
    }
}

object SoQLAnalysis {
  implicit def serialize[MT <: MetaTypes](implicit ev: Writable[Statement[MT]]): Writable[SoQLAnalysis[MT]] =
    new Writable[SoQLAnalysis[MT]] {
      def writeTo(buffer: WriteBuffer, analysis: SoQLAnalysis[MT]): Unit = {
        buffer.write(analysis.labelProvider)
        buffer.write(analysis.statement)
        buffer.write(analysis.usesSelectListReferences)
      }
    }

  implicit def deserialize[MT <: MetaTypes](implicit ev: Readable[Statement[MT]]): Readable[SoQLAnalysis[MT]] =
    new Readable[SoQLAnalysis[MT]] {
      def readFrom(buffer: ReadBuffer): SoQLAnalysis[MT] =
        new SoQLAnalysis(
          buffer.read[LabelProvider](),
          buffer.read[Statement[MT]](),
          buffer.read[Boolean]()
        )
    }
}
