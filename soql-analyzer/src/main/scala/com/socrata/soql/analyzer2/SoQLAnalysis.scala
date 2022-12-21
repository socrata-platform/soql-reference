package com.socrata.soql.analyzer2

import com.socrata.soql.functions.MonomorphicFunction

import com.socrata.soql.analyzer2.serialization.{ReadBuffer, WriteBuffer, Readable, Writable}

class SoQLAnalysis[RNS, CT, CV] private (
  val labelProvider: LabelProvider,
  val statement: Statement[RNS, CT, CV],
  private val usesSelectListReferences: Boolean
) {
  private[analyzer2] def this(labelProvider: LabelProvider, statement: Statement[RNS, CT, CV]) =
    this(labelProvider, statement, false)

  /** Rewrite the analysis plumbing through enough information to
    * preserve table-ordering (except across joins and aggregates,
    * which of course destroy ordering). */
  def preserveOrdering: SoQLAnalysis[RNS, CT, CV] = {
    withoutSelectListReferences { self =>
      val nlp = self.labelProvider.clone()
      self.copy(
        labelProvider = nlp,
        statement = rewrite.PreserveOrdering(nlp, self.statement)
      )
    }
  }

  /** Simplify subselects on a best-effort basis. */
  def merge(and: MonomorphicFunction[CT]): SoQLAnalysis[RNS, CT, CV] =
    copy(statement = new rewrite.Merger(and).merge(statement))

  /** Remove columns not actually used by the query */
  def removeUnusedColumns: SoQLAnalysis[RNS, CT, CV] =
    withoutSelectListReferences { self =>
      self.copy(statement = rewrite.RemoveUnusedColumns(self.statement))
    }

  /** Rewrite expressions in group/order/distinct clauses which are
    * identical to expressions in the select list to use select-list
    * indexes instead. */
  def useSelectListReferences =
    if(usesSelectListReferences) this
    else copy(statement = rewrite.SelectListReferences.use(statement), usesSelectListReferences = true)

  private def copy[RNS2, CT2, CV2](
    labelProvider: LabelProvider = this.labelProvider,
    statement: Statement[RNS2, CT2, CV2] = this.statement,
    usesSelectListReferences: Boolean = this.usesSelectListReferences
  ) =
    new SoQLAnalysis(labelProvider, statement, usesSelectListReferences)

  private def withoutSelectListReferences(f: SoQLAnalysis[RNS, CT, CV] => SoQLAnalysis[RNS, CT, CV]) =
    if(usesSelectListReferences) {
      f(this.copy(statement = rewrite.SelectListReferences.unuse(statement))).useSelectListReferences
    } else {
      f(this)
    }
}

object SoQLAnalysis {
  implicit def serialize[RNS, CT, CV](implicit ev: Writable[Statement[RNS, CT, CV]]): Writable[SoQLAnalysis[RNS, CT, CV]] =
    new Writable[SoQLAnalysis[RNS, CT, CV]] {
      def writeTo(buffer: WriteBuffer, analysis: SoQLAnalysis[RNS, CT, CV]): Unit = {
        buffer.write(analysis.labelProvider)
        buffer.write(analysis.statement)
        buffer.write(analysis.usesSelectListReferences)
      }
    }

  implicit def deserialize[RNS, CT, CV](implicit ev: Readable[Statement[RNS, CT, CV]]): Readable[SoQLAnalysis[RNS, CT, CV]] =
    new Readable[SoQLAnalysis[RNS, CT, CV]] {
      def readFrom(buffer: ReadBuffer): SoQLAnalysis[RNS, CT, CV] =
        new SoQLAnalysis(
          buffer.read[LabelProvider](),
          buffer.read[Statement[RNS, CT, CV]](),
          buffer.read[Boolean]()
        )
    }
}
