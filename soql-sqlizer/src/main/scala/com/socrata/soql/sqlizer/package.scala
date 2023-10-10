package com.socrata.soql

import scala.collection.compat._

import com.socrata.prettyprint.prelude._
import com.socrata.soql.analyzer2._
import com.socrata.soql.collection.OrderedMap

package object sqlizer {
  implicit class AugmentSeq[T](private val underlying: Seq[Doc[T]]) extends AnyVal {
    def commaSep: Doc[T] =
      underlying.concatWith { (l, r) =>
        l ++ Doc.Symbols.comma ++ Doc.lineSep ++ r
      }

    def funcall[U >: T](functionName: Doc[U]): Doc[U] =
      ((functionName ++ d"(" ++ Doc.lineCat ++ commaSep).nest(2) ++ Doc.lineCat ++ d")").group

    def parenthesized: Doc[T] =
      ((d"(" ++ Doc.lineCat ++ commaSep).nest(2) ++ Doc.lineCat ++ d")").group
  }

  implicit class AugmentDoc[T](private val underlying: Doc[T]) extends AnyVal {
    def funcall[U >: T](functionName: Doc[U]): Doc[U] =
      Seq(underlying).funcall(functionName)

    def parenthesized: Doc[T] =
      Seq(underlying).parenthesized
  }

  implicit class LayoutSingleLine[T](private val doc: Doc[T]) extends AnyVal {
    def layoutSingleLine = {
      doc.group.layoutPretty(LayoutOptions(PageWidth.Unbounded))
    }
  }

  implicit class AugmentedIterableOnce[T](private val underlying: IterableOnce[T]) extends AnyVal {
    def foldMap[S, U](initialValue: S)(f: (S, T) => (S, U)): Iterator[U] = {
      new Iterator[U] {
        val iter = underlying.iterator
        var state = initialValue

        def hasNext = iter.hasNext
        def next(): U = {
          val (newState, result) = f(state, iter.next())
          state = newState
          result
        }
      }
    }
  }

  type AugmentedSchema[MT <: MetaTypes with MetaTypesExt] = OrderedMap[types.ColumnLabel[MT], AugmentedType[MT]]
  type AvailableSchemas[MT <: MetaTypes with MetaTypesExt] = Map[AutoTableLabel, AugmentedSchema[MT]]
}
