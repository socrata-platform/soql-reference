package com.socrata.soql.analyzer2.rollup

import org.scalatest.Assertions

import com.socrata.soql.analyzer2._
import com.socrata.soql.collection.NonEmptySeq
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.functions.MonomorphicFunction

trait RollupTestHelper extends TestHelper { this: Assertions =>
  object TestSemigroupRewriter extends SemigroupRewriter[TestMT] {
    private val Max = TestFunctions.Max.identity
    private val Sum = TestFunctions.Sum.identity
    private val Count = TestFunctions.Count.identity
    private val CountStar = TestFunctions.CountStar.identity

    override def apply(f: MonomorphicFunction): Option[Expr => Expr] = {
      f.function.identity match {
        case Max =>
          Some { max => AggregateFunctionCall[TestMT](TestFunctions.Max.monomorphic.get, Seq(max), false, None)(FuncallPositionInfo.None) }
        case Sum =>
          Some { sum => AggregateFunctionCall[TestMT](TestFunctions.Sum.monomorphic.get, Seq(sum), false, None)(FuncallPositionInfo.None) }
        case Count | CountStar =>
          Some { count => AggregateFunctionCall[TestMT](TestFunctions.Sum.monomorphic.get, Seq(count), false, None)(FuncallPositionInfo.None) }
        case _ =>
          None
      }
    }
  }

  object TestFunctionSubset extends SimpleFunctionSubset[TestMT] {
    val BottomByte = TestFunctions.BottomByte.monomorphic.get
    val BottomDWord = TestFunctions.BottomDWord.monomorphic.get

    override def funcallSubset(a: MonomorphicFunction, b: MonomorphicFunction) =
      (a, b) match {
        case (BottomByte, BottomDWord) =>
          Some(BottomByte)
        case _ =>
          None
      }
  }

  object TestFunctionSplitter extends FunctionSplitter[TestMT] {
    private val avg = (
      TestFunctions.Div.monomorphic.get,
      Seq(
        TestFunctions.Sum.monomorphic.get,
        MonomorphicFunction(TestFunctions.Count, Map("a" -> TestNumber))
      )
    )

    override def apply(f: MonomorphicFunction): Option[(MonomorphicFunction, Seq[MonomorphicFunction])] =
      if(f.function.identity == TestFunctions.Avg.identity) {
        Some(avg)
      } else {
        None
      }
  }

  object TestSplitAnd extends SplitAnd[TestMT] {
    val And = TestFunctions.And.monomorphic.get
    override def split(e: Expr) =
      e match {
        case FunctionCall(And, args) =>
          val nonEmptyArgs = NonEmptySeq.fromSeq(args).getOrElse {
            throw new Exception("AND with no arguments??")
          }
          nonEmptyArgs.flatMap(split)
        case other =>
          NonEmptySeq(other)
      }
    override def merge(e: NonEmptySeq[Expr]) = {
      e.reduceLeft { (acc, expr) =>
        FunctionCall[TestMT](And, Seq(acc, expr))(FuncallPositionInfo.None)
      }
    }
  }

  object TestRollupExact extends RollupExact[TestMT](
    TestSemigroupRewriter,
    TestFunctionSubset,
    TestFunctionSplitter,
    TestSplitAnd,
    Stringifier.pretty
  )

  class TestRollupInfo(
    val id: Int,
    val statement: Statement[TestMT],
    val resourceName: types.ScopedResourceName[TestMT],
    val databaseName: types.DatabaseTableName[TestMT]
  ) extends RollupInfo[TestMT, Int] {
    override def databaseColumnNameOfIndex(i: Int) = DatabaseColumnName(s"c${i+1}")
  }

  object TestRollupInfo {
    def apply(id: Int, name: String, tf: TableFinder[TestMT], soql: String): TestRollupInfo = {
      val Right(foundTables) = tf.findTables(0, soql, Map.empty)
      val analysis = analyzer(foundTables, UserParameters.empty) match {
        case Right(a) => a
        case Left(e) => fail(e.toString)
      }
      new TestRollupInfo(id, analysis.statement, ScopedResourceName(0, ResourceName(name)), DatabaseTableName(name))
    }
  }

  class TestRollupRewriter(
    labelProvider: LabelProvider,
    rollups: Seq[TestRollupInfo]
  ) extends RollupRewriter(labelProvider, TestRollupExact, rollups)
}
