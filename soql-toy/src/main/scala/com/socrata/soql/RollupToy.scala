package com.socrata.soql.analyzer2

import java.nio.charset.StandardCharsets

import com.rojoma.json.v3.util.{AutomaticJsonDecodeBuilder, JsonUtil, AllowMissing}
import com.socrata.prettyprint.prelude._
import com.socrata.soql.environment.{ColumnName, TypeName, ResourceName, Provenance, ScopedResourceName}
import com.socrata.soql.types._
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo2}
import com.socrata.soql.analyzer2.mocktablefinder._
import com.socrata.soql.analyzer2.rollup.{RollupRewriter, RollupInfo}
import com.socrata.soql.analyzer2.rewrite
import com.socrata.soql.stdlib.analyzer2.rollup.SoQLRollupExact
import com.socrata.soql.stdlib.analyzer2.SoQLRewritePassHelpers
import com.socrata.soql.util.LazyToString

object RollupToy extends (Array[String] => Unit) {
  case class Rollup(
    baseTable: String,
    soql: String,
    @AllowMissing(value = "Nil")
    rewritePasses: Seq[Seq[rewrite.Pass]]
  )
  case class Config(
    tables: Map[String, Map[String, TypeName]],
    rollups: Map[String, Rollup],
    baseTable: String,
    query: String,
    @AllowMissing(value = "Nil")
    rewritePasses: Seq[Seq[rewrite.Pass]]
  )

  implicit val rollupDecode = AutomaticJsonDecodeBuilder[Rollup]
  implicit val configDecode = AutomaticJsonDecodeBuilder[Config]

  trait MT extends MetaTypes {
    type ColumnType = SoQLType
    type ColumnValue = SoQLValue
    type DatabaseTableNameImpl = String
    type DatabaseColumnNameImpl = String
    type ResourceNameScope = Unit
  }

  val typeInfo = new SoQLTypeInfo2[MT]

  val toProvenance = new ToProvenance[String] {
    override def toProvenance(dtn: DatabaseTableName[String]): Provenance =
      Provenance(dtn.name)
  }

  val analyzer = new SoQLAnalyzer[MT](typeInfo, SoQLFunctionInfo, toProvenance)

  val rewritePassHelpers = new SoQLRewritePassHelpers[MT]

  implicit val cvHasDoc = new HasDoc[SoQLValue] {
    def docOf(cv: SoQLValue) = Doc(cv.toString)
  }

  val stringifier = new Stringifier[MT] {
    def statement(stmt: Statement) = LazyToString(stmt.debugDoc)
    def from(from: From) = LazyToString(from.debugDoc)
    def expr(expr: Expr) = LazyToString(expr.debugDoc(implicitly, implicitly))
  }

  val rollupExact = new SoQLRollupExact[MT](stringifier)

  val transformManager = new TransformManager[MT, String](rollupExact, rewritePassHelpers, stringifier)

  def apply(args: Array[String]) = {
    for(file <- args) {
      val config = JsonUtil.readJsonFile[Config](file, StandardCharsets.UTF_8) match {
        case Right(c) => c
        case Left(e) => throw new Exception(e.english)
      }

      val tf = MockTableFinder[MT](
        config.tables.map { case (name, schema) =>
          ((), name) -> D(schema.map { case (cn, tn) => cn -> typeInfo.typeFor(tn).getOrElse(throw new Exception("Unknown type " + tn)) }.toSeq : _*)
        }.toSeq : _*
      )

      val rollups = config.rollups.iterator.map { case (name, Rollup(baseTable, soql, rewritePasses)) =>
        val foundTables = tf.findTables((), ResourceName(baseTable), soql, Map.empty) match {
          case Right(ft) => ft
          case Left(err) => throw new Exception(err.toString)
        }

        val analysis = analyzer(foundTables, UserParameters.emptyFor(foundTables)) match {
          case Right(a) => a
          case Left(err) => throw new Exception(err.toString)
        }

        new RollupInfo[MT, String] {
          override val id = name
          override val statement = analysis.applyPasses(rewritePasses.flatten, rewritePassHelpers).statement
          override val resourceName = ScopedResourceName((), ResourceName(s"rollup:$name"))
          override val canonicalName = CanonicalName(s"rollup:$name")
          override val databaseName = DatabaseTableName(name)
          override def databaseColumnNameOfIndex(idx: Int) = DatabaseColumnName("c" + idx)
        }
      }.toVector

      val foundTables = tf.findTables((), ResourceName(config.baseTable), config.query, Map.empty) match {
        case Right(ft) => ft
        case Left(err) => throw new Exception(err.toString)
      }

      val analysis = analyzer(foundTables, UserParameters.emptyFor(foundTables)) match {
        case Right(a) => a
        case Left(err) => throw new Exception(err.toString)
      }

      println("For query:")
      println(s"  ${analysis.statement.debugStr.replaceAll("\n", "\n  ")}")
      println("Found rewrite-candidates:")
      for((analysis, rids) <- transformManager(analysis, rollups, config.rewritePasses)) {
        println(s"  Using rollups: ${rids.mkString("[", ", ", "]")}")
        println(s"    ${analysis.statement.debugStr.replaceAll("\n", "\n    ")}")
      }
    }
  }
}
