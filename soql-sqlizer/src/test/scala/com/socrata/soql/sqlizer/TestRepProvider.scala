package com.socrata.soql.sqlizer

import scala.{collection => sc}

import java.sql.ResultSet

import com.rojoma.json.v3.ast.JString

import com.socrata.prettyprint.prelude._
import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.Provenance
import com.socrata.soql.sqlizer._

class TestRepProvider(
  override val namespace: SqlNamespaces[TestHelper.TestMT],
  override val toProvenance: types.ToProvenance[TestHelper.TestMT],
  override val isRollup: types.DatabaseTableName[TestHelper.TestMT] => Boolean
) extends Rep.Provider[TestHelper.TestMT] {
  type TestMT = TestHelper.TestMT

  override val exprSqlFactory = TestHelper.TestExprSqlFactory

  override def mkStringLiteral(s: String) =
    Doc(JString(s).toString)

  override def mkTextLiteral(s: String) =
    d"text" +#+ mkStringLiteral(s)

  override def mkByteaLiteral(bytes: Array[Byte]): Doc =
    d"bytea" +#+ mkStringLiteral(bytes.iterator.map { b => "%02x".format(b & 0xff) }.mkString("\\x", "", ""))

  def apply(typ: TestType): Rep = reps(typ)

  val reps = Map[TestType, Rep](
    TestID -> new ProvenancedRep(TestID, d"bigint") {
      override def provenanceOf(e: LiteralValue) = {
        val rawId = e.value.asInstanceOf[TestID]
        Set(rawId.provenance)
      }

      override def convertToText(e: ExprSql) = None

      override def literal(e: LiteralValue) = {
        val rawId = e.value.asInstanceOf[TestID]

        val provLit = rawId.provenance match {
          case None => d"null :: text"
          case Some(Provenance(s)) => mkTextLiteral(s)
        }
        val numLit = Doc(rawId.value) +#+ d":: bigint"

        exprSqlFactory(Seq(provLit, numLit), e)
      }

      override def compressedSubColumns(table: String, column: ColumnLabel) = {
        val sourceName = compressedDatabaseColumn(column)
        val Seq(provenancedName, dataName) = expandedDatabaseColumns(column)
        Seq(
          d"(" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 0 AS" +#+ provenancedName,
          d"((" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 1) :: bigint AS" +#+ dataName,
        )
      }

      override def compressedDatabaseType = d"jsonb"

      override protected def doExtractExpanded(rs: ResultSet, dbCol: Int): CV = {
        ???
      }

      override protected def doExtractCompressed(rs: ResultSet, dbCol: Int): CV = {
        ???
      }

      override protected def doExtractExpandedFromCsv(row: sc.Seq[Option[String]], dbCol: Int): CV = {
        ???
      }

      override protected def doExtractCompressedFromCsv(value: Option[String]): CV = {
        ???
      }

      override def ingressRep(tableName: DatabaseTableName, label: ColumnLabel) = {
        ???
      }
    },
    TestText -> new SingleColumnRep(TestText, d"text") {
      override def literal(e: LiteralValue) = {
        val TestText(s) = e.value
        exprSqlFactory(mkTextLiteral(s), e)
      }

      override def convertToText(e: ExprSql) = Some(e)

      override protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        ???
      }

      override protected def doExtractFromCsv(value: Option[String]): CV = {
        ???
      }

      override def ingressRep(tableName: DatabaseTableName, label: ColumnLabel) = {
        ???
      }
    },
    TestNumber -> new SingleColumnRep(TestNumber, d"numeric") {
      override def literal(e: LiteralValue) = {
        val TestNumber(n) = e.value
        exprSqlFactory(Doc(n.toString) +#+ d"::" +#+ sqlType, e)
      }

      override def convertToText(e: ExprSql) = {
        val converted = d"(" ++ e.compressed.sql ++ d") :: text"
        Some(exprSqlFactory(converted, e.expr))
      }

      override protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        ???
      }

      override protected def doExtractFromCsv(value: Option[String]): CV = {
        ???
      }

      override def ingressRep(tableName: DatabaseTableName, label: ColumnLabel) = {
        ???
      }
    },
    TestBoolean -> new SingleColumnRep(TestBoolean, d"boolean") {
      override def literal(e: LiteralValue) = {
        val TestBoolean(b) = e.value
        exprSqlFactory(if(b) d"true" else d"false", e)
      }

      override def convertToText(e: ExprSql) = None

      override protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        ???
      }

      override protected def doExtractFromCsv(value: Option[String]): CV = {
        ???
      }

      override def ingressRep(tableName: DatabaseTableName, label: ColumnLabel) = {
        ???
      }
    },

    TestCompound -> new CompoundColumnRep(TestCompound) {
      override def nullLiteral(e: NullLiteral) =
        exprSqlFactory(Seq(d"null :: text", d"null :: numeric"), e)

      override def convertToText(e: ExprSql) = None

      override def physicalColumnCount = 2

      override def physicalDatabaseColumns(name: ColumnLabel) = {
        Seq(namespace.columnName(name, "a"), namespace.columnName(name, "b"))
      }

      override def physicalDatabaseTypes = Seq(d"text", d"numeric")

      override def compressedSubColumns(table: String, column: ColumnLabel) = {
        val sourceName = compressedDatabaseColumn(column)
        val Seq(aName, bName) = expandedDatabaseColumns(column)
        Seq(
          d"(" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 0 AS" +#+ aName,
          d"((" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 1) :: numeric AS" +#+ bName,
        )
      }

      override def compressedDatabaseType = d"jsonb"

      override def compressedDatabaseColumn(name: ColumnLabel) =
        namespace.columnName(name)

      override def literal(e: LiteralValue) = {
        val cmp@TestCompound(_, _) = e.value

        cmp match {
          case TestCompound(None, None) =>
            exprSqlFactory(Seq(d"null :: text", d"null :: numeric"), e)
          case TestCompound(a, b) =>
            val aLit = a match {
              case Some(n) => mkTextLiteral(n)
              case None => d"null :: text"
            }
            val bLit = b match {
              case Some(n) => Doc(n.toString) +#+ d" :: numeric"
              case None => d"null :: numeric"
            }

            exprSqlFactory(Seq(aLit, bLit), e)
        }
      }

      override def subcolInfo(field: String) =
        field match {
          case "a" => SubcolInfo[TestMT](TestCompound, 0, "text", TestText, _.parenthesized +#+ d"->> 0")
          case "b" => SubcolInfo[TestMT](TestCompound, 1, "numeric", TestNumber, { e => (e.parenthesized +#+ d"->> 1").parenthesized +#+ d":: numeric" }) // ->> because it handles jsonb null => sql null
        }

      override protected def doExtractExpanded(rs: ResultSet, dbCol: Int): CV = {
        ???
      }

      override protected def doExtractCompressed(rs: ResultSet, dbCol: Int): CV = {
        ???
      }

      override protected def doExtractExpandedFromCsv(row: sc.Seq[Option[String]], dbCol: Int): CV = {
        ???
      }

      override protected def doExtractCompressedFromCsv(value: Option[String]): CV = {
        ???
      }

      override def ingressRep(tableName: DatabaseTableName, label: ColumnLabel) = {
        ???
      }
    }
  )
}
