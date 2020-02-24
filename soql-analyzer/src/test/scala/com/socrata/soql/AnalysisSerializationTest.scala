package com.socrata.soql

import org.scalatest.{FunSpec, MustMatchers}
import com.socrata.soql.environment.{ColumnName, DatasetContext, TypeName, ResourceName, Qualified, TableRef}
import com.socrata.soql.types._
import com.socrata.soql.typed.CoreExpr
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.Base64
import com.rojoma.json.v3.ast.{JArray, JString}
import com.rojoma.json.v3.io.{CompactJsonWriter, JsonReader}

import com.socrata.soql.collection.NonEmptySeq

class AnalysisSerializationTest extends FunSpec with MustMatchers {
  implicit val datasetCtx = new DatasetContext[TestType] {
    val schema = com.socrata.soql.collection.OrderedMap(
      ColumnName(":id") -> TestNumber,
      ColumnName(":updated_at") -> TestFixedTimestamp,
      ColumnName(":created_at") -> TestFixedTimestamp,
      ColumnName("name_last") -> TestText,
      ColumnName("name_first") -> TestText,
      ColumnName("visits") -> TestNumber,
      ColumnName("last_visit") -> TestFixedTimestamp,
      ColumnName("address") -> TestLocation,
      ColumnName("balance") -> TestMoney,
      ColumnName("object") -> TestObject,
      ColumnName("array") -> TestArray
    )
  }

  val joinCtx = new DatasetContext[TestType] {
    val schema = com.socrata.soql.collection.OrderedMap(
      ColumnName(":id") -> TestNumber,
      ColumnName(":updated_at") -> TestFixedTimestamp,
      ColumnName(":created_at") -> TestFixedTimestamp,
      ColumnName("name_last") -> TestText
    )
  }

  val primary = ResourceName("primary")
  val datasetCtxMap = Map(
    primary -> datasetCtx,
    ResourceName("aaaa-aaaa") -> joinCtx
  )

  def tableFinder(in: Set[ResourceName]) = datasetCtxMap.filterKeys(in)

  val analyzer = new SoQLAnalyzer(TestTypeInfo, TestFunctionInfo, tableFinder)

  def serializeColumnName(c: ColumnName) = c.name
  def deserializeColumnName(s: String) = ColumnName(s)

  def serializeQualifiedColumnName(c: Qualified[ColumnName]) =
    CompactJsonWriter.toString(JArray(Seq(JString(TableRef.serialize(c.table)),
                                          JString(c.columnName.name))))
  def deserializeQualifiedColumnName(s: String) =
    JsonReader.fromString(s) match {
      case JArray(Seq(JString(tr), JString(cn))) =>
        Qualified(TableRef.deserialize(tr).get, ColumnName(cn))
      case _ =>
        fail("QCN isn't a two-element array of strings?")
    }

  def serializeTestType(t: TestType) = t.name.name
  def deserializeTestType(s: String): TestType = TestType.typesByName(TypeName(s))

  val serializer = new AnalysisSerializer[ColumnName, Qualified[ColumnName], TestType](serializeColumnName, serializeQualifiedColumnName, serializeTestType)
  val deserializer = new AnalysisDeserializer(deserializeColumnName, deserializeQualifiedColumnName, deserializeTestType, TestFunctions.functionsByIdentity)

  type Analysis = SoQLAnalysis[ColumnName, Qualified[ColumnName], TestType]

  val b64Decoder: Base64.Decoder = Base64.getDecoder

  def analyzeUnchained(query: String) = analyzer.analyzeUnchainedQuery(primary, query)
  def analyzeFull(query: String) = analyzer.analyzeFullQuery(primary, query)

  def testRoundtrip(query: String) = {
    val analysis = analyzeFull(query)
    SoQLAnalysis.assertSaneInputs(analysis, TableRef.Primary)

    val baos = new ByteArrayOutputStream
    serializer(baos, analysis)
    val result = deserializer(new ByteArrayInputStream(baos.toByteArray))

    SoQLAnalysis.assertSaneInputs(result, TableRef.Primary)
    result must equal(analysis)
  }

  def testV5(query: String, v5Encoded: String)(implicit convert : NonEmptySeq[Analysis] => NonEmptySeq[Analysis] = identity) = {
    val bytes = b64Decoder.decode(v5Encoded)
    val analysis = deserializer(new ByteArrayInputStream(bytes))
    SoQLAnalysis.assertSaneInputs(analysis, TableRef.Primary)
    analysis must equal (convert(analyzeFull(query)))
  }

  def hexdump(bs: Array[Byte], chunks: Int = 4, chunkSize: Int = 8, unprintable: Char = '\u00b7') = {
    val rowSize = chunks * chunkSize
    val sb = new StringBuilder()
    bs.grouped(rowSize).zipWithIndex.foreach { case (chunk, rowNum) =>
      sb.setLength(0)
      sb.append("%08x ".format(rowNum*rowSize))
      chunk.iterator.map { b => " %02x".format(b & 0xff) }.padTo(rowSize, "   ").grouped(chunkSize).map(_.mkString).addString(sb, " -")
      sb.append("  ")
      chunk.iterator.map { b => if(b > 31 && b < 127) b.toChar else unprintable }.padTo(rowSize, ' ').addString(sb, "|", "", "|")

      println(sb.toString())
    }
  }

  describe("simple unchained") {
    val query = "select :id"

    // it("serialize / deserialize equals analysis (v5)") {
    //   testV5(query, "BQMKc2VsZWN0IDppZAEGbnVtYmVyAgM6aWQAAQAAAQICAQAAAAEAAAEAAAEIAQcBAAACAAAAAAAAAAA=")
    // }

    it("serialize / deserialize equals analysis (v6)") {
      testRoundtrip(query)
    }
  }

  describe("complex unchained") {
    val query =
      """select :id as i, sum(balance)
        |   where visits > 0
        |   group by i, visits
        |   having sum_balance < 5
        |   order by i desc,
        |            sum(balance) null first
        |   search 'gnu'
        |   limit 5
        |   offset 10""".stripMargin

    // it("serialize / deserialize equals analysis (v5)") {
    //   testV5(query, "BREHYmFsYW5jZQgGbnVtYmVyAwNzdW0FBW1vbmV5BwZ2aXNpdHMKD251bWJlciB0byBtb25leQ0CMTAPAT4JAzppZAIBPAwLc3VtX2JhbGFuY2UEAWkAATUOwQFzZWxlY3QgOmlkIGFzIGksIHN1bShiYWxhbmNlKQogICB3aGVyZSB2aXNpdHMgPiAwCiAgIGdyb3VwIGJ5IGksIHZpc2l0cwogICBoYXZpbmcgc3VtX2JhbGFuY2UgPCA1CiAgIG9yZGVyIGJ5IGkgZGVzYywKICAgICAgICAgICAgc3VtKGJhbGFuY2UpIG51bGwgZmlyc3QKICAgc2VhcmNoICdnbnUnCiAgIGxpbWl0IDUKICAgb2Zmc2V0IDEwAQFhBgNnbnUQATALAgAABAECBwcDAwMICAICCgoEDQADCQEGAwEMAQYHAgUBBgcAAQEAAgAAAQgBBwEAAgMBAAESAREGAAESAREAAQABFgEVAQAIBwABAAIKAScGAAIRAS4BAgACCgEnAQAKAwACEwEwAwsDAgABCAEHAQACAwADEAFBAQAKAwEABAsBUgYABBcBXgICAAESAREGAAESAREAAQABFgEVAQAIBwAEGQFgBgAEGQFgAwEABBkBYAMOAwIAAQgBBwEAAgMAAAAGDQGCAQYABg0BggEAAQAGEQGGAQEACAcBAAEOAQ8BEA==")
    // }

    it("serialize / deserialize equals analysis (v6)") {
      testRoundtrip(query)
    }
  }

  describe("complex full") {
    val query =
      """select :id, balance as amt, visits |>
        |  select :id as i, sum(amt)
        |  where visits > 0
        |  group by i, visits
        |  having sum_amt < 5
        |  order by i desc,
        |           sum(amt) null first
        |  search 'gnu'
        |  limit 5
        |  offset 10""".stripMargin

    // it("serialize / deserialize equals analysis (v5)") {
    //   testV5(query, "BRIHYmFsYW5jZQQGbnVtYmVyAgNzdW0JA2FtdAPVAXNlbGVjdCA6aWQsIGJhbGFuY2UgYXMgYW10LCB2aXNpdHMgfD4KICBzZWxlY3QgOmlkIGFzIGksIHN1bShhbXQpCiAgd2hlcmUgdmlzaXRzID4gMAogIGdyb3VwIGJ5IGksIHZpc2l0cwogIGhhdmluZyBzdW1fYW10IDwgNQogIG9yZGVyIGJ5IGkgZGVzYywKICAgICAgICAgICBzdW0oYW10KSBudWxsIGZpcnN0CiAgc2VhcmNoICdnbnUnCiAgbGltaXQgNQogIG9mZnNldCAxMAEFbW9uZXkFBnZpc2l0cwYHc3VtX2FtdAgCMTAQAT4LAzppZAABPA0BaQcPbnVtYmVyIHRvIG1vbmV5DgE1DwFhCgNnbnURATAMBQgEBwMAAAMBBgICBQUCAgQEBAAAAwMGBgQLAQoCAQkBCgUADQEKBQIOAAMCAAADAAABCAEHAQAAAgEAAQ0BDAEABAUCAAEdARwBAAYCAAAAAAAAAAABAAIDAAIKAS8BAAACBAACFAE5BgACFAE5AAEAAhgBPQEAAwUAAQADCQFKBgADEAFRAQIAAwkBSgEABgIAAxIBUwMMAgIAAgoBLwEAAAIABA8BYwEABgIBAAUKAXMGAAUSAXsCAgACFAE5BgACFAE5AAEAAhgBPQEAAwUABRQBfQYABRQBfQMBAAUUAX0DDwICAAIKAS8BAAACAAAABwwBnQEGAAcMAZ0BAAEABxABoQEBAAMFAQABDwEQARE=")
    // }

    it("serialize / deserialize equals analysis (v6)") {
      testRoundtrip(query)
    }
  }

  describe("simple join") {
    val query = "select @aaaa-aaaa.name_last join @aaaa-aaaa on name_last = @aaaa-aaaa.name_last"

    // it("serialize / deserialize equals analysis (v5)") {
    //   testV5(query, "BQVPc2VsZWN0IEBhYWFhLWFhYWEubmFtZV9sYXN0IGpvaW4gQGFhYWEtYWFhYSBvbiBuYW1lX2xhc3QgPSBAYWFhYS1hYWFhLm5hbWVfbGFzdAEBPQMBYQQJbmFtZV9sYXN0AAR0ZXh0AgEAAAECAgEAAAEDAQQCAAEAAAEAAAEIAQcBAQpfYWFhYS1hYWFhAAIBBEpPSU4KX2FhYWEtYWFhYQAAAAEwAS8GAAE6ATkAAgABMAEvAQAAAgABPAE7AQEKX2FhYWEtYWFhYQACAAAAAAAAAA==") { analyses =>
    //     analyses.map { analysis =>
    //       analysis.copy(selection =
    //                       analysis.selection.get(ColumnName("aaaa_aaaa_name_last")) match {
    //                         case None =>
    //                           analysis.selection
    //                         case Some(expr) =>
    //                           analysis.selection - ColumnName("aaaa_aaaa_name_last") + (ColumnName("name_last") -> expr)
    //                       })
    //     }
    //   }
    // }

    it("serialize / deserialize equals analysis (v6)") {
      testRoundtrip(query)
    }
  }

  describe("simple join 2") {
    val query = "select :id, @a.name_last join @aaaa-aaaa as a on name_last = @a.name_last"

    // it("serialize / deserialize equals analysis (v5)") {
    //   testV5(query, "BQdJc2VsZWN0IDppZCwgQGEubmFtZV9sYXN0IGpvaW4gQGFhYWEtYWFhYSBhcyBhIG9uIG5hbWVfbGFzdCA9IEBhLm5hbWVfbGFzdAEBPQUGbnVtYmVyAgM6aWQAAWEGCW5hbWVfbGFzdAMEdGV4dAQCAAADAQIEBAICAgAAAwMBBQEGBAABAAACAAABCAEHAQAAAgEAAQ0BDAEBAl9hAwQBBEpPSU4KX2FhYWEtYWFhYQECX2EAAAEyATEGAAE8ATsAAgABMgExAQADBAABPgE9AQECX2EDBAAAAAAAAAA=") { analyses =>
    //     analyses.map { analysis =>
    //       analysis.copy(selection =
    //                       analysis.selection.get(ColumnName("a_name_last")) match {
    //                         case None =>
    //                           analysis.selection
    //                         case Some(expr) =>
    //                           analysis.selection - ColumnName("a_name_last") + (ColumnName("name_last") -> expr)
    //                       })
    //     }
    //   }
    // }

    it("serialize / deserialize equals analysis (v6)") {
      testRoundtrip(query)
    }
  }

  describe("complex join") {
    val query = "select :id, balance, @b.name_last join (select * from @aaaa-aaaa as a |> select name_last) as b on name_last = @b.name_last |> select :id"

    // it("serialize / deserialize equals analysis (v5)") {
    //   testV5(query, "BQkFbW9uZXkECW5hbWVfbGFzdAUHYmFsYW5jZQMGbnVtYmVyAgM6aWQAAWEIjAFzZWxlY3QgOmlkLCBiYWxhbmNlLCBAYi5uYW1lX2xhc3Qgam9pbiAoc2VsZWN0ICogZnJvbSBAYWFhYS1hYWFhIGFzIGEgfD4gc2VsZWN0IEBhLm5hbWVfbGFzdCkgYXMgYiBvbiBuYW1lX2xhc3QgPSBAYi5uYW1lX2xhc3QgfD4gc2VsZWN0IDppZAEEdGV4dAYBPQcDAwEAAAUCAwQEBgYCAgMDAwAABQUBBwEIBgACAAADAAABCAEHAQAAAgEAAQ0BDAEAAwQCAAEWARUBAQJfYgUGAQRKT0lOCl9hYWFhLWFhYWEBAl9hAQIAAAECAAEwAS8BAAUGAAAAAAAAAAAAAAECAAFRAVABAQJfYQUGAAAAAAAAAAACX2IAAWcBZgYAAXEBcAACAAFnAWYBAAUGAAFzAXIBAQJfYgUGAAAAAAAAAAAAAQAAAYoBAYkBAQAAAgAAAAAAAAAA") { analyses =>
    //     analyses.map { analysis =>
    //       analysis.copy(selection =
    //                       analysis.selection.get(ColumnName("b_name_last")) match {
    //                         case None =>
    //                           analysis.selection
    //                         case Some(expr) =>
    //                           analysis.selection - ColumnName("b_name_last") + (ColumnName("name_last") -> expr)
    //                       })
    //     }
    //   }
    // }

    it("serialize / deserialize equals analysis (v6)") {
      testRoundtrip(query)
    }
  }
}
