package com.socrata.soql

import org.scalatest.{FunSpec, MustMatchers}
import com.socrata.soql.environment.{ColumnName, DatasetContext, TableName, TypeName}
import com.socrata.soql.types._
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.Base64

import com.socrata.NonEmptySeq

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

  implicit val datasetCtxMap = Map(
    TableName.PrimaryTable.qualifier -> datasetCtx,
    "_aaaa-aaaa" -> joinCtx,
    "_a" -> joinCtx
  )

  val analyzer = new SoQLAnalyzer(TestTypeInfo, TestFunctionInfo)

  def serializeColumnName(columnName: ColumnName) = columnName.name
  def deserializeColumnName(s: String): ColumnName = ColumnName(s)

  def serializeTestType(t: TestType) = t.name.name
  def deserializeTestType(s: String): TestType = TestType.typesByName(TypeName(s))

  val serializer = new AnalysisSerializer[ColumnName, TestType](serializeColumnName, serializeTestType)
  val deserializer = new AnalysisDeserializer(deserializeColumnName, deserializeTestType, TestFunctions.functionsByIdentity)

  type Analysis = SoQLAnalysis[ColumnName, TestType]

  val b64Decoder: Base64.Decoder = Base64.getDecoder

  def analyzeUnchained(query: String) = analyzer.analyzeUnchainedQuery(query)
  def analyzeFull(query: String) = analyzer.analyzeFullQuery(query)

  def testV5Unchained(query: String) = {
    val analysis = analyzeUnchained(query)
    val baos = new ByteArrayOutputStream
    serializer.unchainedSerializer(baos, analysis)
    deserializer(new ByteArrayInputStream(baos.toByteArray)) must equal(NonEmptySeq(analysis))
  }

  def testCompatibilityUnchained(query: String, v4Serializedb64: String) = {
    val analysis = analyzeUnchained(query)
    val baos = b64Decoder.decode(v4Serializedb64)
    deserializer(new ByteArrayInputStream(baos)) must equal(NonEmptySeq(analysis))
  }

  def testV5Full(query: String) = {
    val analysis = analyzeFull(query)
    val baos = new ByteArrayOutputStream
    serializer(baos, analysis)
    deserializer(new ByteArrayInputStream(baos.toByteArray)) must equal(analysis)
  }

  def testCompatibilityFull(query: String, v4Serializedb64: String) = {
    val analysis = analyzeFull(query)
    val baos = b64Decoder.decode(v4Serializedb64)
    deserializer(new ByteArrayInputStream(baos)) must equal(analysis)
  }

  describe("simple unchained") {
    val query = "select :id"
    val v4Serialized = "AAMKc2VsZWN0IDppZAEGbnVtYmVyAgM6aWQAAQAAAQICAQAAAAAAAQAAAQgBBwEAAAIAAAAAAAAAAAA="

    it("serialize / deserialize equals analysis (v5)") {
      testV5Unchained(query)
    }

    it("v4 deserializes to v5 analysis") {
      testCompatibilityUnchained(query, v4Serialized)
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
    val v4Serialized = "ABEHYmFsYW5jZQgGbnVtYmVyAwNzdW0FBW1vbmV5BwZ2aXNpdHMKD251bWJlciB0byBtb25leQ0CMTAPAT4JAzppZAIBPAwLc3VtX2JhbGFuY2UEAWkAATUOwQFzZWxlY3QgOmlkIGFzIGksIHN1bShiYWxhbmNlKQogICB3aGVyZSB2aXNpdHMgPiAwCiAgIGdyb3VwIGJ5IGksIHZpc2l0cwogICBoYXZpbmcgc3VtX2JhbGFuY2UgPCA1CiAgIG9yZGVyIGJ5IGkgZGVzYywKICAgICAgICAgICAgc3VtKGJhbGFuY2UpIG51bGwgZmlyc3QKICAgc2VhcmNoICdnbnUnCiAgIGxpbWl0IDUKICAgb2Zmc2V0IDEwAQFhBgNnbnUQATALAgAABAECBwcDAwMICAICCgoEDAEGBwIFAQYHAA0AAwkBBgMBAQACAAABCAEHAQACAwEAARIBEQYAARIBEQABAAEWARUBAAgHAAABAAIKAScGAAIRAS4BAgACCgEnAQAKAwACEwEwAwsDAQIAAQgBBwEAAgMAAxABQQEACgMBAAQLAVIGAAQXAV4CAgABEgERBgABEgERAAEAARYBFQEACAcABBkBYAYABBkBYAMBAAQZAWADDgMBAgABCAEHAQACAwAAAAYNAYIBBgAGDQGCAQABAAYRAYYBAQAIBwEAAQ4BDwEQ"

    it("serialize / deserialize equals analysis (v5)") {
      testV5Unchained(query)
    }

    ignore("v4 deserializes to v5 analysis") {
      testCompatibilityUnchained(query, v4Serialized)
    }
  }

  describe("simple full") {
    val query = "select :id"
    val v4Serialized = "BAMKc2VsZWN0IDppZAEGbnVtYmVyAgM6aWQAAQAAAQICAQAAAAEAAAEAAAEIAQcBAAACAAAAAAAAAAAA"

    it("serialize / deserialize equals analysis (v5)") {
      testV5Full(query)
    }

    ignore("v4 deserializes to v5 analysis") {
      testCompatibilityFull(query, v4Serialized)
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
    val v4Serialized = "BBIHYmFsYW5jZQQGbnVtYmVyAgNzdW0JA2FtdAMFbW9uZXkFBnZpc2l0cwYHc3VtX2FtdAgCMTAQAT4LAzppZAABPA0BaQfuAXNlbGVjdCA6aWQsIGJhbGFuY2UgYXMgYW10LCB2aXNpdHMgfD4KICAgc2VsZWN0IDppZCBhcyBpLCBzdW0oYW10KQogICAgIHdoZXJlIHZpc2l0cyA+IDAKICAgICBncm91cCBieSBpLCB2aXNpdHMKICAgICBoYXZpbmcgc3VtX2FtdCA8IDUKICAgICBvcmRlciBieSBpIGRlc2MsCiAgICAgICAgICAgICAgc3VtKGFtdCkgbnVsbCBmaXJzdAogICAgIHNlYXJjaCAnZ251JwogICAgIGxpbWl0IDUKICAgICBvZmZzZXQgMTABD251bWJlciB0byBtb25leQ4BNQ8BYQoDZ251EQEwDAUIBAcDAAADAQYCAgUFAgIEBAQAAAMDBgYEDQEKBQIJAQoFAA4AAwsBCgIBAgAAAwAAAQgBBwEAAAIBAAENAQwBAAQFAgABHQEcAQAGAgAAAAAAAAAAAAEAAgMAAgsBMAEAAAIEAAIVAToGAAIVAToAAQACGQE+AQADBQAAAQADDAFOBgADEwFVAQIAAwwBTgEABgIAAxUBVwMMAgECAAILATABAAACAAQSAWoBAAYCAQAFDQF9BgAFFQGFAQICAAIVAToGAAIVAToAAQACGQE+AQADBQAFFwGHAQYABRcBhwEDAQAFFwGHAQMPAgECAAILATABAAACAAAABw8BrQEGAAcPAa0BAAEABxMBsQEBAAMFAQABDwEQARE="

    it("serialize / deserialize equals analysis (v5)") {
      testV5Full(query)
    }

    ignore("v4 deserializes to v5 analysis") {
      testCompatibilityFull(query, v4Serialized)
    }
  }

  describe("simple join") {
    val query = "select @aaaa-aaaa.name_last join @aaaa-aaaa on name_last = @a.name_last"
    val v4Serialized = "BAUBPQMBYQQJbmFtZV9sYXN0AAR0ZXh0AkdzZWxlY3QgQGFhYWEtYWFhYS5uYW1lX2xhc3Qgam9pbiBAYWFhYS1hYWFhIG9uIG5hbWVfbGFzdCA9IEBhLm5hbWVfbGFzdAEBAAABAgIBAAABAwEEAgABAAABAAABCAEHAQEKX2FhYWEtYWFhYQACAAEESk9JTgEAAAABCl9hYWFhLWFhYWEAAAAAAAAAAAAAATABLwYAAToBOQACAAEwAS8BAAACAAE8ATsBAQJfYQACAAAAAAAAAA=="

    it("serialize / deserialize equals analysis (v5)") {
      testV5Full(query)
    }

    ignore("v4 deserializes to v5 analysis") {
      testCompatibilityFull(query, v4Serialized)
    }
  }

  describe("simple join 2") {
    val query = "select :id, @a.name_last join @aaaa-aaaa as a on name_last = @a.name_last"
    val v4Serialized = "BAdJc2VsZWN0IDppZCwgQGEubmFtZV9sYXN0IGpvaW4gQGFhYWEtYWFhYSBhcyBhIG9uIG5hbWVfbGFzdCA9IEBhLm5hbWVfbGFzdAEBPQUGbnVtYmVyAgM6aWQAAWEGCW5hbWVfbGFzdAMEdGV4dAQCAAADAQIEBAICAgAAAwMBBQEGBAABAAACAAABCAEHAQAAAgEAAQ0BDAEBAl9hAwQAAQRKT0lOAQAAAAEKX2FhYWEtYWFhYQAAAAAAAAAAAQJfYQABMgExBgABPAE7AAIAATIBMQEAAwQAAT4BPQEBAl9hAwQAAAAAAAAA"

    it("serialize / deserialize equals analysis (v5)") {
      testV5Full(query)
    }

    ignore("v4 deserializes to v5 analysis") {
      testCompatibilityFull(query, v4Serialized)
    }
  }

  describe("complex join") {
    val query = "select :id, balance, @b.name_last join (select * from @aaaa-aaaa as a |> select @a.name_last) as b on name_last = @b.name_last |> select :id"

    it("serialize / deserialize equals analysis (v5)") {
      testV5Full(query)
    }

    // v4 does not parse this correctly and loses information,
    // so comparing it to v5 (which is more correct, having all the information) will fail
  }
}
