package com.socrata.soql

import org.scalatest.{FunSpec, FunSuite, MustMatchers}
import com.socrata.soql.environment.{ColumnName, DatasetContext, TableName, TypeName}
import com.socrata.soql.types._
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, OutputStream}
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

  val allQueryOptions = """select :id, balance as amt, visits |>
    |  select :id as i, sum(amt)
    |  where visits > 0
    |  group by i, visits
    |  having sum_amt < 5
    |  order by i desc,
    |           sum(amt) null first
    |  search 'gnu'
    |  limit 5
    |  offset 10""".stripMargin

  val unchained =
    """select :id as i, sum(balance)
      |   where visits > 0
      |   group by i, visits
      |   having sum_balance < 5
      |   order by i desc,
      |            sum(balance) null first
      |   search 'gnu'
      |   limit 5
      |   offset 10""".stripMargin

  val join = "select :id, balance, @b.name_last join (select * from @aaaa-aaaa as a |> select @a.name_last) as b on name_last = @b.name_last |> select :id"

  type Analysis = SoQLAnalysis[ColumnName, TestType]

  sealed trait SerializationMethod[T] {
    type typ = T
    val analyze: String => T
    val serialize: (OutputStream, T) => Unit
    val toFullAnalysis: T => NonEmptySeq[Analysis]

    def test(query: String): Unit = {
      val analysis = analyze(query)
      val baos = new ByteArrayOutputStream
      serialize(baos, analysis)
      test(baos.toByteArray, toFullAnalysis(analysis))
    }

    def test(bytesIn: Array[Byte], expected: NonEmptySeq[Analysis]): Unit = {
      deserializer(new ByteArrayInputStream(bytesIn)) must equal(expected)
    }
  }

  case object Full extends SerializationMethod[NonEmptySeq[Analysis]] {
    val analyze = analyzer.analyzeFullQuery _
    val serialize = serializer.apply(_, _)
    val toFullAnalysis: typ => NonEmptySeq[Analysis] = identity
  }

  case object Unchained extends SerializationMethod[SoQLAnalysis[ColumnName, TestType]] {
    val analyze = analyzer.analyzeUnchainedQuery _
    val serialize = serializer.unchainedSerializer(_, _)
    val toFullAnalysis: typ => NonEmptySeq[Analysis] = NonEmptySeq(_)
  }

  case class Test[T](testName: String, queryString: String, method: SerializationMethod[T], serializedV4B64: String)

  val tests = List(
    Test("simple unchained", "select :id", Unchained, "AAMKc2VsZWN0IDppZAEGbnVtYmVyAgM6aWQAAQAAAQICAQAAAAAAAQAAAQgBBwEAAAIAAAAAAAAAAAA="),
    Test("complex unchained", unchained, Unchained, "ABEHYmFsYW5jZQgGbnVtYmVyAwNzdW0FBW1vbmV5BwZ2aXNpdHMKD251bWJlciB0byBtb25leQ0CMTAPAT4JAzppZAIBPAwLc3VtX2JhbGFuY2UEAWkAATUOwQFzZWxlY3QgOmlkIGFzIGksIHN1bShiYWxhbmNlKQogICB3aGVyZSB2aXNpdHMgPiAwCiAgIGdyb3VwIGJ5IGksIHZpc2l0cwogICBoYXZpbmcgc3VtX2JhbGFuY2UgPCA1CiAgIG9yZGVyIGJ5IGkgZGVzYywKICAgICAgICAgICAgc3VtKGJhbGFuY2UpIG51bGwgZmlyc3QKICAgc2VhcmNoICdnbnUnCiAgIGxpbWl0IDUKICAgb2Zmc2V0IDEwAQFhBgNnbnUQATALAgAABAECBwcDAwMICAICCgoEDAEGBwIFAQYHAA0AAwkBBgMBAQACAAABCAEHAQACAwEAARIBEQYAARIBEQABAAEWARUBAAgHAAABAAIKAScGAAIRAS4BAgACCgEnAQAKAwACEwEwAwsDAQIAAQgBBwEAAgMAAxABQQEACgMBAAQLAVIGAAQXAV4CAgABEgERBgABEgERAAEAARYBFQEACAcABBkBYAYABBkBYAMBAAQZAWADDgMBAgABCAEHAQACAwAAAAYNAYIBBgAGDQGCAQABAAYRAYYBAQAIBwEAAQ4BDwEQ"),
    Test("join", join, Full, "BAkFbW9uZXkECW5hbWVfbGFzdAUHYmFsYW5jZQMGbnVtYmVyAgM6aWQAAWEIjAFzZWxlY3QgOmlkLCBiYWxhbmNlLCBAYi5uYW1lX2xhc3Qgam9pbiAoc2VsZWN0ICogZnJvbSBAYWFhYS1hYWFhIGFzIGEgfD4gc2VsZWN0IEBhLm5hbWVfbGFzdCkgYXMgYiBvbiBuYW1lX2xhc3QgPSBAYi5uYW1lX2xhc3QgfD4gc2VsZWN0IDppZAEEdGV4dAYBPQcDAwEAAAUCAwQEBgYCAgMDAwAABQUBBwEIBgACAAADAAABCAEHAQAAAgEAAQ0BDAEAAwQCAAEWARUBAQJfYgUGAAEESk9JTgIAAAECAAEwAS8BAAUGAQpfYWFhYS1hYWFhAAAAAAAAAAAAAAECAAFRAVABAQJfYQUGAAAAAAAAAAAAAQJfYgABZwFmBgABcQFwAAIAAWcBZgEABQYAAXMBcgEBAl9iBQYAAAAAAAAAAAABAAABigEBiQEBAAACAAAAAAAAAAAA"),
    Test("complex full", allQueryOptions, Full, "BBIHYmFsYW5jZQQGbnVtYmVyAgNzdW0JA2FtdAMFbW9uZXkFBnZpc2l0cwYHc3VtX2FtdAgCMTAQAT4LAzppZAABPA0BaQfuAXNlbGVjdCA6aWQsIGJhbGFuY2UgYXMgYW10LCB2aXNpdHMgfD4KICAgc2VsZWN0IDppZCBhcyBpLCBzdW0oYW10KQogICAgIHdoZXJlIHZpc2l0cyA+IDAKICAgICBncm91cCBieSBpLCB2aXNpdHMKICAgICBoYXZpbmcgc3VtX2FtdCA8IDUKICAgICBvcmRlciBieSBpIGRlc2MsCiAgICAgICAgICAgICAgc3VtKGFtdCkgbnVsbCBmaXJzdAogICAgIHNlYXJjaCAnZ251JwogICAgIGxpbWl0IDUKICAgICBvZmZzZXQgMTABD251bWJlciB0byBtb25leQ4BNQ8BYQoDZ251EQEwDAUIBAcDAAADAQYCAgUFAgIEBAQAAAMDBgYEDQEKBQIJAQoFAA4AAwsBCgIBAgAAAwAAAQgBBwEAAAIBAAENAQwBAAQFAgABHQEcAQAGAgAAAAAAAAAAAAEAAgMAAgsBMAEAAAIEAAIVAToGAAIVAToAAQACGQE+AQADBQAAAQADDAFOBgADEwFVAQIAAwwBTgEABgIAAxUBVwMMAgECAAILATABAAACAAQSAWoBAAYCAQAFDQF9BgAFFQGFAQICAAIVAToGAAIVAToAAQACGQE+AQADBQAFFwGHAQYABRcBhwEDAQAFFwGHAQMPAgECAAILATABAAACAAAABw8BrQEGAAcPAa0BAAEABxMBsQEBAAMFAQABDwEQARE="),
    Test("simple full", "select :id", Full, "BAMKc2VsZWN0IDppZAEGbnVtYmVyAgM6aWQAAQAAAQICAQAAAAEAAAEAAAEIAQcBAAACAAAAAAAAAAAA")
  )

  val b64Decoder: Base64.Decoder = Base64.getDecoder

  tests.foreach { case Test(name, query, method, v4b64) =>
    describe(name) {
      it(s"v5 serialize / v5 deserialize") {
        method.test(query)
      }

      it("v4 serialized / v5 deserialize") {
        val analysis = method.analyze(query)
        val decodedV4 = b64Decoder.decode(v4b64)
        method.test(decodedV4, method.toFullAnalysis(analysis))
      }
    }
  }
//
//  test("deserialize-of-serialize is the identity (all query options)") {
//    val analysis = analyzer.analyzeFullQuery(
//      """select :id, balance as amt, visits |>
//        |   select :id as i, sum(amt)
//        |     where visits > 0
//        |     group by i, visits
//        |     having sum_amt < 5
//        |     order by i desc,
//        |              sum(amt) null first
//        |     search 'gnu'
//        |     limit 5
//        |     offset 10""".stripMargin)
//    val baos = new ByteArrayOutputStream
//    serializer(baos, analysis)
//    deserializer(new ByteArrayInputStream(baos.toByteArray)) must equal (analysis)
//  }
//
//  test("deserialize-of-serialize is the identity (minimal query options)") {
//    val analysis = analyzer.analyzeFullQuery("select :id")
//    val baos = new ByteArrayOutputStream
//    serializer(baos, analysis)
//    deserializer(new ByteArrayInputStream(baos.toByteArray)) must equal (analysis)
//  }
//
//  test("deserialize-of-unchained-serialize is Seq(_) (all query options)") {
//    val analysis = analyzer.analyzeUnchainedQuery(
//      """select :id as i, sum(balance)
//        |   where visits > 0
//        |   group by i, visits
//        |   having sum_balance < 5
//        |   order by i desc,
//        |            sum(balance) null first
//        |   search 'gnu'
//        |   limit 5
//        |   offset 10""".stripMargin)
//    val baos = new ByteArrayOutputStream
//    serializer.unchainedSerializer(baos, analysis)
//    deserializer(new ByteArrayInputStream(baos.toByteArray)) must equal (NonEmptySeq(analysis))
//  }
//
//  test("deserialize-of-unchained-serialize is Seq(_) (minimal query options)") {
//    val analysis = analyzer.analyzeUnchainedQuery("select :id")
//    val baos = new ByteArrayOutputStream
//    serializer.unchainedSerializer(baos, analysis)
//    deserializer(new ByteArrayInputStream(baos.toByteArray)) must equal (NonEmptySeq(analysis))
//  }
//
//  test("join") {
//    val query = """select :id, balance, @b.name_last join (select * from @aaaa-aaaa as a |> select @a.name_last) as b on name_last = @b.name_last |> select :id"""
//    val analysis = analyzer.analyzeFullQuery(query)
//    val baos = new ByteArrayOutputStream
//    serializer(baos, analysis)
//    deserializer(new ByteArrayInputStream(baos.toByteArray)) must equal (analysis)
//  }
}
