package com.socrata.soql

import org.scalatest.{FunSpec, MustMatchers}
import com.socrata.soql.environment.{ColumnName, DatasetContext, TypeName, ResourceName, TableRef}
import com.socrata.soql.types._
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.Base64
import com.rojoma.json.v3.util.{JsonUtil, AutomaticJsonCodecBuilder, SimpleHierarchyCodecBuilder, TagToValue, WrapperJsonCodec}
import com.rojoma.json.v3.ast.{JValue, JNull}
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode}

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

  def serializeTestType(t: TestType) = t.name.name
  def deserializeTestType(s: String): TestType = TestType.typesByName(TypeName(s))

  val serializer = new AnalysisSerializer[ColumnName, TestType](serializeColumnName, serializeTestType)
  val deserializer = new AnalysisDeserializer(deserializeColumnName, deserializeTestType, TestFunctions.functionsByIdentity)

  type Analysis = SoQLAnalysis[ColumnName, TestType]

  val b64Decoder: Base64.Decoder = Base64.getDecoder

  def analyzeUnchained(query: String) = analyzer.analyzeUnchainedQuery(primary, query)
  def analyzeFull(query: String) = analyzer.analyzeFullQuery(primary, query)

  def testRoundtrip(query: String) = {
    val analysis = analyzeFull(query)
    val baos = new ByteArrayOutputStream
    serializer(baos, analysis)
    deserializer(new ByteArrayInputStream(baos.toByteArray)) must equal(analysis)
  }

  describe("simple unchained") {
    val query = "select :id"

    it("serialize / deserialize equals analysis (v5)") {
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

    it("serialize / deserialize equals analysis (v5)") {
      testRoundtrip(query)
    }
  }

  describe("simple full") {
    val query = "select :id"

    it("serialize / deserialize equals analysis (v5)") {
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
    it("serialize / deserialize equals analysis (v5)") {
      testRoundtrip(query)
    }
  }

  describe("simple join") {
    val query = "select @aaaa-aaaa.name_last join @aaaa-aaaa on name_last = @aaaa-aaaa.name_last"

    it("serialize / deserialize equals analysis (v5)") {
      testRoundtrip(query)
    }
  }

  describe("simple join 2") {
    val query = "select :id, @a.name_last join @aaaa-aaaa as a on name_last = @a.name_last"

    it("serialize / deserialize equals analysis (v5)") {
      testRoundtrip(query)
    }
  }

  describe("complex join") {
    val query = "select :id, balance, @b.name_last join (select * from @aaaa-aaaa as a |> select name_last) as b on name_last = @b.name_last |> select :id"

    it("serialize / deserialize equals analysis (v5)") {
      testRoundtrip(query)
    }
  }
}
