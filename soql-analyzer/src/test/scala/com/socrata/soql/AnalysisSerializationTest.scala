package com.socrata.soql

import org.scalatest.FunSuite
import com.socrata.soql.environment.{ColumnName, DatasetContext, TableName, TypeName}
import com.socrata.soql.types._
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.scalatest.MustMatchers

// TODO: join serialization test
class AnalysisSerializationTest extends FunSuite with MustMatchers {
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

  implicit val datasetCtxMap = Map(TableName.PrimaryTable.name -> datasetCtx)

  val analyzer = new SoQLAnalyzer(TestTypeInfo, TestFunctionInfo)

  def serializeColumnName(columnName: ColumnName) = columnName.name
  def deserializeColumnName(s: String): ColumnName = ColumnName(s)

  def serializeTestType(t: TestType) = t.name.name
  def deserializeTestType(s: String): TestType = TestType.typesByName(TypeName(s))

  val serializer = new AnalysisSerializer(serializeColumnName, serializeTestType)
  val deserializer = new AnalysisDeserializer(deserializeColumnName, deserializeTestType, TestFunctions.functionsByIdentity)

  test("deserialize-of-serialize is the identity (all query options)") {
    val analysis = analyzer.analyzeFullQuery(
      """select :id, balance as amt, visits |>
        |   select :id as i, sum(amt)
        |     where visits > 0
        |     group by i, visits
        |     having sum_amt < 5
        |     order by i desc,
        |              sum(amt) null first
        |     search 'gnu'
        |     limit 5
        |     offset 10""".stripMargin)
    val baos = new ByteArrayOutputStream
    serializer(baos, analysis)
    deserializer(new ByteArrayInputStream(baos.toByteArray)) must equal (analysis)
  }

  test("deserialize-of-serialize is the identity (minimal query options)") {
    val analysis = analyzer.analyzeFullQuery("select :id")
    val baos = new ByteArrayOutputStream
    serializer(baos, analysis)
    deserializer(new ByteArrayInputStream(baos.toByteArray)) must equal (analysis)
  }

  test("deserialize-of-unchained-serialize is Seq(_) (all query options)") {
    val analysis = analyzer.analyzeUnchainedQuery(
      """select :id as i, sum(balance)
        |   where visits > 0
        |   group by i, visits
        |   having sum_balance < 5
        |   order by i desc,
        |            sum(balance) null first
        |   search 'gnu'
        |   limit 5
        |   offset 10""".stripMargin)
    val baos = new ByteArrayOutputStream
    serializer.unchainedSerializer(baos, analysis)
    deserializer(new ByteArrayInputStream(baos.toByteArray)) must equal (Seq(analysis))
  }

  test("deserialize-of-unchained-serialize is Seq(_) (minimal query options)") {
    val analysis = analyzer.analyzeUnchainedQuery("select :id")
    val baos = new ByteArrayOutputStream
    serializer.unchainedSerializer(baos, analysis)
    deserializer(new ByteArrayInputStream(baos.toByteArray)) must equal (Seq(analysis))
  }
}
