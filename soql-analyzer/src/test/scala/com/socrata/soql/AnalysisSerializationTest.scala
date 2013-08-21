package com.socrata.soql

import org.scalatest.FunSuite
import com.socrata.soql.environment.{TypeName, ColumnName, DatasetContext}
import com.socrata.soql.types._
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import org.scalatest.matchers.MustMatchers

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

  val analyzer = new SoQLAnalyzer(TestTypeInfo, TestFunctionInfo)

  def serializeColumnName(columnName: ColumnName) = columnName.name
  def deserializeColumnName(s: String): ColumnName = ColumnName(s)

  def serializeTestType(t: TestType) = t.name.name
  def deserializeTestType(s: String): TestType = TestType.typesByName(TypeName(s))

  val serializer = new AnalysisSerializer(serializeColumnName, serializeTestType)
  val deserializer = new AnalysisDeserializer(deserializeColumnName, deserializeTestType, TestFunctions.functionsByIdentity)

  test("deserialize-of-serialize is the identity (all query options)") {
    val analysis = analyzer.analyzeFullQuery("select :id as i, sum(balance) where visits > 0 group by i, visits having sum_balance < 5 order by i desc, sum(balance) null first limit 5 offset 10 search 'gnu'")
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
}
