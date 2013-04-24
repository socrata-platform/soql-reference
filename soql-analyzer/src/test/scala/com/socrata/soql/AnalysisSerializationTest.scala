package com.socrata.soql

import org.scalatest.FunSuite
import com.socrata.soql.environment.{TypeName, ColumnName, DatasetContext}
import com.socrata.soql.types._
import com.google.protobuf.{CodedInputStream, CodedOutputStream}
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

  def serializeTestType(out: CodedOutputStream, t: TestType) {
    out.writeStringNoTag(t.name.name)
  }

  def deserializeTestType(in: CodedInputStream): TestType =
    TestType.typesByName(TypeName(in.readString()))

  val serializer = new AnalysisSerializer(serializeTestType)
  val deserializer = new AnalysisDeserializer(deserializeTestType, TestFunctions.functionsByIdentity)

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
