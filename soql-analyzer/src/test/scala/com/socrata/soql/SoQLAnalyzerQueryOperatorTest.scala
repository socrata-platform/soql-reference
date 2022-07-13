package com.socrata.soql

import com.socrata.soql.collection.OrderedMap
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.FunSuite
import org.scalatest.MustMatchers
import com.socrata.soql.environment.{ColumnName, DatasetContext, TableName}
import com.socrata.soql.mapping.ColumnIdMapper
import com.socrata.soql.parsing.{Parser, StandaloneParser}
import com.socrata.soql.typechecker.{Typechecker, ParameterSpec}
import com.socrata.soql.typed.Qualifier
import com.socrata.soql.types._

class SoQLAnalyzerQueryOperatorTest extends FunSuite with MustMatchers with ScalaCheckPropertyChecks {

  val commonColumns = OrderedMap(
    ColumnName(":id") -> TestNumber,
    ColumnName(":updated_at") -> TestFixedTimestamp,
    ColumnName(":created_at") -> TestFixedTimestamp,
    ColumnName("name") -> TestText,
    ColumnName("breed") -> TestText,
    ColumnName("age") -> TestNumber
  )

  val catCtx = new DatasetContext[TestType] {
    val schema = commonColumns + (ColumnName("cat") -> TestText)
  }

  val dogCtx = new DatasetContext[TestType] {
    val schema = commonColumns + (ColumnName("dog") -> TestText)
  }

  val birdCtx = new DatasetContext[TestType] {
    val schema = commonColumns + (ColumnName("bird") -> TestText)
  }

  val fishCtx = new DatasetContext[TestType] {
    val schema = commonColumns + (ColumnName("fish") -> TestText)
  }

  implicit val datasetCtxMap = AnalysisContext[TestType, SoQLValue](
    schemas = Map(TableName.PrimaryTable.qualifier -> catCtx,
                  TableName("_cat", None).qualifier -> catCtx,
                  TableName("_dog", None).qualifier -> dogCtx,
                  TableName("_bird", None).qualifier -> birdCtx,
                  TableName("_fish", None).qualifier -> fishCtx),
    parameters = ParameterSpec.empty
  )

  val analyzer = new SoQLAnalyzer(TestTypeInfo, TestFunctionInfo)

  def expression(s: String) = new Parser().expression(s)

  def typedExpression(s: String) = {
    val tc = new Typechecker(TestTypeInfo, TestFunctionInfo)
    tc(expression(s), Map.empty, None)
  }

  test("Analyze compound queries") {
    val list = "List"
    val parser = new StandaloneParser()
    val soqls = Seq( /* tuple3 of soql, analysis::string, mapped analysis::string */
      ("SELECT name, @dog.breed join @dog on true",
       "SELECT ListMap(name -> name :: text, breed -> _dog.breed :: text) JOIN _dog ON TRUE :: boolean",
       "SELECT ListMap(name -> str_cat_name :: text, breed -> _dog.str_dog_breed :: text) JOIN _dog ON TRUE :: boolean"),
      ("SELECT name, @d1.breed join @dog as d1 on true",
       "SELECT ListMap(name -> name :: text, breed -> _d1.breed :: text) JOIN _dog AS @d1 ON TRUE :: boolean",
       "SELECT ListMap(name -> str_cat_name :: text, breed -> _d1.str_dog_breed :: text) JOIN _dog AS @d1 ON TRUE :: boolean"),
      ("SELECT name, @j1.breed join (select name, breed from @dog) as j1 on true",
       "SELECT ListMap(name -> name :: text, breed -> _j1.breed :: text) JOIN (SELECT ListMap(name -> name :: text, breed -> breed :: text) FROM @dog) AS @j1 ON TRUE :: boolean",
       "SELECT ListMap(name -> str_cat_name :: text, breed -> _j1.breed :: text) JOIN (SELECT ListMap(name -> str_dog_name :: text, breed -> str_dog_breed :: text) FROM @dog) AS @j1 ON TRUE :: boolean"),
      ("SELECT name, @j1.breed join (select @dog.name, @dog.breed from @dog) as j1 on true",
       "SELECT ListMap(name -> name :: text, breed -> _j1.breed :: text) JOIN (SELECT ListMap(name -> _dog.name :: text, breed -> _dog.breed :: text) FROM @dog) AS @j1 ON TRUE :: boolean",
       "SELECT ListMap(name -> str_cat_name :: text, breed -> _j1.breed :: text) JOIN (SELECT ListMap(name -> _dog.str_dog_name :: text, breed -> _dog.str_dog_breed :: text) FROM @dog) AS @j1 ON TRUE :: boolean"),
      ("SELECT name, @j1.breed join (select @d1.name, @d1.breed from @dog as d1) as j1 on true",
       "SELECT ListMap(name -> name :: text, breed -> _j1.breed :: text) JOIN (SELECT ListMap(name -> _d1.name :: text, breed -> _d1.breed :: text) FROM @dog AS @d1) AS @j1 ON TRUE :: boolean",
       "SELECT ListMap(name -> str_cat_name :: text, breed -> _j1.breed :: text) JOIN (SELECT ListMap(name -> _d1.str_dog_name :: text, breed -> _d1.str_dog_breed :: text) FROM @dog AS @d1) AS @j1 ON TRUE :: boolean"),
      ("SELECT name UNION SELECT name from @dog",
       "SELECT ListMap(name -> name :: text) UNION SELECT ListMap(name -> name :: text) FROM @dog",
       "SELECT ListMap(name -> str_cat_name :: text) UNION SELECT ListMap(name -> str_dog_name :: text) FROM @dog"),
      ("""
       SELECT name, @dog.name as dogname, @j2.name as j2catname, @j4.name as j4name,
              @dog.dog, @j2.cat as j2cat, @j3.cat as j3cat, @j4.bird as j4bird
         JOIN @dog ON TRUE
         JOIN @cat as j2 ON TRUE
         JOIN @cat as j3 ON TRUE
         JOIN (SELECT @b1.name, @b1.bird FROM @bird as b1
                UNION
              (SELECT name, fish, @c2.cat as cat2 FROM @fish JOIN @cat as c2 ON TRUE |> SELECT name, cat2)
                UNION ALL
               SELECT @cat.name, @cat.cat FROM @cat) as j4 ON TRUE
      """,
       "SELECT ListMap(name -> name :: text, dogname -> _dog.name :: text, j2catname -> _j2.name :: text, j4name -> _j4.name :: text, dog -> _dog.dog :: text, j2cat -> _j2.cat :: text, j3cat -> _j3.cat :: text, j4bird -> _j4.bird :: text) JOIN _dog ON TRUE :: boolean JOIN _cat AS @j2 ON TRUE :: boolean JOIN _cat AS @j3 ON TRUE :: boolean JOIN (SELECT ListMap(name -> _b1.name :: text, bird -> _b1.bird :: text) FROM @bird AS @b1 UNION (SELECT ListMap(name -> name :: text, fish -> fish :: text, cat2 -> _c2.cat :: text) FROM @fish JOIN _cat AS @c2 ON TRUE :: boolean |> SELECT ListMap(name -> name :: text, cat2 -> cat2 :: text)) UNION ALL SELECT ListMap(name -> _cat.name :: text, cat -> _cat.cat :: text) FROM @cat) AS @j4 ON TRUE :: boolean",
       "SELECT ListMap(name -> str_cat_name :: text, dogname -> _dog.str_dog_name :: text, j2catname -> _j2.str_cat_name :: text, j4name -> _j4.name :: text, dog -> _dog.str_dog_dog :: text, j2cat -> _j2.str_cat_cat :: text, j3cat -> _j3.str_cat_cat :: text, j4bird -> _j4.bird :: text) JOIN _dog ON TRUE :: boolean JOIN _cat AS @j2 ON TRUE :: boolean JOIN _cat AS @j3 ON TRUE :: boolean JOIN (SELECT ListMap(name -> _b1.str_bird_name :: text, bird -> _b1.str_bird_bird :: text) FROM @bird AS @b1 UNION (SELECT ListMap(name -> str_fish_name :: text, fish -> str_fish_fish :: text, cat2 -> _c2.str_cat_cat :: text) FROM @fish JOIN _cat AS @c2 ON TRUE :: boolean |> SELECT ListMap(name -> name :: text, cat2 -> cat2 :: text)) UNION ALL SELECT ListMap(name -> _cat.str_cat_name :: text, cat -> _cat.str_cat_cat :: text) FROM @cat) AS @j4 ON TRUE :: boolean")
    )

    val qColumnIdNewColumnIdMap = Map(
      (ColumnName("name"), None) -> "str_cat_name",
      (ColumnName("cat"), None) -> "str_cat_cat",
      (ColumnName("breed"), None) -> "str_cat_breed",

      (ColumnName("name"), Some("_cat")) -> "str_cat_name",
      (ColumnName("cat"), Some("_cat")) -> "str_cat_cat",
      (ColumnName("breed"), Some("_cat")) -> "str_cat_breed",

      (ColumnName("name"), Some("_dog")) -> "str_dog_name",
      (ColumnName("dog"), Some("_dog")) -> "str_dog_dog",
      (ColumnName("breed"), Some("_dog")) -> "str_dog_breed",

      (ColumnName("name"), Some("_bird")) -> "str_bird_name",
      (ColumnName("bird"), Some("_bird")) -> "str_bird_bird",
      (ColumnName("breed"), Some("_bird")) -> "str_bird_breed",

      (ColumnName("name"), Some("_fish")) -> "str_fish_name",
      (ColumnName("fish"), Some("_fish")) -> "str_fish_fish",
      (ColumnName("breed"), Some("_fish")) -> "str_fish_breed"
    )
    val qColumnNameToQColumnId = (q: Qualifier, cn: ColumnName) => (cn, q)
    val columnNameToNewColumnId = (cn: ColumnName) => cn.name
    val columnIdToNewColumnId = (cn: ColumnName) => cn.name

    val serializerTest = new AnalysisSerializationTest()

    soqls.foreach {
      case (soql, expectedAnalysis, expectedMappedAnalysis) =>
      val ast = parser.binaryTreeSelect(soql)
      val analysis = analyzer.analyzeBinary(ast)(datasetCtxMap)
      analysis.toString must equal(expectedAnalysis)
      // test mapColumnIds
      val mappedAnalysis = ColumnIdMapper.mapColumnIds(analysis)(qColumnIdNewColumnIdMap, qColumnNameToQColumnId, columnNameToNewColumnId, columnIdToNewColumnId)
      mappedAnalysis.toString must equal(expectedMappedAnalysis)
      // test analysis serialization
      serializerTest.testFull(analysis)
    }
  }
}
