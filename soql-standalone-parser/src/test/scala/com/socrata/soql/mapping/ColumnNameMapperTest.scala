package com.socrata.soql.mapping

import com.socrata.soql.environment.ColumnName
import com.socrata.soql.parsing.StandaloneParser
import com.socrata.soql.ast.Select
import org.scalatest.{Assertions, FunSuite}
import org.scalatest.MustMatchers

class ColumnNameMapperTest extends FunSuite with MustMatchers with Assertions {

  lazy val parser = new StandaloneParser()

  lazy val columnIdMap = Map(
    ColumnName("crime_date") -> ColumnName("MAP_crime_date"),
    ColumnName("ward") -> ColumnName("MAP_ward"),
    ColumnName("arrest") -> ColumnName("MAP_arrest"),
    ColumnName("crime_type") -> ColumnName("MAP_crime_type"),

    ColumnName("name") -> ColumnName("MAP_name"),
    ColumnName("cat") -> ColumnName("MAP_cat"),
    ColumnName("dog") -> ColumnName("MAP_dog"),
    ColumnName("bird") -> ColumnName("MAP_bird"),
    ColumnName("fish") -> ColumnName("MAP_fish")
  )

  lazy val mapper = new ColumnNameMapper(columnIdMap)


  test("Missing column in selection fails") {
    val s =
      parser.selection("date_trunc_ym(purple_date) AS crime_date, ward")

    a [NoSuchElementException] must be thrownBy {
      mapper.mapSelection(s)
    }
  }

  test("Selection mapped") {
    val s =
      parser.selection("date_trunc_ym(crime_date) AS crime_date, ward, count(*), arrest :: text, 'purple'")
    val expS =
      parser.selection("date_trunc_ym(MAP_crime_date) AS crime_date, MAP_ward, count(*), MAP_arrest :: text, 'purple'")

    assert(mapper.mapSelection(s).toString === expS.toString)
  }

  test("SelectionExcept mapped") {
    val s = parser.selection("* (EXCEPT ward)")
    val expS = parser.selection("* (EXCEPT MAP_ward)")
    assert(mapper.mapSelection(s).toString === expS.toString)
  }

  test("OrderBy mapped") {
    val ob = parser.orderings("date_trunc_ym(crime_date) ASC, ward DESC NULL LAST")
    val expOb = parser.orderings("date_trunc_ym(MAP_crime_date) ASC, MAP_ward DESC NULL LAST")
    assert(ob.map(o => mapper.mapOrderBy(o)).toString === expOb.toString)
  }

  test("Select mapped") {
    def s = parser.binaryTreeSelect(
      """
        |SELECT
        |  date_trunc_ym(crime_date) AS crime_date,
        |  ward,
        |  count(*),
        |  23,
        |  "purple",
        |  NULL
        |WHERE
        | (crime_type = "HOMICIDE" OR crime_type = "CLOWNICIDE") AND arrest=true
        |GROUP BY date_trunc_ym(crime_date), ward, count(*)
        |HAVING ward > '01'
        |ORDER BY date_trunc_ym(crime_date), ward DESC
        |LIMIT 12
        |OFFSET 11
      """.stripMargin)

    def expS = parser.binaryTreeSelect(
      """
        |SELECT
        |  date_trunc_ym(MAP_crime_date) AS crime_date,
        |  MAP_ward,
        |  count(*),
        |  23,
        |  "purple",
        |  NULL
        |WHERE
        | (MAP_crime_type = "HOMICIDE" OR MAP_crime_type = "CLOWNICIDE") AND MAP_arrest=true
        |GROUP BY date_trunc_ym(MAP_crime_date), MAP_ward, count(*)
        |HAVING MAP_ward > '01'
        |ORDER BY date_trunc_ym(MAP_crime_date), MAP_ward DESC
        |LIMIT 12
        |OFFSET 11
      """.stripMargin
    )

    assert(mapper.mapSelect(s).toString === expS.toString)
  }

  test("Compound query mapped") {
    val soql =  """
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
      """

    val expected = "SELECT `MAP_name`, @dog.`MAP_name` AS `dogname`, @j2.`MAP_name` AS `j2catname`, @j4.`MAP_name` AS `j4name`, @dog.`MAP_dog`, @j2.`MAP_cat` AS `j2cat`, @j3.`MAP_cat` AS `j3cat`, @j4.`MAP_bird` AS `j4bird` JOIN @dog ON TRUE JOIN @cat AS @j2 ON TRUE JOIN @cat AS @j3 ON TRUE JOIN (SELECT @b1.`MAP_name`, @b1.`MAP_bird` FROM @bird AS @b1 UNION (SELECT `MAP_name`, `MAP_fish`, @c2.`MAP_cat` AS `cat2` FROM @fish JOIN @cat AS @c2 ON TRUE |> SELECT `name`, `cat2`) UNION ALL SELECT @cat.`MAP_name`, @cat.`MAP_cat` FROM @cat) AS @j4 ON TRUE"
    val s = parser.binaryTreeSelect(soql)
    val actual = mapper.mapSelect(s)
    // Note that the (second) chained query in join union is not mapped and retains "SELECT name, cat2" because this is not supported.
    // But mapSelect should not raise exception because of that.
    assert(Select.toString(actual) === expected)
  }
}
