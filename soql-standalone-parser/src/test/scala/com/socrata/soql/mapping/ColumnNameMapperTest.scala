package com.socrata.soql.mapping

import com.socrata.soql.environment.ColumnName
import com.socrata.soql.parsing.StandaloneParser
import org.scalatest.{Assertions, FunSuite}
import org.scalatest.MustMatchers

/*
class ColumnNameMapperTest extends FunSuite with MustMatchers with Assertions {

  lazy val parser = new StandaloneParser()

  lazy val columnIdMap = Map(
    ColumnName("crime_date") -> ColumnName("MAP_crime_date"),
    ColumnName("ward") -> ColumnName("MAP_ward"),
    ColumnName("arrest") -> ColumnName("MAP_arrest"),
    ColumnName("crime_type") -> ColumnName("MAP_crime_type")
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
    def s = parser.selectStatement(
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

    def expS = parser.selectStatement(
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
}
*/
