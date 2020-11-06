package com.socrata.soql.functions

import java.lang.reflect.Modifier

import com.socrata.soql.ast.SpecialFunctions
import com.socrata.soql.environment.FunctionName
import com.socrata.soql.types._

sealed abstract class SoQLFunctions



object SoQLFunctions {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[SoQLFunctions])

  import SoQLTypeClasses.{Ordered, Equatable, NumLike, RealNumLike, GeospatialLike, TimestampLike}
  private val AllTypes = SoQLType.typesByName.values.toSet

  val NoDocs = "No documentation available"

  // helpers to guide type inference (specifically forces SoQLType to be inferred)
  private def mf(
    identity: String,
    name: FunctionName,
    params: Seq[SoQLType],
    varargs: Seq[SoQLType],
    result: SoQLType,
    isAggregate: Boolean = false)(doc: String, examples: Example*) =
    new MonomorphicFunction(identity, name, params, varargs, result, isAggregate = isAggregate)(doc, examples:_*).function
  private def f(
    identity: String,
    name: FunctionName,
    constraints: Map[String, Set[SoQLType]],
    params: Seq[TypeLike[SoQLType]],
    varargs: Seq[TypeLike[SoQLType]],
    result: TypeLike[SoQLType],
    isAggregate: Boolean = false
  )(doc: String, examples: Example*) =
    Function(identity, name, constraints, params, varargs, result, isAggregate = isAggregate, doc, examples)
  private def field(source: SoQLType, field: String, result: SoQLType) =
    mf(
      source.name.name + "_" + field,
      SpecialFunctions.Field(source.name, field),
      Seq(source),
      Seq.empty,
      result
    )(s"Get the field named ${field} with type ${result}")

  val TextToFixedTimestamp = mf("text to fixed timestamp", SpecialFunctions.Cast(SoQLFixedTimestamp.name), Seq(SoQLText), Seq.empty, SoQLFixedTimestamp)(
    NoDocs
  )
  val TextToFloatingTimestamp = mf("text to floating timestamp", SpecialFunctions.Cast(SoQLFloatingTimestamp.name), Seq(SoQLText), Seq.empty, SoQLFloatingTimestamp)(
    NoDocs
  )
  val TextToDate = mf("text to date", SpecialFunctions.Cast(SoQLDate.name), Seq(SoQLText), Seq.empty, SoQLDate)(
    NoDocs
  )
  val TextToTime = mf("text to time", SpecialFunctions.Cast(SoQLTime.name), Seq(SoQLText), Seq.empty, SoQLTime)(
    NoDocs
  )
  val TextToPoint = mf("text to point", SpecialFunctions.Cast(SoQLPoint.name), Seq(SoQLText), Seq.empty, SoQLPoint)(
    NoDocs
  )
  val TextToMultiPoint = mf("text to multi point", SpecialFunctions.Cast(SoQLMultiPoint.name), Seq(SoQLText), Seq.empty, SoQLMultiPoint)(
    NoDocs
  )
  val TextToLine = mf("text to line", SpecialFunctions.Cast(SoQLLine.name), Seq(SoQLText), Seq.empty, SoQLLine)(
    NoDocs
  )
  val TextToMultiLine = mf("text to multi line", SpecialFunctions.Cast(SoQLMultiLine.name), Seq(SoQLText), Seq.empty, SoQLMultiLine)(
    NoDocs
  )
  val TextToPolygon = mf("text to polygon", SpecialFunctions.Cast(SoQLPolygon.name), Seq(SoQLText), Seq.empty, SoQLPolygon)(
    NoDocs
  )
  val TextToMultiPolygon = mf("text to multi polygon", SpecialFunctions.Cast(SoQLMultiPolygon.name), Seq(SoQLText), Seq.empty, SoQLMultiPolygon)(
    NoDocs
  )
  val TextToBlob = mf("text to blob", SpecialFunctions.Cast(SoQLBlob.name), Seq(SoQLText), Seq.empty, SoQLBlob)(
    NoDocs
  )
  val TextToPhoto = mf("text to photo", SpecialFunctions.Cast(SoQLPhoto.name), Seq(SoQLText), Seq.empty, SoQLPhoto)(
    NoDocs
  )
  val TextToLocation = mf("text to location", SpecialFunctions.Cast(SoQLLocation.name), Seq(SoQLText), Seq.empty, SoQLLocation)(
    NoDocs
  )
  val TextToPhone = mf("text to phone", SpecialFunctions.Cast(SoQLPhone.name), Seq(SoQLText), Seq.empty, SoQLPhone)(
    NoDocs
  )
  val TextToUrl = mf("text to url", SpecialFunctions.Cast(SoQLUrl.name), Seq(SoQLText), Seq.empty, SoQLUrl)(
    NoDocs
  )

  val Concat = f("||", SpecialFunctions.Operator("||"), Map.empty, Seq(VariableType("a"), VariableType("b")), Seq.empty, FixedType(SoQLText))(
    "Concatenate two strings"
  )
  val Gte = f(">=", SpecialFunctions.Operator(">="), Map("a" -> Ordered), Seq(VariableType("a"), VariableType("a")), Seq.empty, FixedType(SoQLBoolean))(
    "Return true if the value on the left is greater than or equal to the value on the right"
  )
  val Gt = f(">", SpecialFunctions.Operator(">"), Map("a" -> Ordered), Seq(VariableType("a"), VariableType("a")), Seq.empty, FixedType(SoQLBoolean))(
    "Return true if the value on the left is greater than the value on the right"
  )
  val Lt = f("<", SpecialFunctions.Operator("<"), Map("a" -> Ordered), Seq(VariableType("a"), VariableType("a")), Seq.empty, FixedType(SoQLBoolean))(
    "Return true if the value on the left is less than the value on the right"
  )
  val Lte = f("<=", SpecialFunctions.Operator("<="), Map("a" -> Ordered), Seq(VariableType("a"), VariableType("a")), Seq.empty, FixedType(SoQLBoolean))(
    "Return true if the value on the left is less than or equal to the value on the right"
  )
  val Eq = f("=", SpecialFunctions.Operator("="), Map("a" -> Equatable), Seq(VariableType("a"), VariableType("a")), Seq.empty, FixedType(SoQLBoolean))(
    "Return true if the left side equals the right"
  )
  val EqEq = Eq.copy(identity = "==", name = SpecialFunctions.Operator("=="))
  val Neq = f("<>", SpecialFunctions.Operator("<>"), Map("a" -> Equatable), Seq(VariableType("a"), VariableType("a")), Seq.empty, FixedType(SoQLBoolean))(
    "Return true if the left side does not equal the right"
  )
  val BangEq = Neq.copy(identity = "!=", name = SpecialFunctions.Operator("!="))

  // arguments: lat, lon, distance in meter
  val WithinCircle = f("within_circle", FunctionName("within_circle"), Map("a" -> GeospatialLike, "b" -> RealNumLike),
    Seq(VariableType("a"), VariableType("b"), VariableType("b"), VariableType("b")), Seq.empty, FixedType(SoQLBoolean))
                      ("Returns the rows that have locations within a specified circle, measured in meters")
  // arguments: nwLat, nwLon, seLat, seLon (yMax,  xMin , yMin,  xMax)
  val WithinBox = f("within_box", FunctionName("within_box"), Map("a" -> GeospatialLike, "b" -> RealNumLike),
    Seq(VariableType("a"), VariableType("b"), VariableType("b"), VariableType("b"), VariableType("b")), Seq.empty, FixedType(SoQLBoolean))
                   ("Returns the rows that have geodata within the specified box, defined by latitude, longitude corners")
  val WithinPolygon = f("within_polygon", FunctionName("within_polygon"), Map("a" -> GeospatialLike, "b" -> GeospatialLike),
    Seq(VariableType("a"), VariableType("b")), Seq.empty, FixedType(SoQLBoolean))
                       ("Returns the rows that have locations within the specified box, defined by latitude, longitude corners")
  val Extent = f("extent", FunctionName("extent"), Map("a" -> GeospatialLike),
    Seq(VariableType("a")), Seq.empty, FixedType(SoQLMultiPolygon), isAggregate = true)
                ("Returns a bounding box that encloses a set of geometries")
  val ConcaveHull = f("concave_hull", FunctionName("concave_hull"), Map("a" -> GeospatialLike, "b" -> RealNumLike),
    Seq(VariableType("a"), VariableType("b")), Seq.empty, FixedType(SoQLMultiPolygon), isAggregate = true)(NoDocs)
  val ConvexHull = Function(
    "convex_hull",
    FunctionName("convex_hull"),
    Map("a" -> GeospatialLike),
    Seq(VariableType("a")),
    Seq.empty,
    FixedType(SoQLMultiPolygon),
    isAggregate = true,
    """
    Returns the minimum convex geometry that encloses all of the geometries within a set

    The convex_hull(...) generates a polygon that represents the minimum convex geometry that
    can encompass a set of points or geometries. All of the points in the set will either
    represent vertexes of that polygon, or will be enclosed within it, much like if you were
    to take a rubber band and snap it around the set of points.
    """,
    Seq()
  )
  val Intersects = f("intersects", FunctionName("intersects"), Map("a" -> GeospatialLike, "b" -> GeospatialLike),
    Seq(VariableType("a"), VariableType("b")), Seq.empty, FixedType(SoQLBoolean))
                    ("Allows you to compare two geospatial types to see if they intersect or overlap each other")
  val DistanceInMeters = f("distance_in_meters", FunctionName("distance_in_meters"), Map("a" -> GeospatialLike, "b" -> GeospatialLike),
    Seq(VariableType("a"), VariableType("b")), Seq.empty, FixedType(SoQLNumber))(NoDocs)
  val GeoMakeValid = f("geo_make_valid", FunctionName("geo_make_valid"), Map("a" -> GeospatialLike),
    Seq(VariableType("a")), Seq.empty, VariableType("a"))(NoDocs)

  // multi to multi conversion
  val GeoMultiPolygonFromMultiPolygon = mf("geo_multi_mpg_mpg", FunctionName("geo_multi"), Seq(SoQLMultiPolygon), Seq.empty, SoQLMultiPolygon)(
    NoDocs
  )
  val GeoMultiLineFromMultiLine = mf("geo_multi_mln_mln", FunctionName("geo_multi"), Seq(SoQLMultiLine), Seq.empty, SoQLMultiLine)(
    NoDocs
  )
  val GeoMultiPointFromMultiPoint = mf("geo_multi_mpt_mpt", FunctionName("geo_multi"), Seq(SoQLMultiPoint), Seq.empty, SoQLMultiPoint)(
    NoDocs
  )

  // geo to multi geo conversion
  val GeoMultiPolygonFromPolygon = mf("geo_multi_mpg_pg", FunctionName("geo_multi"), Seq(SoQLPolygon), Seq.empty, SoQLMultiPolygon)(
    NoDocs
  )
  val GeoMultiLineFromLine = mf("geo_multi_mln_ln", FunctionName("geo_multi"), Seq(SoQLLine), Seq.empty, SoQLMultiLine)(
    NoDocs
  )
  val GeoMultiPointFromPoint = mf("geo_multi_mpt_pt", FunctionName("geo_multi"), Seq(SoQLPoint), Seq.empty, SoQLMultiPoint)(
    NoDocs
  )

  // collection to multi geo conversion
  val GeoCollectionExtractMultiPolygonFromPolygon = mf("geo_collection_extract_mpg_mpg", FunctionName("geo_collection_extract"), Seq(SoQLMultiPolygon), Seq.empty, SoQLMultiPolygon)(
    NoDocs
  )
  val GeoCollectionExtractMultiLineFromLine = mf("geo_multi_collection_mln_mln", FunctionName("geo_collection_extract"), Seq(SoQLMultiLine), Seq.empty, SoQLMultiLine)(
    NoDocs
  )
  val GeoCollectionExtractMultiPointFromPoint = mf("geo_multi_collection_mpt_mpt", FunctionName("geo_collection_extract"), Seq(SoQLMultiPoint), Seq.empty, SoQLMultiPoint)(
    NoDocs
  )

  val NumberOfPoints = f("num_points", FunctionName("num_points"), Map("a" -> GeospatialLike),
    Seq(VariableType("a")), Seq.empty, FixedType(SoQLNumber))
                        ("Returns the number of vertices in a geospatial data record")
  val Simplify = f("simplify", FunctionName("simplify"), Map("a" -> GeospatialLike, "b" -> NumLike),
                          Seq(VariableType("a"), VariableType("b")), Seq.empty, VariableType("a"))
                  ("Reduces the number of vertices in a line or polygon")
  val SimplifyPreserveTopology =
    f("simplify_preserve_topology",
             FunctionName("simplify_preserve_topology"),
             Map("a" -> GeospatialLike, "b" -> NumLike),
             Seq(VariableType("a"), VariableType("b")), Seq.empty, VariableType("a"))
     ("Reduces the number of vertices in a line or polygon, preserving topology")
  val SnapToGrid = f("snap_to_grid",
                     FunctionName("snap_to_grid"),
                     Map("a" -> GeospatialLike, "b" -> NumLike),
                     Seq(VariableType("a"), VariableType("b")), Seq.empty, VariableType("a"))(NoDocs)
  val SnapForZoom = f("snap_for_zoom",
                      FunctionName("snap_for_zoom"),
                      Map("a" -> GeospatialLike, "b" -> NumLike),
                      Seq(VariableType("a"), VariableType("b")), Seq.empty, VariableType("a"))(NoDocs)
  val CuratedRegionTest = f("curated_region_test", FunctionName("curated_region_test"), Map("a" -> GeospatialLike, "b" -> NumLike),
    Seq(VariableType("a"), VariableType("b")), Seq.empty, FixedType(SoQLText))(NoDocs)
  val VisibleAt = f("visible_at",
                           FunctionName("visible_at"),
                           Map("a" -> GeospatialLike, "b" -> RealNumLike),
                           Seq(VariableType("a"), VariableType("b")), Seq.empty, FixedType(SoQLBoolean))(NoDocs)
  val IsEmpty = f("is_empty",
                         FunctionName("is_empty"),
                         Map("a" -> GeospatialLike),
                         Seq(VariableType("a")), Seq.empty, FixedType(SoQLBoolean))(NoDocs)


  val IsNull = f("is null", SpecialFunctions.IsNull, Map.empty, Seq(VariableType("a")), Seq.empty, FixedType(SoQLBoolean))(
    "Return TRUE for values that are NULL"
  )
  val IsNotNull = f("is not null", SpecialFunctions.IsNotNull, Map.empty, Seq(VariableType("a")), Seq.empty, FixedType(SoQLBoolean))(
    "Return TRUE for values that are not NULL"
  )

  val Between = f("between", SpecialFunctions.Between, Map("a" -> Ordered), Seq(VariableType("a"), VariableType("a"), VariableType("a")), Seq.empty, FixedType(SoQLBoolean))(
    "Returns TRUE for values in a given range"
    // EN-41182
    // This is an example from dev.socrata.com
    // But it points at the wrong place. Solving the jira ticket
    // above will allow us to surface query examples that don't
    // have so many problems.
    // Once that is dealt with, we can write examples like below,
    // and they'll be rendered in the SoQLDocs component
    // Example(
    //   "Get the rows where annual salary is between 40,000 and 60,000",
    //   "select * where annual_salary between '40000' and '60000'",
    //   "https://data.cityofchicago.org/resource/tt4n-kn4t.json"
    // )
  )
  val NotBetween = f("not between", SpecialFunctions.NotBetween, Map("a" -> Ordered), Seq(VariableType("a"), VariableType("a"), VariableType("a")), Seq.empty, FixedType(SoQLBoolean))(
    "Returns TRUE for values not in a given range"
  )

  val Least = f("least", FunctionName("least"), Map("a" -> Ordered), Seq(VariableType("a")), Seq(VariableType("a")), VariableType("a"))(
    "Returns the smallest of its arguments, ignoring nulls"
  )
  val Greatest = f("greatest", FunctionName("greatest"), Map("a" -> Ordered), Seq(VariableType("a")), Seq(VariableType("a")), VariableType("a"))(
    "Returns the largest of its arguments, ignoring nulls"
  )

  val Min = f("min", FunctionName("min"), Map("a" -> Ordered), Seq(VariableType("a")), Seq.empty, VariableType("a"), isAggregate = true)(
    "Returns the minimum of a given set of values"
  )
  val Max = f("max", FunctionName("max"), Map("a" -> Ordered), Seq(VariableType("a")), Seq.empty, VariableType("a"), isAggregate = true)(
    "Returns the maximum of a given set of values"
  )
  val CountStar = mf("count(*)", SpecialFunctions.StarFunc("count"), Seq(), Seq.empty, SoQLNumber, isAggregate = true)(
    "Returns a count of a given set of records"
  )
  val Count = f("count", FunctionName("count"), Map.empty, Seq(VariableType("a")), Seq.empty, FixedType(SoQLNumber), isAggregate = true)(
    "Returns a count of a given set of records"
  )
  val CountDistinct = f("count_distinct", FunctionName("count_distinct"), Map.empty, Seq(VariableType("a")), Seq.empty, FixedType(SoQLNumber), isAggregate = true)(
    "Returns a distinct count of a given set of records"
  )
  val Sum = f("sum", FunctionName("sum"), Map("a" -> NumLike), Seq(VariableType("a")), Seq.empty, VariableType("a"), isAggregate = true)(
    "Returns the sum of a given set of numbers"
  )
  val Avg = f("avg", FunctionName("avg"), Map("a" -> NumLike), Seq(VariableType("a")), Seq.empty, VariableType("a"), isAggregate = true)(
    "Returns the average of a given set of numbers"
  )
  val Median = f("median", FunctionName("median"), Map("a" -> NumLike), Seq(VariableType("a")), Seq.empty, VariableType("a"), isAggregate = true)(
    "Returns a median of a given set of numbers"
  )
  val MedianDisc = f("median_disc", FunctionName("median"), Map("a" -> (Ordered -- NumLike)), Seq(VariableType("a")), Seq.empty, VariableType("a"), isAggregate = true)(
    "Returns a discrete median of a given set of numbers"
  )
  val StddevPop = f("stddev_pop", FunctionName("stddev_pop"), Map("a" -> NumLike), Seq(VariableType("a")), Seq.empty, VariableType("a"), isAggregate = true)(
    "Returns the population standard deviation of a given set of numbers"
  )
  val StddevSamp = f("stddev_samp", FunctionName("stddev_samp"), Map("a" -> NumLike), Seq(VariableType("a")), Seq.empty, VariableType("a"), isAggregate = true)(
    "Returns a sampled standard deviation of a given set of numbers"
  )

  val WindowFunctionOver = f("wf_over",
                             SpecialFunctions.WindowFunctionOver,
                             Map("a" -> AllTypes),
                             Seq(VariableType("a")),
                             Seq(WildcardType()),
                             VariableType("a"))(NoDocs)

  val UnaryPlus = f("unary +", SpecialFunctions.Operator("+"), Map("a" -> NumLike), Seq(VariableType("a")), Seq.empty, VariableType("a"))(
    NoDocs
  )
  val UnaryMinus = f("unary -", SpecialFunctions.Operator("-"), Map("a" -> NumLike), Seq(VariableType("a")), Seq.empty, VariableType("a"))(
    NoDocs
  )

  val SignedMagnitude10 = f("signed_magnitude_10", FunctionName("signed_magnitude_10"), Map("a" -> NumLike), Seq(VariableType("a")), Seq.empty, VariableType("a"))(
    NoDocs
  )
  val SignedMagnitudeLinear = f("signed_magnitude_linear", FunctionName("signed_magnitude_linear"), Map("a" -> NumLike, "b" -> NumLike), Seq(VariableType("a"), VariableType("b")), Seq.empty, VariableType("a"))(
    NoDocs
  )

  val BinaryPlus = f("+", SpecialFunctions.Operator("+"), Map("a" -> NumLike), Seq(VariableType("a"), VariableType("a")), Seq.empty, VariableType("a"))(
    "Add two numbers together"
  )
  val BinaryMinus = f("-", SpecialFunctions.Operator("-"), Map("a" -> NumLike), Seq(VariableType("a"), VariableType("a")), Seq.empty, VariableType("a"))(
    "Subtract a number from another"
  )

  val TimesNumNum = mf("*NN", SpecialFunctions.Operator("*"), Seq(SoQLNumber, SoQLNumber), Seq.empty, SoQLNumber)(
    "Multiply two numbers together"
  )
  val TimesDoubleDouble = mf("*DD", SpecialFunctions.Operator("*"), Seq(SoQLDouble, SoQLDouble), Seq.empty, SoQLDouble)(
    "Multiply two numbers together"
  )
  val TimesNumMoney = mf("*NM", SpecialFunctions.Operator("*"), Seq(SoQLNumber, SoQLMoney), Seq.empty, SoQLMoney)(
    "Multiply two numbers together"
  )
  val TimesMoneyNum = mf("*MN", SpecialFunctions.Operator("*"), Seq(SoQLMoney, SoQLNumber), Seq.empty, SoQLMoney)(
    "Multiply two numbers together"
  )

  val DivNumNum = mf("/NN", SpecialFunctions.Operator("/"), Seq(SoQLNumber, SoQLNumber), Seq.empty, SoQLNumber)(
    "Divide a number by another"
  )
  val DivDoubleDouble = mf("/DD", SpecialFunctions.Operator("/"), Seq(SoQLDouble, SoQLDouble), Seq.empty, SoQLDouble)(
    "Divide a number by another"
  )
  val DivMoneyNum = mf("/MN", SpecialFunctions.Operator("/"), Seq(SoQLMoney, SoQLNumber), Seq.empty, SoQLMoney)(
    "Divide a number by another"
  )
  val DivMoneyMoney = mf("/MM", SpecialFunctions.Operator("/"), Seq(SoQLMoney, SoQLMoney), Seq.empty, SoQLNumber)(
    "Divide a number by another"
  )

  val ExpNumNum = mf("^NN", SpecialFunctions.Operator("^"), Seq(SoQLNumber, SoQLNumber), Seq.empty, SoQLNumber)(
    "Return the value of one number raised to the power of another number"
  )
  val ExpDoubleDouble = mf("^DD", SpecialFunctions.Operator("^"), Seq(SoQLDouble, SoQLDouble), Seq.empty, SoQLDouble)(
    "Return the value of one number raised to the power of another number"
  )

  val ModNumNum = mf("%NN", SpecialFunctions.Operator("%"), Seq(SoQLNumber, SoQLNumber), Seq.empty, SoQLNumber)(
    "Find the remainder(modulus) of one number divided by another"
  )
  val ModDoubleDouble = mf("%DD", SpecialFunctions.Operator("%"), Seq(SoQLDouble, SoQLDouble), Seq.empty, SoQLDouble)(
    "Find the remainder(modulus) of one number divided by another"
  )
  val ModMoneyNum = mf("%MN", SpecialFunctions.Operator("%"), Seq(SoQLMoney, SoQLNumber), Seq.empty, SoQLMoney)(
    "Find the remainder(modulus) of one number divided by another"
  )
  val ModMoneyMoney = mf("%MM", SpecialFunctions.Operator("%"), Seq(SoQLMoney, SoQLMoney), Seq.empty, SoQLNumber)(
    "Find the remainder(modulus) of one number divided by another"
  )

  val Absolute = f("abs", FunctionName("abs"), Map("a" -> NumLike), Seq(VariableType("a")), Seq.empty, VariableType("a"))(
    "Produce the absolute value of a number"
  )
  val Ceiling = f("ceil", FunctionName("ceil"), Map("a" -> NumLike), Seq(VariableType("a")), Seq.empty, VariableType("a"))(
    NoDocs
  )
  val Floor = f("floor", FunctionName("floor"), Map("a" -> NumLike), Seq(VariableType("a")), Seq.empty, VariableType("a"))(
    NoDocs
  )

  val NumberToMoney = mf("number to money", SpecialFunctions.Cast(SoQLMoney.name), Seq(SoQLNumber), Seq.empty, SoQLMoney)(
    NoDocs
  )
  val NumberToDouble = mf("number to double", SpecialFunctions.Cast(SoQLDouble.name), Seq(SoQLNumber), Seq.empty, SoQLDouble)(
    NoDocs
  )

  val And = mf("and", SpecialFunctions.Operator("and"), Seq(SoQLBoolean, SoQLBoolean), Seq.empty, SoQLBoolean)(
    "Logical and of two boolean values"
  )
  val Or = mf("or", SpecialFunctions.Operator("or"), Seq(SoQLBoolean, SoQLBoolean), Seq.empty, SoQLBoolean)(
    "Logical or of two boolean values"
  )
  val Not = mf("not", SpecialFunctions.Operator("not"), Seq(SoQLBoolean), Seq.empty, SoQLBoolean)(
    "Invert a boolean"
  )

  val In = f("in", SpecialFunctions.In, Map.empty, Seq(VariableType("a")), Seq(VariableType("a")), FixedType(SoQLBoolean))(
    "Matches values in a given set of options"
  )
  val NotIn = f("not in", SpecialFunctions.NotIn, Map.empty, Seq(VariableType("a")), Seq(VariableType("a")), FixedType(SoQLBoolean))(
    "Matches values not in a given set of options"
  )

  val Like = mf("like", SpecialFunctions.Like, Seq(SoQLText, SoQLText), Seq.empty, SoQLBoolean)(
    "Allows for substring searches in text strings"
  )
  val NotLike = mf("not like", SpecialFunctions.NotLike, Seq(SoQLText, SoQLText), Seq.empty, SoQLBoolean)(
    "Allows for matching text fields that do not contain a substring"
  )

  val Contains = mf("contains", FunctionName("contains"), Seq(SoQLText, SoQLText), Seq.empty, SoQLBoolean)(
    "Matches on text strings that contain a given substring"
  )
  val StartsWith = mf("starts_with", FunctionName("starts_with"), Seq(SoQLText, SoQLText), Seq.empty, SoQLBoolean)(
    "Matches on text strings that start with a given substring"
  )
  val Lower = mf("lower", FunctionName("lower"), Seq(SoQLText), Seq.empty, SoQLText)(
    "Returns the lowercase equivalent of a string of text"
  )
  val Upper = mf("upper", FunctionName("upper"), Seq(SoQLText), Seq.empty, SoQLText)(
    "Returns the uppercase equivalent of a string of text"
  )

  val RowNumber = mf("row_number", FunctionName("row_number"), Seq(), Seq.empty, SoQLNumber)(
    NoDocs
  )
  val Rank = mf("rank", FunctionName("rank"), Seq(), Seq.empty, SoQLNumber)(
    NoDocs
  )
  val DenseRank = mf("dense_rank", FunctionName("dense_rank"), Seq(), Seq.empty, SoQLNumber)(
    NoDocs
  )
  val FirstValue = f("first value", FunctionName("first_value"), Map.empty, Seq(VariableType("a")), Seq.empty, VariableType("a"))(
    NoDocs
  )
  val LastValue = f("last value", FunctionName("last_value"), Map.empty, Seq(VariableType("a")), Seq.empty, VariableType("a"))(
    NoDocs
  )

  val FloatingTimeStampTruncYmd = mf("floating timestamp trunc day", FunctionName("date_trunc_ymd"), Seq(SoQLFloatingTimestamp), Seq.empty, SoQLFloatingTimestamp)(
    "Truncates a date at the year/month/day threshold"
  )
  val FloatingTimeStampTruncYm = mf("floating timestamp trunc month", FunctionName("date_trunc_ym"), Seq(SoQLFloatingTimestamp), Seq.empty, SoQLFloatingTimestamp)(
    "Truncates a date at the year/month threshold"
  )
  val FloatingTimeStampTruncY = mf("floating timestamp trunc year", FunctionName("date_trunc_y"), Seq(SoQLFloatingTimestamp), Seq.empty, SoQLFloatingTimestamp)(
    "Truncates a date at the year threshold"
  )

  val FloatingTimeStampExtractY = mf("floating timestamp extract year", FunctionName("date_extract_y"), Seq(SoQLFloatingTimestamp), Seq.empty, SoQLNumber)(
    "Extract the year as an integer"
  )
  val FloatingTimeStampExtractM = mf("floating timestamp extract month", FunctionName("date_extract_m"), Seq(SoQLFloatingTimestamp), Seq.empty, SoQLNumber)(
    "Extract the month as an integer"
  )
  val FloatingTimeStampExtractD = mf("floating timestamp extract day", FunctionName("date_extract_d"), Seq(SoQLFloatingTimestamp), Seq.empty, SoQLNumber)(
    "Extract the day from the date as an integer"
  )
  val FloatingTimeStampExtractHh = mf("floating timestamp extract hour", FunctionName("date_extract_hh"), Seq(SoQLFloatingTimestamp), Seq.empty, SoQLNumber)(
    "Extract the hour the date as an integer"
  )
  val FloatingTimeStampExtractMm = mf("floating timestamp extract minute", FunctionName("date_extract_mm"), Seq(SoQLFloatingTimestamp), Seq.empty, SoQLNumber)(
    "Extract the minute from the date as an integer"
  )
  val FloatingTimeStampExtractSs = mf("floating timestamp extract second", FunctionName("date_extract_ss"), Seq(SoQLFloatingTimestamp), Seq.empty, SoQLNumber)(
    "Extract the second from the date as an integer"
  )
  val FloatingTimeStampExtractDow = mf("floating timestamp extract day of week", FunctionName("date_extract_dow"), Seq(SoQLFloatingTimestamp), Seq.empty, SoQLNumber)(
    "Extracts the day of the week as an integer between 0 and 6"
  )
  // Extracting the week from a floating timestamp extracts the iso week (1-53).
  // Sometimes the last few days of December may be considered the first week of
  // next year. See https://en.wikipedia.org/wiki/ISO_week_date for more info.
  val FloatingTimeStampExtractWoy = mf("floating timestamp extract week of year", FunctionName("date_extract_woy"), Seq(SoQLFloatingTimestamp), Seq.empty, SoQLNumber)(
    "Extracts the week of the year as an integer between 0 and 51"
  )
  // This is useful when you are also extracting the week (iso week). This is
  // because the iso year will give the year associated with the iso week whereas
  // the year will give the year associated with the iso date.
  val FloatingTimestampExtractIsoY = mf("floating timestamp extract isoyear", FunctionName("date_extract_iso_y"), Seq(SoQLFloatingTimestamp), Seq.empty, SoQLNumber)(
    NoDocs
  )

  // This set of date_trunc functions for fixed_timestamp are for obe compatibility purpose.
  // The truncated boundary does not aligned with the client time zone unless it happens to have the same time zone as the server.
  // The FixedTimeStampTrunc*AtTimeZone set give the client more control to align at particular time zone.
  val FixedTimeStampZTruncYmd = mf("fixed timestamp z trunc day", FunctionName("datez_trunc_ymd"), Seq(SoQLFixedTimestamp), Seq.empty, SoQLFixedTimestamp)(
    "Truncates a date at the year/month/day threshold"
  )
  val FixedTimeStampZTruncYm = mf("fixed timestamp z trunc month", FunctionName("datez_trunc_ym"), Seq(SoQLFixedTimestamp), Seq.empty, SoQLFixedTimestamp)(
    "Truncates a date at the year/month threshold"
  )
  val FixedTimeStampZTruncY = mf("fixed timestamp z trunc year", FunctionName("datez_trunc_y"), Seq(SoQLFixedTimestamp), Seq.empty, SoQLFixedTimestamp)(
    "Truncates a date at the year threshold"
  )

  val FixedTimeStampTruncYmdAtTimeZone = mf("fixed timestamp trunc day at time zone", FunctionName("date_trunc_ymd"), Seq(SoQLFixedTimestamp, SoQLText), Seq.empty, SoQLFloatingTimestamp)(
    NoDocs
  )
  val FixedTimeStampTruncYmAtTimeZone = mf("fixed timestamp trunc month at time zone", FunctionName("date_trunc_ym"), Seq(SoQLFixedTimestamp, SoQLText), Seq.empty, SoQLFloatingTimestamp)(
    NoDocs
  )
  val FixedTimeStampTruncYAtTimeZone = mf("fixed timestamp trunc year at time zone", FunctionName("date_trunc_y"), Seq(SoQLFixedTimestamp, SoQLText), Seq.empty, SoQLFloatingTimestamp)(
    NoDocs
  )

  val TimeStampDiffD = f("timestamp diff in days", FunctionName("date_diff_d"), Map("a" -> TimestampLike), Seq(VariableType("a"), VariableType("a")), Seq.empty, FixedType(SoQLNumber))(
    NoDocs
  )

  // Translate a fixed timestamp to a given time zone and convert it to a floating timestamp.
  val ToFloatingTimestamp = mf("to floating timestamp", FunctionName("to_floating_timestamp"), Seq(SoQLFixedTimestamp, SoQLText), Seq.empty, SoQLFloatingTimestamp)(
    NoDocs
  )

  val castIdentities = for ((n, t) <- SoQLType.typesByName.toSeq) yield {
    f(n.caseFolded + "::" + n.caseFolded, SpecialFunctions.Cast(n), Map.empty, Seq(FixedType(t)), Seq.empty, FixedType(t))(
      NoDocs
    )
  }

  val NumberToText = mf("number to text", SpecialFunctions.Cast(SoQLText.name), Seq(SoQLNumber), Seq.empty, SoQLText)(
    NoDocs
  )
  val TextToNumber = mf("text to number", SpecialFunctions.Cast(SoQLNumber.name), Seq(SoQLText), Seq.empty, SoQLNumber)(
    NoDocs
  )
  val TextToMoney = mf("text to money", SpecialFunctions.Cast(SoQLMoney.name), Seq(SoQLText), Seq.empty, SoQLMoney)(
    NoDocs
  )

  val TextToBool = mf("text to boolean", SpecialFunctions.Cast(SoQLBoolean.name), Seq(SoQLText), Seq.empty, SoQLBoolean)(
    NoDocs
  )
  val BoolToText = mf("boolean to text", SpecialFunctions.Cast(SoQLText.name), Seq(SoQLBoolean), Seq.empty, SoQLText)(
    NoDocs
  )

  val Prop = mf(".", SpecialFunctions.Subscript, Seq(SoQLObject, SoQLText), Seq.empty, SoQLJson)(
    NoDocs
  )
  val Index = mf("[]", SpecialFunctions.Subscript, Seq(SoQLArray, SoQLNumber), Seq.empty, SoQLJson)(
    NoDocs
  )
  val JsonProp = mf(".J", SpecialFunctions.Subscript, Seq(SoQLJson, SoQLText), Seq.empty, SoQLJson)(
    NoDocs
  )
  val JsonIndex = mf("[]J", SpecialFunctions.Subscript, Seq(SoQLJson, SoQLNumber), Seq.empty, SoQLJson)(
    NoDocs
  )

  // Cannot use property subscript syntax (loc.prop) to access properties of different types - number, text, point. Use cast syntax (loc::prop) instead
  val LocationToPoint = mf("loc to point", SpecialFunctions.Cast(SoQLPoint.name), Seq(SoQLLocation), Seq.empty, SoQLPoint)(
    NoDocs
  )
  val LocationToLatitude = field(SoQLLocation, "latitude", SoQLNumber)
  val LocationToLongitude = field(SoQLLocation, "longitude", SoQLNumber)
  val LocationToAddress = field(SoQLLocation, "human_address", SoQLText)

  val LocationWithinCircle = f("location_within_circle", FunctionName("within_circle"),
    Map("a" -> RealNumLike),
    Seq(FixedType(SoQLLocation), VariableType("a"), VariableType("a"), VariableType("a")), Seq.empty,
    FixedType(SoQLBoolean))(
    NoDocs
  )
  val LocationWithinBox = f("location_within_box", FunctionName("within_box"),
    Map("a" -> RealNumLike),
    Seq(FixedType(SoQLLocation), VariableType("a"), VariableType("a"), VariableType("a"), VariableType("a")), Seq.empty,
    FixedType(SoQLBoolean))(
    NoDocs
  )
  val LocationWithinPolygon = f("location_within_polygon", FunctionName("within_polygon"),
    Map("a" -> GeospatialLike),
    Seq(FixedType(SoQLLocation), VariableType("a")), Seq.empty, FixedType(SoQLBoolean))(
    NoDocs
  )
  val LocationDistanceInMeters = f("location_distance_in_meters", FunctionName("distance_in_meters"),
    Map("a" -> GeospatialLike),
    Seq(FixedType(SoQLLocation), VariableType("a")), Seq.empty, FixedType(SoQLNumber))(
    NoDocs
  )

  val Location = f("location", FunctionName("location"), Map.empty,
    Seq(FixedType(SoQLPoint), FixedType(SoQLText), FixedType(SoQLText), FixedType(SoQLText), FixedType(SoQLText)), Seq.empty,
    FixedType(SoQLLocation))(
    NoDocs
  )

  /**
   * human_address should return a string address in json object form
   * like { "address": "101 Main St", "city": "Seattle", "state": "WA", "zip": "98104" }
   */
  val HumanAddress = mf("human_address", FunctionName("human_address"),
    Seq(SoQLText, SoQLText, SoQLText, SoQLText), Seq.empty, SoQLText)(
    NoDocs
  )

  val PointToLatitude = field(SoQLPoint, "latitude", SoQLNumber)
  val PointToLongitude = field(SoQLPoint, "longitude", SoQLNumber)

  val PhoneToPhoneNumber = field(SoQLPhone, "phone_number", SoQLText)
  val PhoneToPhoneType = field(SoQLPhone, "phone_type", SoQLText)

  val Phone = f("phone", FunctionName("phone"), Map.empty,
    Seq(FixedType(SoQLText), FixedType(SoQLText)), Seq.empty,
    FixedType(SoQLPhone))(
    NoDocs
  )

  val UrlToUrl = field(SoQLUrl, "url", SoQLText)
  val UrlToDescription = field(SoQLUrl, "description", SoQLText)

  val Url = f("url", FunctionName("url"), Map.empty,
    Seq(FixedType(SoQLText), FixedType(SoQLText)), Seq.empty,
    FixedType(SoQLUrl))(
    NoDocs
  )

  val DocumentToFilename = field(SoQLDocument, "filename", SoQLText)
  val DocumentToFileId = field(SoQLDocument, "file_id", SoQLText)
  val DocumentToContentType = field(SoQLDocument, "content_type", SoQLText)

  val JsonToText = mf("json to text", SpecialFunctions.Cast(SoQLText.name), Seq(SoQLJson), Seq.empty, SoQLText)(
    NoDocs
  )
  val JsonToNumber = mf("json to number", SpecialFunctions.Cast(SoQLNumber.name), Seq(SoQLJson), Seq.empty, SoQLNumber)(
    NoDocs
  )
  val JsonToBool = mf("json to bool", SpecialFunctions.Cast(SoQLBoolean.name), Seq(SoQLJson), Seq.empty, SoQLBoolean)(
    NoDocs
  )
  val JsonToObject = mf("json to obj", SpecialFunctions.Cast(SoQLObject.name), Seq(SoQLJson), Seq.empty, SoQLObject)(
    NoDocs
  )
  val JsonToArray = mf("json to array", SpecialFunctions.Cast(SoQLArray.name), Seq(SoQLJson), Seq.empty, SoQLArray)(
    NoDocs
  )

  val TextToRowIdentifier = mf("text to rid", SpecialFunctions.Cast(SoQLID.name), Seq(SoQLText), Seq.empty, SoQLID)(
    NoDocs
  )
  val TextToRowVersion = mf("text to rowver", SpecialFunctions.Cast(SoQLVersion.name), Seq(SoQLText), Seq.empty, SoQLVersion)(
    NoDocs
  )

  val Case = f("case", FunctionName("case"),
    Map("a" -> AllTypes),
    Seq(FixedType(SoQLBoolean), VariableType("a")),
    Seq(FixedType(SoQLBoolean), VariableType("a")),
    VariableType("a"))(
    """
    Returns different values based on the evaluation of boolean comparisons

    The case(...) function is a special boolean function that can be used to return
    different values based on the result of boolean comparisons, similar to if/then/else
    statements in other languages.

    Instead of a fixed number of parameters, case(...) takes a sequence of boolean, value
    pairs. Each pair will be evaluated to see if the boolean condition is true, and if
    so, the specified value will be returned. If no boolean condition can be evaluated
    as true, the function will return null.
    """
  )

  val Coalesce = f("coalesce", FunctionName("coalesce"),
    Map.empty,
    Seq(VariableType("a")),
    Seq(VariableType("a")),
    VariableType("a"))(
    "Take the leftmost non-null value"
  )

  def potentialAccessors = for {
    method <- getClass.getMethods
    if Modifier.isPublic(method.getModifiers) && method.getParameterTypes.length == 0
  } yield method

  val allFunctions: Seq[Function[SoQLType]] = {
    val reflectedFunctions = for {
      method <- getClass.getMethods
      if Modifier.isPublic(method.getModifiers) && method.getParameterTypes.length == 0 && method.getReturnType == classOf[Function[_]]
    } yield method.invoke(this).asInstanceOf[Function[SoQLType]]
    castIdentities ++ reflectedFunctions
  }

  val functionsByIdentity = allFunctions.map { f => f.identity -> f }.toMap

  val nAdicFunctions = SoQLFunctions.allFunctions.filterNot(_.isVariadic)
  val variadicFunctions = SoQLFunctions.allFunctions.filter(_.isVariadic)

  val nAdicFunctionsByNameThenArity: Map[FunctionName, Map[Int, Set[Function[SoQLType]]]] =
    nAdicFunctions.groupBy(_.name).mapValues { fs =>
      fs.groupBy(_.minArity).mapValues(_.toSet).toMap
    }.toMap

  val variadicFunctionsByNameThenMinArity: Map[FunctionName, Map[Int, Set[Function[SoQLType]]]] =
    variadicFunctions.groupBy(_.name).mapValues { fs =>
      fs.groupBy(_.minArity).mapValues(_.toSet).toMap
    }.toMap
}
