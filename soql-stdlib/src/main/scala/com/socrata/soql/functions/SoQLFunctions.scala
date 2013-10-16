package com.socrata.soql.functions

import com.socrata.soql.types._
import com.socrata.soql.ast.SpecialFunctions
import com.socrata.soql.environment.FunctionName
import java.lang.reflect.Modifier

sealed abstract class SoQLFunctions

object SoQLFunctions {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[SoQLFunctions])

  private val Ordered = Set[Any](
    SoQLText,
    SoQLNumber,
    SoQLDouble,
    SoQLMoney,
    SoQLBoolean,
    SoQLFixedTimestamp,
    SoQLFloatingTimestamp,
    SoQLDate,
    SoQLTime,
    SoQLID,
    SoQLVersion
  )
  private val NumLike = Set[Any](SoQLNumber, SoQLDouble, SoQLMoney)
  private val RealNumLike = Set[Any](SoQLNumber, SoQLDouble)
  private val GeospatialLike = Set[Any](SoQLLocation)

  val TextToFixedTimestamp = new MonomorphicFunction("text to fixed timestamp", FunctionName("to_fixed_timestamp"), Seq(SoQLText), None, SoQLFixedTimestamp).function
  val TextToFloatingTimestamp = new MonomorphicFunction("text to floating timestamp", FunctionName("to_floating_timestamp"), Seq(SoQLText), None, SoQLFloatingTimestamp).function
  val TextToDate = new MonomorphicFunction("text to date", FunctionName("to_date"), Seq(SoQLText), None, SoQLDate).function
  val TextToTime = new MonomorphicFunction("text to time", FunctionName("to_time"), Seq(SoQLText), None, SoQLTime).function
  val Concat = Function("||", SpecialFunctions.Operator("||"), Map.empty, Seq(VariableType("a"), VariableType("b")), None, FixedType(SoQLText))
  val Gte = Function(">=", SpecialFunctions.Operator(">="), Map("a"->Ordered), Seq(VariableType("a"), VariableType("a")), None, FixedType(SoQLBoolean))
  val Gt = Function(">", SpecialFunctions.Operator(">"), Map("a"->Ordered), Seq(VariableType("a"), VariableType("a")), None, FixedType(SoQLBoolean))
  val Lt = Function("<", SpecialFunctions.Operator("<"), Map("a"->Ordered), Seq(VariableType("a"), VariableType("a")), None, FixedType(SoQLBoolean))
  val Lte = Function("<=", SpecialFunctions.Operator("<="), Map("a"->Ordered), Seq(VariableType("a"), VariableType("a")), None, FixedType(SoQLBoolean))
  val Eq = Function("=", SpecialFunctions.Operator("="), Map("a"->Ordered), Seq(VariableType("a"), VariableType("a")), None, FixedType(SoQLBoolean))
  val EqEq = Eq.copy(identity = "==", name = SpecialFunctions.Operator("=="))
  val Neq = Function("<>", SpecialFunctions.Operator("<>"), Map("a"->Ordered), Seq(VariableType("a"), VariableType("a")), None, FixedType(SoQLBoolean))
  val BangEq = Neq.copy(identity = "!=", name = SpecialFunctions.Operator("!="))

  // arguments: lat, lon, distance in meter
  val WithinCircle = Function("within_circle", FunctionName("within_circle"), Map ("a"-> GeospatialLike, "b" -> RealNumLike),
    Seq(VariableType("a"), VariableType("b"), VariableType("b"), VariableType("b")), None, FixedType(SoQLBoolean))
  // arguments: nwLat, nwLon, seLat, seLon (yMax,  xMin , yMin,  xMax)
  val WithinBox = Function("within_box", FunctionName("within_box"), Map ("a"-> GeospatialLike, "b" -> RealNumLike),
    Seq(VariableType("a"), VariableType("b"), VariableType("b"), VariableType("b"), VariableType("b")), None, FixedType(SoQLBoolean))

  val LatitudeField = new MonomorphicFunction("latitude field", SpecialFunctions.Subscript, Seq(SoQLLocation, SoQLTextLiteral("latitude")), None, SoQLDouble).function
  val LongitudeField = new MonomorphicFunction("longitude field", SpecialFunctions.Subscript, Seq(SoQLLocation, SoQLTextLiteral("longitude")), None, SoQLDouble).function

  val IsNull = Function("is null", SpecialFunctions.IsNull, Map.empty, Seq(VariableType("a")), None, FixedType(SoQLBoolean))
  val IsNotNull = Function("is not null", SpecialFunctions.IsNotNull, Map.empty, Seq(VariableType("a")), None, FixedType(SoQLBoolean))

  val Between = Function("between", SpecialFunctions.Between, Map("a"->Ordered), Seq(VariableType("a"),VariableType("a"),VariableType("a")), None, FixedType(SoQLBoolean))
  val NotBetween = Function("not between", SpecialFunctions.NotBetween, Map("a"->Ordered), Seq(VariableType("a"),VariableType("a"),VariableType("a")), None, FixedType(SoQLBoolean))

  val Min = Function("min", FunctionName("min"), Map("a"->Ordered), Seq(VariableType("a")), None, VariableType("a"), isAggregate = true)
  val Max = Function("max", FunctionName("max"), Map("a"->Ordered), Seq(VariableType("a")), None, VariableType("a"), isAggregate = true)
  val CountStar = new MonomorphicFunction("count(*)", SpecialFunctions.StarFunc("count"), Seq(), None, SoQLNumber, isAggregate = true).function
  val Count = Function("count", FunctionName("count"), Map.empty, Seq(VariableType("a")), None, FixedType(SoQLNumber), isAggregate = true)
  val Sum = Function("sum", FunctionName("sum"), Map("a"->NumLike), Seq(VariableType("a")), None, VariableType("a"), isAggregate = true)
  val Avg = Function("avg", FunctionName("avg"), Map("a"->NumLike), Seq(VariableType("a")), None, VariableType("a"), isAggregate = true)

  val UnaryPlus = Function("unary +", SpecialFunctions.Operator("+"), Map("a"->NumLike), Seq(VariableType("a")), None, VariableType("a"))
  val UnaryMinus = Function("unary -", SpecialFunctions.Operator("-"), Map("a"->NumLike), Seq(VariableType("a")), None, VariableType("a"))

  val BinaryPlus = Function("+", SpecialFunctions.Operator("+"), Map("a"->NumLike), Seq(VariableType("a"), VariableType("a")), None, VariableType("a"))
  val BinaryMinus = Function("-", SpecialFunctions.Operator("-"), Map("a"->NumLike), Seq(VariableType("a"), VariableType("a")), None, VariableType("a"))

  val TimesNumNum = new MonomorphicFunction("*NN", SpecialFunctions.Operator("*"), Seq(SoQLNumber, SoQLNumber), None, SoQLNumber).function
  val TimesDoubleDouble = new MonomorphicFunction("*DD", SpecialFunctions.Operator("*"), Seq(SoQLDouble, SoQLDouble), None, SoQLDouble).function
  val TimesNumMoney = new MonomorphicFunction("*NM", SpecialFunctions.Operator("*"), Seq(SoQLNumber, SoQLMoney), None, SoQLMoney).function
  val TimesMoneyNum = new MonomorphicFunction("*MN", SpecialFunctions.Operator("*"), Seq(SoQLMoney, SoQLNumber), None, SoQLMoney).function

  val DivNumNum = new MonomorphicFunction("/NN", SpecialFunctions.Operator("/"), Seq(SoQLNumber, SoQLNumber), None, SoQLNumber).function
  val DivDoubleDouble = new MonomorphicFunction("/DD", SpecialFunctions.Operator("/"), Seq(SoQLDouble, SoQLDouble), None, SoQLDouble).function
  val DivMoneyNum = new MonomorphicFunction("/MN", SpecialFunctions.Operator("/"), Seq(SoQLMoney, SoQLNumber), None, SoQLMoney).function
  val DivMoneyMoney = new MonomorphicFunction("/MM", SpecialFunctions.Operator("/"), Seq(SoQLMoney, SoQLMoney), None, SoQLNumber).function

  val NumberToMoney = new MonomorphicFunction("number to money", SpecialFunctions.Operator("to_money"), Seq(SoQLNumber), None, SoQLMoney).function
  val NumberToDouble = new MonomorphicFunction("number to double", SpecialFunctions.Operator("to_double"), Seq(SoQLNumber), None, SoQLDouble).function

  val And = new MonomorphicFunction("and", SpecialFunctions.Operator("and"), Seq(SoQLBoolean, SoQLBoolean), None, SoQLBoolean).function
  val Or = new MonomorphicFunction("or", SpecialFunctions.Operator("or"), Seq(SoQLBoolean, SoQLBoolean), None, SoQLBoolean).function
  val Not = new MonomorphicFunction("not", SpecialFunctions.Operator("not"), Seq(SoQLBoolean), None, SoQLBoolean).function

  val In = new Function("in", SpecialFunctions.In, Map.empty, Seq(VariableType("a")), Some(VariableType("a")), FixedType(SoQLBoolean))
  val NotIn = new Function("not in", SpecialFunctions.NotIn, Map.empty, Seq(VariableType("a")), Some(VariableType("a")), FixedType(SoQLBoolean))

  val Like = new MonomorphicFunction("like", SpecialFunctions.Like, Seq(SoQLText, SoQLText), None, SoQLBoolean).function
  val NotLike = new MonomorphicFunction("not like", SpecialFunctions.NotLike, Seq(SoQLText, SoQLText), None, SoQLBoolean).function

  val Contains = new MonomorphicFunction("contains",  FunctionName("contains"), Seq(SoQLText, SoQLText), None, SoQLBoolean).function
  val StartsWith = new MonomorphicFunction("starts_with",  FunctionName("starts_with"), Seq(SoQLText, SoQLText), None, SoQLBoolean).function

  val castIdentities = for((n, t) <- SoQLType.typesByName.toSeq) yield {
    Function(n.caseFolded+"::"+n.caseFolded, SpecialFunctions.Cast(n), Map.empty, Seq(FixedType(t)), None, FixedType(t))
  }

  val NumberToText = new MonomorphicFunction("number to text", SpecialFunctions.Cast(SoQLText.name), Seq(SoQLNumber), None, SoQLText).function
  val TextToNumber = new MonomorphicFunction("text to number", SpecialFunctions.Cast(SoQLNumber.name), Seq(SoQLText), None, SoQLNumber).function

  val Prop = new MonomorphicFunction(".", SpecialFunctions.Subscript, Seq(SoQLObject, SoQLText), None, SoQLJson).function
  val Index = new MonomorphicFunction("[]", SpecialFunctions.Subscript, Seq(SoQLArray, SoQLNumber), None, SoQLJson).function
  val JsonProp = new MonomorphicFunction(".J", SpecialFunctions.Subscript, Seq(SoQLJson, SoQLText), None, SoQLJson).function
  val JsonIndex = new MonomorphicFunction("[]J", SpecialFunctions.Subscript, Seq(SoQLJson, SoQLNumber), None, SoQLJson).function

  val JsonToText = new MonomorphicFunction("json to text", SpecialFunctions.Cast(SoQLText.name), Seq(SoQLJson), None, SoQLText).function
  val JsonToNumber = new MonomorphicFunction("json to number", SpecialFunctions.Cast(SoQLNumber.name), Seq(SoQLJson), None, SoQLNumber).function
  val JsonToBool = new MonomorphicFunction("json to bool", SpecialFunctions.Cast(SoQLBoolean.name), Seq(SoQLJson), None, SoQLBoolean).function
  val JsonToObject = new MonomorphicFunction("json to obj", SpecialFunctions.Cast(SoQLObject.name), Seq(SoQLJson), None, SoQLObject).function
  val JsonToArray = new MonomorphicFunction("json to array", SpecialFunctions.Cast(SoQLArray.name), Seq(SoQLJson), None, SoQLArray).function

  val TextToRowIdentifier = new MonomorphicFunction("text to rid", SpecialFunctions.Cast(SoQLID.name), Seq(SoQLText), None, SoQLID).function
  val TextToRowVersion = new MonomorphicFunction("text to rowver", SpecialFunctions.Cast(SoQLVersion.name), Seq(SoQLText), None, SoQLVersion).function

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

  private def analysisify(f: Function[SoQLType]): Function[SoQLAnalysisType] = f

  val nAdicFunctionsByNameThenArity: Map[FunctionName, Map[Int, Set[Function[SoQLAnalysisType]]]] =
    nAdicFunctions.groupBy(_.name).mapValues { fs =>
      fs.groupBy(_.minArity).mapValues(_.map(analysisify).toSet).toMap
    }.toMap

  val variadicFunctionsByNameThenMinArity: Map[FunctionName, Map[Int, Set[Function[SoQLAnalysisType]]]] =
    variadicFunctions.groupBy(_.name).mapValues { fs =>
      fs.groupBy(_.minArity).mapValues(_.map(analysisify).toSet).toMap
    }.toMap
}
