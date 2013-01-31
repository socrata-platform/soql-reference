package com.socrata.soql.types

import com.socrata.soql.ast.SpecialFunctions
import com.socrata.soql.environment.FunctionName
import java.lang.reflect.Modifier
import com.socrata.soql.functions.{FixedType, VariableType, MonomorphicFunction, Function}

sealed abstract class TestFunctions

object TestFunctions {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[TestFunctions])

  private val Ordered = TestTypeConversions.typeParameterUniverse.toSet[Any] // might want to narrow this down
  private val NumLike = Set[Any](TestNumber, TestDouble, TestMoney)
  private val RealNumLike = Set[Any](TestNumber, TestDouble)
  private val GeospatialLike = Set[Any](TestLocation)

  val TextToFixedTimestamp = new MonomorphicFunction(FunctionName("to_fixed_timestamp"), Seq(TestText), None, TestFixedTimestamp).function
  val TextToFloatingTimestamp = new MonomorphicFunction(FunctionName("to_floating_timestamp"), Seq(TestText), None, TestFloatingTimestamp).function
  val Concat = Function(SpecialFunctions.Operator("||"), Map.empty, Seq(VariableType("a"), VariableType("b")), None, FixedType(TestText))
  val Gte = Function(SpecialFunctions.Operator(">="), Map("a"->Ordered), Seq(VariableType("a"), VariableType("a")), None, FixedType(TestBoolean))
  val Gt = Function(SpecialFunctions.Operator(">"), Map("a"->Ordered), Seq(VariableType("a"), VariableType("a")), None, FixedType(TestBoolean))
  val Lt = Function(SpecialFunctions.Operator("<"), Map("a"->Ordered), Seq(VariableType("a"), VariableType("a")), None, FixedType(TestBoolean))
  val Lte = Function(SpecialFunctions.Operator("<="), Map("a"->Ordered), Seq(VariableType("a"), VariableType("a")), None, FixedType(TestBoolean))
  val Eq = Function(SpecialFunctions.Operator("="), Map("a"->Ordered), Seq(VariableType("a"), VariableType("a")), None, FixedType(TestBoolean))
  val EqEq = Eq.copy(name = SpecialFunctions.Operator("=="))
  val Neq = Function(SpecialFunctions.Operator("<>"), Map("a"->Ordered), Seq(VariableType("a"), VariableType("a")), None, FixedType(TestBoolean))
  val BangEq = Neq.copy(name = SpecialFunctions.Operator("!="))

  // arguments: lat, lon, distance in meter
  val WithinCircle = Function(FunctionName("within_circle"), Map ("a"-> GeospatialLike, "b" -> RealNumLike),
    Seq(VariableType("a"), VariableType("b"), VariableType("b"), VariableType("b")), None, FixedType(TestBoolean))
  // arguments: nwLat, nwLon, seLat, seLon (yMax,  xMin , yMin,  xMax)
  val WithinBox = Function(FunctionName("within_box"), Map ("a"-> GeospatialLike, "b" -> RealNumLike),
    Seq(VariableType("a"), VariableType("b"), VariableType("b"), VariableType("b"), VariableType("b")), None, FixedType(TestBoolean))

  val LatitudeField = new MonomorphicFunction(SpecialFunctions.Subscript, Seq(TestLocation, TestTextLiteral("latitude")), None, TestDouble).function
  val LongitudeField = new MonomorphicFunction(SpecialFunctions.Subscript, Seq(TestLocation, TestTextLiteral("longitude")), None, TestDouble).function

  val IsNull = Function(SpecialFunctions.IsNull, Map.empty, Seq(VariableType("a")), None, FixedType(TestBoolean))
  val IsNotNull = Function(SpecialFunctions.IsNotNull, Map.empty, Seq(VariableType("a")), None, FixedType(TestBoolean))

  val Between = Function(SpecialFunctions.Between, Map("a"->Ordered), Seq(VariableType("a"),VariableType("a"),VariableType("a")), None, FixedType(TestBoolean))
  val NotBetween = Function(SpecialFunctions.NotBetween, Map("a"->Ordered), Seq(VariableType("a"),VariableType("a"),VariableType("a")), None, FixedType(TestBoolean))

  val Min = Function(FunctionName("min"), Map("a"->Ordered), Seq(VariableType("a")), None, VariableType("a"), isAggregate = true)
  val Max = Function(FunctionName("max"), Map("a"->Ordered), Seq(VariableType("a")), None, VariableType("a"), isAggregate = true)
  val CountStar = new MonomorphicFunction(SpecialFunctions.StarFunc("count"), Seq(), None, TestNumber, isAggregate = true).function
  val Count = Function(FunctionName("count"), Map.empty, Seq(VariableType("a")), None, FixedType(TestNumber), isAggregate = true)
  val Sum = Function(FunctionName("sum"), Map("a"->NumLike), Seq(VariableType("a")), None, VariableType("a"), isAggregate = true)
  val Avg = Function(FunctionName("avg"), Map("a"->NumLike), Seq(VariableType("a")), None, VariableType("a"), isAggregate = true)

  val UnaryPlus = Function(SpecialFunctions.Operator("+"), Map("a"->NumLike), Seq(VariableType("a")), None, VariableType("a"))
  val UnaryMinus = Function(SpecialFunctions.Operator("-"), Map("a"->NumLike), Seq(VariableType("a")), None, VariableType("a"))

  val BinaryPlus = Function(SpecialFunctions.Operator("+"), Map("a"->NumLike), Seq(VariableType("a"), VariableType("a")), None, VariableType("a"))
  val BinaryMinus = Function(SpecialFunctions.Operator("-"), Map("a"->NumLike), Seq(VariableType("a"), VariableType("a")), None, VariableType("a"))

  val TimesNumNum = new MonomorphicFunction(SpecialFunctions.Operator("*"), Seq(TestNumber, TestNumber), None, TestNumber).function
  val TimesDoubleDouble = new MonomorphicFunction(SpecialFunctions.Operator("*"), Seq(TestDouble, TestDouble), None, TestDouble).function
  val TimesNumMoney = new MonomorphicFunction(SpecialFunctions.Operator("*"), Seq(TestNumber, TestMoney), None, TestMoney).function
  val TimesMoneyNum = new MonomorphicFunction(SpecialFunctions.Operator("*"), Seq(TestMoney, TestNumber), None, TestMoney).function

  val DivNumNum = new MonomorphicFunction(SpecialFunctions.Operator("/"), Seq(TestNumber, TestNumber), None, TestNumber).function
  val DivDoubleDouble = new MonomorphicFunction(SpecialFunctions.Operator("/"), Seq(TestDouble, TestDouble), None, TestDouble).function
  val DivMoneyNum = new MonomorphicFunction(SpecialFunctions.Operator("/"), Seq(TestMoney, TestNumber), None, TestMoney).function
  val DivMoneyMoney = new MonomorphicFunction(SpecialFunctions.Operator("/"), Seq(TestMoney, TestMoney), None, TestNumber).function

  val NumberToMoney = new MonomorphicFunction(SpecialFunctions.Operator("to_money"), Seq(TestNumber), None, TestMoney).function
  val NumberToDouble = new MonomorphicFunction(SpecialFunctions.Operator("to_double"), Seq(TestNumber), None, TestDouble).function

  val And = new MonomorphicFunction(SpecialFunctions.Operator("and"), Seq(TestBoolean, TestBoolean), None, TestBoolean).function
  val Or = new MonomorphicFunction(SpecialFunctions.Operator("or"), Seq(TestBoolean, TestBoolean), None, TestBoolean).function
  val Not = new MonomorphicFunction(SpecialFunctions.Operator("not"), Seq(TestBoolean), None, TestBoolean).function

  val In = new Function(SpecialFunctions.In, Map.empty, Seq(VariableType("a")), Some(VariableType("a")), FixedType(TestBoolean))
  val NotIn = new Function(SpecialFunctions.NotIn, Map.empty, Seq(VariableType("a")), Some(VariableType("a")), FixedType(TestBoolean))

  val Like = new MonomorphicFunction(SpecialFunctions.Like, Seq(TestText, TestText), None, TestBoolean).function
  val NotLike = new MonomorphicFunction(SpecialFunctions.NotLike, Seq(TestText, TestText), None, TestBoolean).function

  val castIdentities = for((n, t) <- TestType.typesByName.toSeq) yield {
    Function(SpecialFunctions.Cast(n), Map.empty, Seq(FixedType(t)), None, FixedType(t))
  }

  val NumberToText = new MonomorphicFunction(SpecialFunctions.Cast(TestText.name), Seq(TestNumber), None, TestText).function
  val TextToNumber = new MonomorphicFunction(SpecialFunctions.Cast(TestNumber.name), Seq(TestText), None, TestNumber).function

  val Prop = new MonomorphicFunction(SpecialFunctions.Subscript, Seq(TestObject, TestText), None, TestJson).function
  val Index = new MonomorphicFunction(SpecialFunctions.Subscript, Seq(TestArray, TestNumber), None, TestJson).function
  val JsonProp = new MonomorphicFunction(SpecialFunctions.Subscript, Seq(TestJson, TestText), None, TestJson).function
  val JsonIndex = new MonomorphicFunction(SpecialFunctions.Subscript, Seq(TestJson, TestNumber), None, TestJson).function

  val JsonToText = new MonomorphicFunction(SpecialFunctions.Cast(TestText.name), Seq(TestJson), None, TestText).function
  val JsonToNumber = new MonomorphicFunction(SpecialFunctions.Cast(TestNumber.name), Seq(TestJson), None, TestNumber).function
  val JsonToBool = new MonomorphicFunction(SpecialFunctions.Cast(TestBoolean.name), Seq(TestJson), None, TestBoolean).function
  val JsonToObject = new MonomorphicFunction(SpecialFunctions.Cast(TestObject.name), Seq(TestJson), None, TestObject).function
  val JsonToArray = new MonomorphicFunction(SpecialFunctions.Cast(TestArray.name), Seq(TestJson), None, TestArray).function

  def potentialAccessors = for {
    method <- getClass.getMethods
    if Modifier.isPublic(method.getModifiers) && method.getParameterTypes.length == 0
  } yield method

  for {
    potentialAccessor <- potentialAccessors
    if potentialAccessor.getReturnType == classOf[MonomorphicFunction[_]]
  } log.warn("TestFunction accessor with MonomorphicFunction return type: {}; did you forget a .function?", potentialAccessor.getName)

  val allFunctions: Seq[Function[TestType]] = {
    val reflectedFunctions = for {
      method <- getClass.getMethods
      if Modifier.isPublic(method.getModifiers) && method.getParameterTypes.length == 0 && method.getReturnType == classOf[Function[_]]
    } yield method.invoke(this).asInstanceOf[Function[TestType]]
    castIdentities ++ reflectedFunctions
  }

  val nAdicFunctions = TestFunctions.allFunctions.filterNot(_.isVariadic)
  val variadicFunctions = TestFunctions.allFunctions.filter(_.isVariadic)

  locally {
    val sharedNames = nAdicFunctions.map(_.name).toSet intersect variadicFunctions.map(_.name).toSet
    if(sharedNames.nonEmpty) {
      log.warn("Fixed-arity and variable-arity functions the names {}", sharedNames.mkString(","))
    }
  }

  val nAdicFunctionsByNameThenArity = nAdicFunctions.groupBy(_.name).mapValues { fs =>
    fs.groupBy(_.minArity).mapValues(_.toSet).toMap
  }.toMap

  val variadicFunctionsByNameThenMinArity = variadicFunctions.groupBy(_.name).mapValues { fs =>
    fs.groupBy(_.minArity).mapValues(_.toSet).toMap
  }.toMap
}
