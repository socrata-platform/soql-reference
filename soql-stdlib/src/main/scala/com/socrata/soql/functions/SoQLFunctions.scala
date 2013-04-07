package com.socrata.soql.functions

import com.socrata.soql.types._
import com.socrata.soql.ast.SpecialFunctions
import com.socrata.soql.environment.FunctionName
import java.lang.reflect.Modifier

sealed abstract class SoQLFunctions

object SoQLFunctions {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[SoQLFunctions])

  private val Ordered = SoQLTypeConversions.typeParameterUniverse.toSet[Any] // might want to narrow this down
  private val NumLike = Set[Any](SoQLNumber, SoQLDouble, SoQLMoney)
  private val RealNumLike = Set[Any](SoQLNumber, SoQLDouble)
  private val GeospatialLike = Set[Any](SoQLLocation)

  val TextToFixedTimestamp = new MonomorphicFunction(FunctionName("to_fixed_timestamp"), Seq(SoQLText), None, SoQLFixedTimestamp).function
  val TextToFloatingTimestamp = new MonomorphicFunction(FunctionName("to_floating_timestamp"), Seq(SoQLText), None, SoQLFloatingTimestamp).function
  val TextToDate = new MonomorphicFunction(FunctionName("to_date"), Seq(SoQLText), None, SoQLDate).function
  val TextToTime = new MonomorphicFunction(FunctionName("to_time"), Seq(SoQLText), None, SoQLTime).function
  val Concat = Function(SpecialFunctions.Operator("||"), Map.empty, Seq(VariableType("a"), VariableType("b")), None, FixedType(SoQLText))
  val Gte = Function(SpecialFunctions.Operator(">="), Map("a"->Ordered), Seq(VariableType("a"), VariableType("a")), None, FixedType(SoQLBoolean))
  val Gt = Function(SpecialFunctions.Operator(">"), Map("a"->Ordered), Seq(VariableType("a"), VariableType("a")), None, FixedType(SoQLBoolean))
  val Lt = Function(SpecialFunctions.Operator("<"), Map("a"->Ordered), Seq(VariableType("a"), VariableType("a")), None, FixedType(SoQLBoolean))
  val Lte = Function(SpecialFunctions.Operator("<="), Map("a"->Ordered), Seq(VariableType("a"), VariableType("a")), None, FixedType(SoQLBoolean))
  val Eq = Function(SpecialFunctions.Operator("="), Map("a"->Ordered), Seq(VariableType("a"), VariableType("a")), None, FixedType(SoQLBoolean))
  val EqEq = Eq.copy(name = SpecialFunctions.Operator("=="))
  val Neq = Function(SpecialFunctions.Operator("<>"), Map("a"->Ordered), Seq(VariableType("a"), VariableType("a")), None, FixedType(SoQLBoolean))
  val BangEq = Neq.copy(name = SpecialFunctions.Operator("!="))

  // arguments: lat, lon, distance in meter
  val WithinCircle = Function(FunctionName("within_circle"), Map ("a"-> GeospatialLike, "b" -> RealNumLike),
    Seq(VariableType("a"), VariableType("b"), VariableType("b"), VariableType("b")), None, FixedType(SoQLBoolean))
  // arguments: nwLat, nwLon, seLat, seLon (yMax,  xMin , yMin,  xMax)
  val WithinBox = Function(FunctionName("within_box"), Map ("a"-> GeospatialLike, "b" -> RealNumLike),
    Seq(VariableType("a"), VariableType("b"), VariableType("b"), VariableType("b"), VariableType("b")), None, FixedType(SoQLBoolean))

  val LatitudeField = new MonomorphicFunction(SpecialFunctions.Subscript, Seq(SoQLLocation, SoQLTextLiteral("latitude")), None, SoQLDouble).function
  val LongitudeField = new MonomorphicFunction(SpecialFunctions.Subscript, Seq(SoQLLocation, SoQLTextLiteral("longitude")), None, SoQLDouble).function

  val IsNull = Function(SpecialFunctions.IsNull, Map.empty, Seq(VariableType("a")), None, FixedType(SoQLBoolean))
  val IsNotNull = Function(SpecialFunctions.IsNotNull, Map.empty, Seq(VariableType("a")), None, FixedType(SoQLBoolean))

  val Between = Function(SpecialFunctions.Between, Map("a"->Ordered), Seq(VariableType("a"),VariableType("a"),VariableType("a")), None, FixedType(SoQLBoolean))
  val NotBetween = Function(SpecialFunctions.NotBetween, Map("a"->Ordered), Seq(VariableType("a"),VariableType("a"),VariableType("a")), None, FixedType(SoQLBoolean))

  val Min = Function(FunctionName("min"), Map("a"->Ordered), Seq(VariableType("a")), None, VariableType("a"), isAggregate = true)
  val Max = Function(FunctionName("max"), Map("a"->Ordered), Seq(VariableType("a")), None, VariableType("a"), isAggregate = true)
  val CountStar = new MonomorphicFunction(SpecialFunctions.StarFunc("count"), Seq(), None, SoQLNumber, isAggregate = true).function
  val Count = Function(FunctionName("count"), Map.empty, Seq(VariableType("a")), None, FixedType(SoQLNumber), isAggregate = true)
  val Sum = Function(FunctionName("sum"), Map("a"->NumLike), Seq(VariableType("a")), None, VariableType("a"), isAggregate = true)
  val Avg = Function(FunctionName("avg"), Map("a"->NumLike), Seq(VariableType("a")), None, VariableType("a"), isAggregate = true)

  val UnaryPlus = Function(SpecialFunctions.Operator("+"), Map("a"->NumLike), Seq(VariableType("a")), None, VariableType("a"))
  val UnaryMinus = Function(SpecialFunctions.Operator("-"), Map("a"->NumLike), Seq(VariableType("a")), None, VariableType("a"))

  val BinaryPlus = Function(SpecialFunctions.Operator("+"), Map("a"->NumLike), Seq(VariableType("a"), VariableType("a")), None, VariableType("a"))
  val BinaryMinus = Function(SpecialFunctions.Operator("-"), Map("a"->NumLike), Seq(VariableType("a"), VariableType("a")), None, VariableType("a"))

  val TimesNumNum = new MonomorphicFunction(SpecialFunctions.Operator("*"), Seq(SoQLNumber, SoQLNumber), None, SoQLNumber).function
  val TimesDoubleDouble = new MonomorphicFunction(SpecialFunctions.Operator("*"), Seq(SoQLDouble, SoQLDouble), None, SoQLDouble).function
  val TimesNumMoney = new MonomorphicFunction(SpecialFunctions.Operator("*"), Seq(SoQLNumber, SoQLMoney), None, SoQLMoney).function
  val TimesMoneyNum = new MonomorphicFunction(SpecialFunctions.Operator("*"), Seq(SoQLMoney, SoQLNumber), None, SoQLMoney).function

  val DivNumNum = new MonomorphicFunction(SpecialFunctions.Operator("/"), Seq(SoQLNumber, SoQLNumber), None, SoQLNumber).function
  val DivDoubleDouble = new MonomorphicFunction(SpecialFunctions.Operator("/"), Seq(SoQLDouble, SoQLDouble), None, SoQLDouble).function
  val DivMoneyNum = new MonomorphicFunction(SpecialFunctions.Operator("/"), Seq(SoQLMoney, SoQLNumber), None, SoQLMoney).function
  val DivMoneyMoney = new MonomorphicFunction(SpecialFunctions.Operator("/"), Seq(SoQLMoney, SoQLMoney), None, SoQLNumber).function

  val NumberToMoney = new MonomorphicFunction(SpecialFunctions.Operator("to_money"), Seq(SoQLNumber), None, SoQLMoney).function
  val NumberToDouble = new MonomorphicFunction(SpecialFunctions.Operator("to_double"), Seq(SoQLNumber), None, SoQLDouble).function

  val And = new MonomorphicFunction(SpecialFunctions.Operator("and"), Seq(SoQLBoolean, SoQLBoolean), None, SoQLBoolean).function
  val Or = new MonomorphicFunction(SpecialFunctions.Operator("or"), Seq(SoQLBoolean, SoQLBoolean), None, SoQLBoolean).function
  val Not = new MonomorphicFunction(SpecialFunctions.Operator("not"), Seq(SoQLBoolean), None, SoQLBoolean).function

  val In = new Function(SpecialFunctions.In, Map.empty, Seq(VariableType("a")), Some(VariableType("a")), FixedType(SoQLBoolean))
  val NotIn = new Function(SpecialFunctions.NotIn, Map.empty, Seq(VariableType("a")), Some(VariableType("a")), FixedType(SoQLBoolean))

  val Like = new MonomorphicFunction(SpecialFunctions.Like, Seq(SoQLText, SoQLText), None, SoQLBoolean).function
  val NotLike = new MonomorphicFunction(SpecialFunctions.NotLike, Seq(SoQLText, SoQLText), None, SoQLBoolean).function

  val castIdentities = for((n, t) <- SoQLType.typesByName.toSeq) yield {
    Function(SpecialFunctions.Cast(n), Map.empty, Seq(FixedType(t)), None, FixedType(t))
  }

  val NumberToText = new MonomorphicFunction(SpecialFunctions.Cast(SoQLText.name), Seq(SoQLNumber), None, SoQLText).function
  val TextToNumber = new MonomorphicFunction(SpecialFunctions.Cast(SoQLNumber.name), Seq(SoQLText), None, SoQLNumber).function

  val Prop = new MonomorphicFunction(SpecialFunctions.Subscript, Seq(SoQLObject, SoQLText), None, SoQLJson).function
  val Index = new MonomorphicFunction(SpecialFunctions.Subscript, Seq(SoQLArray, SoQLNumber), None, SoQLJson).function
  val JsonProp = new MonomorphicFunction(SpecialFunctions.Subscript, Seq(SoQLJson, SoQLText), None, SoQLJson).function
  val JsonIndex = new MonomorphicFunction(SpecialFunctions.Subscript, Seq(SoQLJson, SoQLNumber), None, SoQLJson).function

  val JsonToText = new MonomorphicFunction(SpecialFunctions.Cast(SoQLText.name), Seq(SoQLJson), None, SoQLText).function
  val JsonToNumber = new MonomorphicFunction(SpecialFunctions.Cast(SoQLNumber.name), Seq(SoQLJson), None, SoQLNumber).function
  val JsonToBool = new MonomorphicFunction(SpecialFunctions.Cast(SoQLBoolean.name), Seq(SoQLJson), None, SoQLBoolean).function
  val JsonToObject = new MonomorphicFunction(SpecialFunctions.Cast(SoQLObject.name), Seq(SoQLJson), None, SoQLObject).function
  val JsonToArray = new MonomorphicFunction(SpecialFunctions.Cast(SoQLArray.name), Seq(SoQLJson), None, SoQLArray).function

  def potentialAccessors = for {
    method <- getClass.getMethods
    if Modifier.isPublic(method.getModifiers) && method.getParameterTypes.length == 0
  } yield method

  for {
    potentialAccessor <- potentialAccessors
    if potentialAccessor.getReturnType == classOf[MonomorphicFunction[_]]
  } log.warn("SoQLFunction accessor with MonomorphicFunction return type: {}; did you forget a .function?", potentialAccessor.getName)

  val allFunctions: Seq[Function[SoQLType]] = {
    val reflectedFunctions = for {
      method <- getClass.getMethods
      if Modifier.isPublic(method.getModifiers) && method.getParameterTypes.length == 0 && method.getReturnType == classOf[Function[_]]
    } yield method.invoke(this).asInstanceOf[Function[SoQLType]]
    castIdentities ++ reflectedFunctions
  }

  val nAdicFunctions = SoQLFunctions.allFunctions.filterNot(_.isVariadic)
  val variadicFunctions = SoQLFunctions.allFunctions.filter(_.isVariadic)

  locally {
    val sharedNames = nAdicFunctions.map(_.name).toSet intersect variadicFunctions.map(_.name).toSet
    if(sharedNames.nonEmpty) {
      log.warn("Fixed-arity and variable-arity functions the names {}", sharedNames.mkString(","))
    }
  }

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
