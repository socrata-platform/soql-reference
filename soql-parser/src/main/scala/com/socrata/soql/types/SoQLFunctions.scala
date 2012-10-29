package com.socrata.soql.types

import com.socrata.soql.names.FunctionName
import com.socrata.soql.functions.{FixedType, VariableType, MonomorphicFunction, Function}
import java.lang.reflect.Modifier
import com.socrata.soql.ast.SpecialFunctions

object SoQLFunctions {
  private val Ordered = SoQLTypeConversions.typeParameterUniverse.toSet[Any] // might want to narrow this down
  private val NumLike = Set[Any](SoQLNumber, SoQLDouble, SoQLMoney)

  val TextToFixedTimestamp = new MonomorphicFunction(FunctionName("to_fixed_timestamp"), Seq(SoQLText), SoQLFixedTimestamp).function
  val TextToFloatingTimestamp = new MonomorphicFunction(FunctionName("to_floating_timestamp"), Seq(SoQLText), SoQLFloatingTimestamp).function
  val Concat = Function(SpecialFunctions.Operator("||"), Map.empty, Seq(VariableType("a"), VariableType("b")), FixedType(SoQLText))
  val Gte = Function(SpecialFunctions.Operator(">="), Map("a"->Ordered), Seq(VariableType("a"), VariableType("a")), FixedType(SoQLBoolean))
  val Gt = Function(SpecialFunctions.Operator(">"), Map("a"->Ordered), Seq(VariableType("a"), VariableType("a")), FixedType(SoQLBoolean))
  val Lt = Function(SpecialFunctions.Operator("<"), Map("a"->Ordered), Seq(VariableType("a"), VariableType("a")), FixedType(SoQLBoolean))
  val Lte = Function(SpecialFunctions.Operator("<="), Map("a"->Ordered), Seq(VariableType("a"), VariableType("a")), FixedType(SoQLBoolean))
  val Eq = Function(SpecialFunctions.Operator("="), Map("a"->Ordered), Seq(VariableType("a"), VariableType("a")), FixedType(SoQLBoolean))
  val EqEq = Eq.copy(name = SpecialFunctions.Operator("=="))
  val Neq = Function(SpecialFunctions.Operator("<>"), Map("a"->Ordered), Seq(VariableType("a"), VariableType("a")), FixedType(SoQLBoolean))
  val BangEq = Neq.copy(name = SpecialFunctions.Operator("!="))

  val LatitudeField = new MonomorphicFunction(SpecialFunctions.Subscript, Seq(SoQLLocation, SoQLTextLiteral("latitude")), SoQLDouble).function
  val LongitudeField = new MonomorphicFunction(SpecialFunctions.Subscript, Seq(SoQLLocation, SoQLTextLiteral("longitude")), SoQLDouble).function

  val IsNull = Function(SpecialFunctions.IsNull, Map.empty, Seq(VariableType("a")), FixedType(SoQLBoolean))
  val IsNotNull = Function(SpecialFunctions.IsNotNull, Map.empty, Seq(VariableType("a")), FixedType(SoQLBoolean))

  val Between = Function(SpecialFunctions.Between, Map("a"->Ordered), Seq(VariableType("a"),VariableType("a"),VariableType("a")), FixedType(SoQLBoolean))
  val NotBetween = Function(SpecialFunctions.NotBetween, Map("a"->Ordered), Seq(VariableType("a"),VariableType("a"),VariableType("a")), FixedType(SoQLBoolean))

  val Min = Function(FunctionName("min"), Map("a"->Ordered), Seq(VariableType("a")), VariableType("a"), isAggregate = true)
  val Max = Function(FunctionName("max"), Map("a"->Ordered), Seq(VariableType("a")), VariableType("a"), isAggregate = true)
  val CountStar = new MonomorphicFunction(SpecialFunctions.StarFunc("count"), Seq(), SoQLNumber, isAggregate = true).function
  val Count = Function(FunctionName("count"), Map.empty, Seq(VariableType("a")), FixedType(SoQLNumber), isAggregate = true)
  val Sum = Function(FunctionName("sum"), Map("a"->NumLike), Seq(VariableType("a")), VariableType("a"), isAggregate = true)
  val Avg = Function(FunctionName("avg"), Map("a"->NumLike), Seq(VariableType("a")), VariableType("a"), isAggregate = true)

  val UnaryPlus = Function(SpecialFunctions.Operator("+"), Map("a"->NumLike), Seq(VariableType("a")), VariableType("a"))
  val UnaryMinus = Function(SpecialFunctions.Operator("-"), Map("a"->NumLike), Seq(VariableType("a")), VariableType("a"))

  val BinaryPlus = Function(SpecialFunctions.Operator("+"), Map("a"->NumLike), Seq(VariableType("a"), VariableType("a")), VariableType("a"))
  val BinaryMinus = Function(SpecialFunctions.Operator("-"), Map("a"->NumLike), Seq(VariableType("a"), VariableType("a")), VariableType("a"))

  val TimesNumNum = new MonomorphicFunction(SpecialFunctions.Operator("*"), Seq(SoQLNumber, SoQLNumber), SoQLNumber).function
  val TimesDoubleDouble = new MonomorphicFunction(SpecialFunctions.Operator("*"), Seq(SoQLDouble, SoQLDouble), SoQLDouble).function
  val TimesNumMoney = new MonomorphicFunction(SpecialFunctions.Operator("*"), Seq(SoQLNumber, SoQLMoney), SoQLMoney).function
  val TimesMoneyNum = new MonomorphicFunction(SpecialFunctions.Operator("*"), Seq(SoQLMoney, SoQLNumber), SoQLMoney).function

  val DivNumNum = new MonomorphicFunction(SpecialFunctions.Operator("/"), Seq(SoQLNumber, SoQLNumber), SoQLNumber).function
  val DivDoubleDouble = new MonomorphicFunction(SpecialFunctions.Operator("/"), Seq(SoQLDouble, SoQLDouble), SoQLDouble).function
  val DivMoneyNum = new MonomorphicFunction(SpecialFunctions.Operator("/"), Seq(SoQLMoney, SoQLNumber), SoQLMoney).function
  val DivMoneyMoney = new MonomorphicFunction(SpecialFunctions.Operator("/"), Seq(SoQLMoney, SoQLMoney), SoQLNumber).function

  val NumberToMoney = new MonomorphicFunction(SpecialFunctions.Operator("to_money"), Seq(SoQLNumber), SoQLMoney).function
  val NumberToDouble = new MonomorphicFunction(SpecialFunctions.Operator("to_double"), Seq(SoQLNumber), SoQLDouble).function

  val And = new MonomorphicFunction(SpecialFunctions.Operator("and"), Seq(SoQLBoolean, SoQLBoolean), SoQLBoolean).function
  val Or = new MonomorphicFunction(SpecialFunctions.Operator("or"), Seq(SoQLBoolean, SoQLBoolean), SoQLBoolean).function
  val Not = new MonomorphicFunction(SpecialFunctions.Operator("not"), Seq(SoQLBoolean), SoQLBoolean).function

  val castIdentities = for((n, t) <- SoQLType.typesByName.toSeq) yield {
    Function(SpecialFunctions.Cast(n), Map.empty, Seq(FixedType(t)), FixedType(t))
  }

  lazy val allFunctions: Seq[Function[SoQLType]] = {
    val reflectedFunctions = for {
      method <- getClass.getMethods
      if Modifier.isPublic(method.getModifiers) && method.getParameterTypes.length == 0 && method.getReturnType == classOf[Function[_]]
    } yield method.invoke(this).asInstanceOf[Function[SoQLType]]
    castIdentities ++ reflectedFunctions
  }

  lazy val functionsByNameThenArity = SoQLFunctions.allFunctions.groupBy(_.name).mapValues { fs =>
    fs.groupBy(_.arity).mapValues(_.toSet).toMap
  }.toMap
}
