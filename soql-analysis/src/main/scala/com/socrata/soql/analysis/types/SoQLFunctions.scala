package com.socrata.soql.analysis.types

import com.socrata.soql.names.FunctionName
import com.socrata.soql.analysis.{FixedType, VariableType, MonomorphicFunction, Function}
import java.lang.reflect.Modifier
import com.socrata.soql.ast.SpecialFunctions

object SoQLFunctions {
  val TextToFixedTimestamp = new MonomorphicFunction(FunctionName("to_fixed_timestamp"), Seq(SoQLText), SoQLFixedTimestamp).function
  val TextToFloatingTimestamp = new MonomorphicFunction(FunctionName("to_floating_timestamp"), Seq(SoQLText), SoQLFloatingTimestamp).function
  val Concat = Function(SpecialFunctions.Operator("||"), Seq(VariableType("a"), VariableType("b")), FixedType(SoQLText))
  val Gte = Function(SpecialFunctions.Operator(">="), Seq(VariableType("a"), VariableType("a")), FixedType(SoQLBoolean))
  val Gt = Function(SpecialFunctions.Operator(">"), Seq(VariableType("a"), VariableType("a")), FixedType(SoQLBoolean))
  val Lt = Function(SpecialFunctions.Operator("<"), Seq(VariableType("a"), VariableType("a")), FixedType(SoQLBoolean))
  val Lte = Function(SpecialFunctions.Operator("<="), Seq(VariableType("a"), VariableType("a")), FixedType(SoQLBoolean))
  val Eq = Function(SpecialFunctions.Operator("="), Seq(VariableType("a"), VariableType("a")), FixedType(SoQLBoolean))
  val Neq = Function(SpecialFunctions.Operator("<>"), Seq(VariableType("a"), VariableType("a")), FixedType(SoQLBoolean))
  val LatitudeField = new MonomorphicFunction(SpecialFunctions.Subscript, Seq(SoQLLocation, SoQLTextLiteral("latitude")), SoQLDouble).function
  val LongitudeField = new MonomorphicFunction(SpecialFunctions.Subscript, Seq(SoQLLocation, SoQLTextLiteral("longitude")), SoQLDouble).function
  val IsNotNull = Function(SpecialFunctions.IsNotNull, Seq(VariableType("a")), FixedType(SoQLBoolean))
  val Max = Function(FunctionName("max"), Seq(VariableType("a")), VariableType("a"), isAggregate = true)
  val CountStar = Function(SpecialFunctions.StarFunc("count"), Seq(), FixedType(SoQLNumber), isAggregate = true)

  val PlusNumNum = new MonomorphicFunction(SpecialFunctions.Operator("+"), Seq(SoQLNumber, SoQLNumber), SoQLNumber).function
  val PlusMoneyMoney = new MonomorphicFunction(SpecialFunctions.Operator("+"), Seq(SoQLMoney, SoQLMoney), SoQLMoney).function
  val PlusDoubleDouble = new MonomorphicFunction(SpecialFunctions.Operator("+"), Seq(SoQLDouble, SoQLDouble), SoQLDouble).function

  val TimesNumNum = new MonomorphicFunction(SpecialFunctions.Operator("*"), Seq(SoQLNumber, SoQLNumber), SoQLNumber).function
  val TimesDoubleDouble = new MonomorphicFunction(SpecialFunctions.Operator("*"), Seq(SoQLDouble, SoQLDouble), SoQLDouble).function
  val TimesNumMoney = new MonomorphicFunction(SpecialFunctions.Operator("*"), Seq(SoQLNumber, SoQLMoney), SoQLMoney).function
  val TimesMoneyNum = new MonomorphicFunction(SpecialFunctions.Operator("*"), Seq(SoQLMoney, SoQLNumber), SoQLMoney).function

  val NumberToMoney = new MonomorphicFunction(SpecialFunctions.Operator("to_money"), Seq(SoQLNumber), SoQLMoney).function
  val NumberToDouble = new MonomorphicFunction(SpecialFunctions.Operator("to_double"), Seq(SoQLNumber), SoQLDouble).function

  val And = new MonomorphicFunction(FunctionName("and"), Seq(SoQLBoolean, SoQLBoolean), SoQLBoolean).function
  val Or = new MonomorphicFunction(FunctionName("or"), Seq(SoQLBoolean, SoQLBoolean), SoQLBoolean).function
  val Not = new MonomorphicFunction(FunctionName("not"), Seq(SoQLBoolean), SoQLBoolean).function

  lazy val allFunctions = {
    for {
      method <- getClass.getMethods
      if Modifier.isPublic(method.getModifiers) && method.getParameterTypes.length == 0 && method.getReturnType == classOf[Function[_]]
    } yield method.invoke(this).asInstanceOf[Function[SoQLType]]
  }

  lazy val functionsByNameThenArity = SoQLFunctions.allFunctions.groupBy(_.name).mapValues { fs =>
    fs.groupBy(_.arity).mapValues(_.toSet).toMap
  }.toMap
}
