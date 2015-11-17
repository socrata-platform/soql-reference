import sbt._
import Keys._

object Build extends sbt.Build {
  lazy val build = Project(
    "soql",
    file("."),
    settings = BuildSettings.buildSettings
  ) aggregate (soqlEnvironment, soqlParser, soqlAnalyzer, soqlTypes, soqlStdlib, soqlToy, soqlPack)

  lazy val soqlEnvironment = Project(
    "soql-environment",
    file("soql-environment"),
    settings = SoqlEnvironment.settings
  )

  lazy val soqlParser = Project(
    "soql-standalone-parser",
    file("soql-standalone-parser"),
    settings = SoqlParser.settings
  ) dependsOn (soqlEnvironment, soqlTypes % "test")

  lazy val soqlAnalyzer = Project(
    "soql-analyzer",
    file("soql-analyzer"),
    settings = SoqlAnalyzer.settings
  ) dependsOn (soqlParser, soqlTypes % "test")

  lazy val soqlTypes = Project(
    "soql-types",
    file("soql-types"),
    settings = SoqlTypes.settings
  ) dependsOn (soqlEnvironment)

  lazy val soqlStdlib = Project(
    "soql-stdlib",
    file("soql-stdlib"),
    settings = SoqlStdlib.settings
  ) dependsOn (soqlAnalyzer, soqlTypes)

  lazy val soqlToy = Project(
    "soql-toy",
    file("soql-toy"),
    settings = SoqlToy.settings
  ) dependsOn(soqlStdlib)

  lazy val soqlPack = Project(
    "soql-pack",
    file("soql-pack"),
    settings = SoqlPack.settings
  ) dependsOn (soqlTypes)
}
