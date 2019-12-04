lazy val root = (project in file(".")).
  aggregate (soqlEnvironment, soqlParser, soqlAnalyzer, soqlTypes, soqlStdlib, soqlToy, soqlPack)

lazy val soqlEnvironment = (project in file("soql-environment")).
  settings(SoqlEnvironment.settings)

lazy val soqlParser = (project in file("soql-standalone-parser")).
  settings(SoqlParser.settings).
  dependsOn(soqlEnvironment, soqlTypes % "test")

lazy val soqlAnalyzer = (project in file("soql-analyzer")).
  settings(SoqlAnalyzer.settings).
  dependsOn(soqlParser, soqlTypes % "test")

lazy val soqlTypes = (project in file("soql-types")).
  settings(SoqlTypes.settings).
  dependsOn(soqlEnvironment)

lazy val soqlStdlib = (project in file("soql-stdlib")).
  settings(SoqlStdlib.settings).
  dependsOn(soqlAnalyzer, soqlTypes)

lazy val soqlToy = (project in file("soql-toy")).
  settings(SoqlToy.settings).
  dependsOn(soqlStdlib)

lazy val soqlPack = (project in file("soql-pack")).
  settings(SoqlPack.settings).
  dependsOn(soqlTypes)
