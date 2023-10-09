lazy val root = (project in file(".")).
  settings(BuildSettings.buildSettings ++ Seq(publishArtifact := false)).
  aggregate (soqlEnvironment, soqlParser, soqlSerialize, soqlAnalyzer, soqlTypes, soqlStdlib, soqlToy, soqlPack, soqlUtils, soqlDocs, soqlSqlizer)

lazy val soqlEnvironment = (project in file("soql-environment")).
  settings(SoqlEnvironment.settings)

lazy val soqlParser = (project in file("soql-standalone-parser")).
  settings(SoqlParser.settings).
  dependsOn(soqlEnvironment, soqlTypes % "test")

lazy val soqlSerialize = (project in file("soql-serialize")).
  settings(SoqlSerialize.settings).
  dependsOn(soqlEnvironment)

lazy val soqlAnalyzer = (project in file("soql-analyzer")).
  settings(SoqlAnalyzer.settings).
  dependsOn(soqlParser, soqlSerialize, soqlUtils, soqlTypes % "test")

lazy val soqlTypes = (project in file("soql-types")).
  settings(SoqlTypes.settings).
  dependsOn(soqlEnvironment, soqlSerialize)

lazy val soqlStdlib = (project in file("soql-stdlib")).
  settings(SoqlStdlib.settings).
  dependsOn(soqlAnalyzer, soqlTypes)

lazy val soqlToy = (project in file("soql-toy")).
  settings(SoqlToy.settings).
  dependsOn(soqlStdlib)

lazy val soqlPack = (project in file("soql-pack")).
  settings(SoqlPack.settings).
  dependsOn(soqlTypes)

lazy val soqlDocs = (project in file("soql-docs")).
  settings(SoqlDocs.settings).
  dependsOn(soqlStdlib)

lazy val soqlUtils = (project in file("soql-utils")).
  settings(SoqlUtils.settings).
  dependsOn(soqlSerialize)

lazy val soqlSqlizer = (project in file("soql-sqlizer")).
  settings(SoqlSqlizer.settings).
  dependsOn(soqlAnalyzer)

val soqldoc = inputKey[Unit]("Build soql documentation")

soqldoc := {
  val arg: File = sbt.complete.DefaultParsers.fileParser(baseDirectory.value).parsed
  val classpath = (soqlDocs / Runtime / fullClasspath).value.map(_.data.toURI.toURL)
  // The system class loader is the app class loader, which contains
  // SBT's copy of Scala, so we want to move up a step to _its_ parent
  // to avoid scala version clashes.
  val classloader = new java.net.URLClassLoader(classpath.toArray, ClassLoader.getSystemClassLoader().getParent())
  val mainClass = classloader.loadClass("com.socrata.soql.docs.Docs")
  val mainMethod = mainClass.getDeclaredMethod("generate", classOf[File])
  mainMethod.invoke(null, arg)
}
