import sbt._
import Keys._

object SoqlEnvironment{
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    name := "soql-environment",
    libraryDependencies ++= Seq(
      "com.rojoma" %% "rojoma-json-v3" % "3.13.0",
      "com.ibm.icu" % "icu4j" % "63.1"
    )
  )
}
