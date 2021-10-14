import sbt._
import Keys._

object SoqlEnvironment{
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    name := "soql-environment",
    libraryDependencies += "com.ibm.icu" % "icu4j" % "63.1"
  )
}
