import sbt._
import Keys._

object SoqlJsonUtils{
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    name := "soql-json-utils",
    libraryDependencies ++= Seq(
      "com.rojoma" %% "rojoma-json-v3" % "3.13.0"
    )
  )
}
