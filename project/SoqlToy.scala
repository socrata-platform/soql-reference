import sbt._
import Keys._

object SoqlToy {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++ Seq(
    name := "soql-toy",
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-simple" % BuildSettings.slf4jVersion,
      "org.jline" % "jline-reader" % "3.28.0",
      "org.scalacheck" %% "scalacheck" % "1.14.0" % "test"),
    run/fork := true
  )
}
