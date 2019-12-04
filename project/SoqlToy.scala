import sbt._
import Keys._

object SoqlToy {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++ Seq(
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-simple" % BuildSettings.slf4jVersion,
      "org.scalacheck" %% "scalacheck" % "1.13.4" % "test")
  )
}
