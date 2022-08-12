import sbt._
import Keys._

object SoqlExplore {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++ Seq(
    name := "soql-explore",
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-simple" % BuildSettings.slf4jVersion,
      "com.socrata" %% "socrata-http-jetty" % "3.15.1"
    ),
    evictionErrorLevel := Level.Warn
  )
}
