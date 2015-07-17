import sbt._
import Keys._

object SoqlToy {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings ++ Seq(
    libraryDependencies += "org.slf4j" % "slf4j-simple" % BuildSettings.slf4jVersion
  )
}
