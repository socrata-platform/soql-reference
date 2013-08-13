import sbt._
import Keys._

import sbtassembly.Plugin._
import AssemblyKeys._

object SoqlToy {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++ Seq(
    libraryDependencies += "org.slf4j" % "slf4j-simple" % BuildSettings.slf4jVersion
  )
}
