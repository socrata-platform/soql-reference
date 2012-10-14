import sbt._
import Keys._

object Build extends sbt.Build {
  lazy val build = Project(
    "build",
    file("."),
    settings = BuildSettings.buildSettings
  ) aggregate (soqlParser)

  lazy val soqlParser = Project(
    "soql-parser",
    file("soql-parser"),
    settings = SoqlParser.settings
  )
}
