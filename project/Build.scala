import sbt._
import Keys._

object Build extends sbt.Build {
  lazy val build = Project(
    "build",
    file("."),
    settings = BuildSettings.buildSettings
  ) aggregate (soqlParser, soqlToy)

  lazy val soqlParser = Project(
    "soql-parser",
    file("soql-parser"),
    settings = SoqlParser.settings
  )

  lazy val soqlToy = Project(
    "soql-toy",
    file("soql-toy"),
    settings = SoqlToy.settings
  ) dependsOn(soqlParser)
}
