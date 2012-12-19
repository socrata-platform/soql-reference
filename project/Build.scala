import sbt._
import Keys._

object Build extends sbt.Build {
  lazy val build = Project(
    "soql",
    file("."),
    settings = BuildSettings.buildSettings
  ) aggregate (soqlAnalyzer, soqlToy)

  lazy val soqlAnalyzer = Project(
    "soql-analyzer",
    file("soql-analyzer"),
    settings = SoqlAnalyzer.settings
  )

  lazy val soqlToy = Project(
    "soql-toy",
    file("soql-toy"),
    settings = SoqlToy.settings
  ) dependsOn(soqlAnalyzer)
}
