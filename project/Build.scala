import sbt._
import Keys._

object Build extends sbt.Build {
  lazy val build = Project(
    "build",
    file("."),
    settings = BuildSettings.buildSettings
  ) aggregate ()
}
