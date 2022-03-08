import sbt._
import Keys._

object SoqlStdlibCodecs {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    name := "soql-stdlib-codec",
    scalacOptions ++= {
      if(scalaVersion.value.startsWith("2.13")) Seq("-Ymacro-annotations")
      else Nil
    }
  )
}
