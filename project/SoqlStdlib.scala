import sbt._
import Keys._

object SoqlStdlib {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    name := "soql-stdlib",
    libraryDependencies ++= {
      if(scalaVersion.value.startsWith("2.13")) Nil
      else Seq(compilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full))
    },
    scalacOptions ++= {
      if(scalaVersion.value.startsWith("2.13")) Seq("-Ymacro-annotations")
      else Nil
    }
  )
}
