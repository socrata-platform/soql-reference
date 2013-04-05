import sbt._
import Keys._

object SoqlStdlib {
  lazy val settings: Seq[Setting[_]] = BuildSettings.buildSettings ++ Seq(
    libraryDependencies ++= Seq(
      "joda-time" % "joda-time" % "2.1",
      "org.joda" % "joda-convert" % "1.2"
    )
  )
}
