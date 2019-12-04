import sbt._
import Keys._

object SoqlStdlib {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    name := "soql-stdlib"
  )
}
