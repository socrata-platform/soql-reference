import sbt._
import Keys._

object SoqlEnvironment{
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    name := "soql-environment",
    libraryDependencies += "com.ibm.icu" % "icu4j" % "63.1",
    Compile / unmanagedSourceDirectories += {
      val sharedSourceDir = baseDirectory.value / "src/main"
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((3, _) | (2, 13)) =>
          sharedSourceDir / "scala-2.13"
        case _ =>
          sharedSourceDir / "scala-2.11_2.12"
      }
    }
  )
}
