import sbt._
import Keys._

object SoqlPack {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    name := "soql-pack",
    libraryDependencies ++=
      Seq(
        // Only used by serialization
        "org.velvia"         %% "msgpack4s"                % "0.4.3",
        "com.rojoma" %% "simple-arm" % "1.2.0"
      ),
    externalResolvers += "Socrata Jcenter Mirror" at "https://repo.socrata.com/artifactory/jcenter"
  )
}
