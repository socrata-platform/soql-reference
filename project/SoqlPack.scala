import sbt._
import Keys._

object SoqlPack {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings ++ Seq(
    libraryDependencies ++=
      Seq(
        // Only used by serialization
        "org.velvia"         %% "msgpack4s"                % "0.4.3"
      ),
    resolvers += "velvia maven" at "http://dl.bintray.com/velvia/maven"
  )
}
