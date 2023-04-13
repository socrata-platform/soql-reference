import sbt._
import Keys._

object SoqlSerialize {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    name := "soql-serialize",
    libraryDependencies ++= Seq(
      "com.google.protobuf" % "protobuf-java" % "2.6.1"
    ),
    Compile / sourceGenerators += Def.task { TuplesBuilder.write((Compile / sourceManaged).value) },
    Compile / sourceGenerators += Def.task { TuplesBuilder.read((Compile / sourceManaged).value) }
  )
}
