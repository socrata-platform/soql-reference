import com.typesafe.tools.mima.plugin.MimaKeys.previousArtifact
import sbt.Keys._
import sbt._

object BuildSettings {
  val buildSettings: Seq[Setting[_]] = Defaults.coreDefaultSettings ++ Seq(
  )

  def projectSettings: Seq[Setting[_]] = buildSettings ++ Seq(
    // TODO: enable style checks fail the build
    com.socrata.sbtplugins.StylePlugin.StyleKeys.styleFailOnError in Compile := false,
    // TODO: default coverage percent, fail the build
    scoverage.ScoverageSbtPlugin.ScoverageKeys.coverageFailOnMinimum := false,
    scoverage.ScoverageSbtPlugin.ScoverageKeys.coverageMinimum := 60,
    // Haven't made a stable release of this yet
    previousArtifact <<= (scalaBinaryVersion,name) { (sv,name) => None /*Some("com.socrata" % (name + "_" + sv) % "0.1.0")*/ },
    testOptions in Test ++= Seq(
      Tests.Argument(TestFrameworks.ScalaTest, "-oFD")
    ),
    scalacOptions += "-language:implicitConversions",
    libraryDependencies ++= Seq(
        "org.slf4j" % "slf4j-api" % slf4jVersion,
        "org.slf4j" % "jcl-over-slf4j" % slf4jVersion
      )
  )

  val slf4jVersion = "1.7.5"
}
