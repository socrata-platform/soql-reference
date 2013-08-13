import sbt._
import Keys._

object SoqlAnalyzer {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    sourceGenerators in Compile <+= (sourceManaged in Compile, sourceDirectory in Compile) map { (sourceManaged, sourceDir) =>
      val task = new JFlex.anttask.JFlexTask
      task.setDestdir(sourceManaged)
      task.setFile(sourceDir / "jflex" / "SoQLLexer.flex")
      task.execute()
      // and now for some ugliness!
      val outputDir = task.getClass.getDeclaredField("outputDir")
      outputDir.setAccessible(true)
      val className = task.getClass.getDeclaredField("className")
      className.setAccessible(true)
      Seq(new File(outputDir.get(task).asInstanceOf[File], className.get(task).asInstanceOf[String] + ".java"))
    },
    libraryDependencies ++= Seq(
      "com.socrata" %% "soql-brita" % "[1.2.1,2.0.0)",

      // These two are only used by analysis-serialization
      "com.google.protobuf" % "protobuf-java" % "2.4.1" % "optional",
      "net.sf.trove4j" % "trove4j" % "3.0.3" % "optional",

      "org.slf4j" % "slf4j-simple" % BuildSettings.slf4jVersion % "test"
    )
  )
}
