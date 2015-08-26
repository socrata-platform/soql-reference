import sbt._
import Keys._

object SoqlParser {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings ++ Seq(
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
      "com.socrata" %% "soql-brita" % "[1.3.0,2.0.0)",
      "org.slf4j" % "slf4j-simple" % BuildSettings.slf4jVersion % "test"
    )
  )
}
