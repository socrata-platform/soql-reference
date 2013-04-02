addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "0.1.5")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.8.7")

libraryDependencies ++= Seq(
  "de.jflex" % "jflex" % "1.4.3",
  "org.apache.ant" % "ant" % "1.8.4"
)
