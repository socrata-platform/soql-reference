resolvers := Seq(
  "socrata maven" at "https://repo.socrata.com/artifactory/libs-release",
  Resolver.url("socrata ivy", new URL("https://repo.socrata.com/artifactory/ivy-libs-release"))(Resolver.ivyStylePatterns)
)

externalResolvers <<= resolvers map { rs =>
  Resolver.withDefaultResolvers(rs, mavenCentral = false)
}

addSbtPlugin("com.socrata" % "socrata-sbt" % "0.2.6")

libraryDependencies ++= Seq(
  "de.jflex" % "jflex" % "1.4.3",
  "org.apache.ant" % "ant" % "1.8.4"
)
