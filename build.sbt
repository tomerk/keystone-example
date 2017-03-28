import AssemblyKeys._

assemblySettings

name := "keystone-example"

version := "0.4"

organization := "edu.berkeley.cs.amplab"

scalaVersion := "2.11.8"

parallelExecution in Test := false

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.2",
  "org.slf4j" % "slf4j-log4j12" % "1.7.2",
  "org.scalatest" %% "scalatest" % "1.9.1" % "test",
  "org.jsoup" % "jsoup" % "1.8.3",
  "com.google.code.gson" % "gson" % "2.8.0",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7",
  "com.googlecode.json-simple" % "json-simple" % "1.1.1",
  // These next two are for jsonp
  "javax.json" % "javax.json-api" % "1.0",
  "org.glassfish" % "javax.json" % "1.0.4",
  // The following are for regex
  "dk.brics.automaton" % "automaton" % "1.11-8",
  "jakarta-regexp" % "jakarta-regexp" % "1.4",
  "com.stevesoft.pat" % "pat" % "1.5.3",
  "net.sourceforge.jregex" % "jregex" % "1.2_01",
  "oro" % "oro" % "2.0.8",
  "gnu-regexp" % "gnu-regexp" % "1.1.4",
  "jrexx" % "jrexx" % "1.1.1",
  "com.basistech.tclre" % "tcl-regex" % "0.13.6"
)

{
  val defaultSparkVersion = "2.1.0-bandits-snapshot"
  val sparkVersion =
    scala.util.Properties.envOrElse("SPARK_VERSION", defaultSparkVersion)
  val excludeHadoop = ExclusionRule(organization = "org.apache.hadoop")
  val excludeSpark = ExclusionRule(organization = "org.apache.spark")
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion excludeAll(excludeHadoop),
    "org.apache.spark" %% "spark-mllib" % sparkVersion excludeAll(excludeHadoop),
    "org.apache.spark" %% "spark-sql" % sparkVersion excludeAll(excludeHadoop),
    "edu.berkeley.cs.amplab" %% "keystoneml" % "0.4.0" excludeAll(excludeHadoop, excludeSpark)
  )
}

{
  val defaultHadoopVersion = "2.6.0"
  val hadoopVersion =
    scala.util.Properties.envOrElse("SPARK_HADOOP_VERSION", defaultHadoopVersion)
  libraryDependencies += "org.apache.hadoop" % "hadoop-client" % hadoopVersion
}

resolvers ++= Seq(
  "Local Maven Repository" at Path.userHome.asFile.toURI.toURL + ".m2/repository",
  "Typesafe" at "http://repo.typesafe.com/typesafe/releases",
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Spray" at "http://repo.spray.cc",
  "Bintray" at "http://dl.bintray.com/jai-imageio/maven/",
  "ImageJ Public Maven Repo" at "http://maven.imagej.net/content/groups/public/"
)

resolvers += Resolver.sonatypeRepo("public")

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("javax", "servlet", xs @ _*)               => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".html"       => MergeStrategy.first
    case "application.conf"                                  => MergeStrategy.concat
    case "reference.conf"                                    => MergeStrategy.concat
    case "log4j.properties"                                  => MergeStrategy.first
    case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
    case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
    case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
    case _ => MergeStrategy.first
  }
}

test in assembly := {}

jarName in assembly := "keystone-app-assembly.jar"
