import _root_.scala._

name := "ml_connector"

version := "1.0"

scalacOptions ++= Seq("-deprecation", "-feature")

organization := "org.simeonot"

version := "1.0.0"

scalaVersion := "2.10.1"

libraryDependencies ++= Seq(
  "org.springframework" % "spring-aspects" % "3.1.3.RELEASE",
  "org.springframework" % "spring-aop" % "3.1.3.RELEASE",
  "org.springframework" % "spring-asm" % "3.1.3.RELEASE",
  "org.springframework" % "spring-beans" % "3.1.3.RELEASE",
  "org.springframework" % "spring-context" % "3.1.3.RELEASE",
  "org.springframework" % "spring-context-support" % "3.1.3.RELEASE",
  "org.springframework" % "spring-core" % "3.1.3.RELEASE",
  "org.springframework" % "spring-expression" % "3.1.3.RELEASE",
  "org.springframework" % "spring-tx" % "3.1.3.RELEASE",
  "com.gigaspaces" % "gs-runtime" % "9.7.0-10496-RELEASE",
  "com.gigaspaces" % "gs-openspaces" % "9.7.0-10496-RELEASE",
  "commons-logging" % "commons-logging" % "1.1.1",
  "com.thoughtworks.xstream" % "xstream" % "1.4.4",
  "com.marklogic" % "marklogic-xcc" % "6.0.3",
  "junit" % "junit" % "4.10" % "test",
  "org.scalatest" %% "scalatest" % "1.9.1" % "test",
  "org.mockito" % "mockito-all" % "1.9.5" % "test"
)

net.virtualvoid.sbt.graph.Plugin.graphSettings

resolvers += "Maven Central Server" at "http://repo1.maven.org/maven2"

resolvers += "MarkLogic Repository" at "http://developer.marklogic.com/maven2"

resolvers += "org.openspaces" at "http://maven-repository.openspaces.org/"

resolvers += "jonaspatch" at "http://maven.ow2.org/maven2"

resolvers += "jboss" at "https://repository.jboss.org/nexus/content/repositories/releases/"

    