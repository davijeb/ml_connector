logLevel := Level.Warn

addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.5.2")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.7.4")

libraryDependencies += "aopalliance" % "aopalliance" % "1.0"

libraryDependencies += "org.jboss.jbossts" % "jbossjta" % "4.16.5.Final"

resolvers += Classpaths.typesafeSnapshots

resolvers += Classpaths.typesafeResolver

resolvers += "scct-github-repository" at "http://mtkopone.github.com/scct/maven-repo"

