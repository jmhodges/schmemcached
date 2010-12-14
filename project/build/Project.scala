import sbt._
import com.twitter.sbt._

class TfeProject(info: ProjectInfo)
  extends StandardProject(info)
  with InlineDependencies
{
  override def managedStyle = ManagedStyle.Maven
  override def disableCrossPaths = true
  override def shouldCheckOutputDirectories = false

  val nettyRepo = "repository.jboss.org" at "http://repository.jboss.org/nexus/content/groups/public/"
  val twitterRepo  = "twitter.com" at "http://maven.twttr.com/"

  val twitterInternalRepo = "twitter.com" at "http://binaries.local.twitter.com/maven"


  // We need to inline the scala compiler here due to a bug in sbt
  // where it doesn't include it even if a library depends on it
  // (ie. util).
  val scalaTools = "org.scala-lang" % "scala-compiler" % "2.8.1" % "compile"
  override def filterScalaJars = false

  val netty = "org.jboss.netty" %  "netty" % "3.2.2.Final"
  inline("com.twitter" % "finagle"  % "1.0.2"          )
  inline("com.twitter" % "util"     % "1.2.5"          )

  override def distZipName = "%s.zip".format(name)

  val mockito  = "org.mockito"             %  "mockito-all" % "1.8.5" % "test" withSources()
  val specs    = "org.scala-tools.testing" %  "specs_2.8.0" % "1.6.5" % "test" withSources()
}
