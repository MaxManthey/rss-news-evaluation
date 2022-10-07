ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "rss-news-evaluation"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.0",
  "org.apache.spark" %% "spark-sql" % "3.3.0",
  "org.scalatest" %% "scalatest" % "3.2.12" % Test,
  "org.scalamock" %% "scalamock" % "5.2.0" % Test,
  "com.h2database" % "h2" % "1.4.196",
  "io.spray" %%  "spray-json" % "1.3.6",
  "de.l3s.boilerpipe" % "boilerpipe" % "1.1.0",
  "org.maxmanthey" %% "rss-news-persistence" % "0.1.1-SNAPSHOT"
)