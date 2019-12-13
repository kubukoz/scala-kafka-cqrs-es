inThisBuild(
  List(
    organization := "com.kubukoz",
    homepage := Some(url("https://github.com/kubukoz/kafka-demo")),
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers := List(
      Developer(
        "kubukoz",
        "Jakub Kozłowski",
        "kubukoz@gmail.com",
        url("https://kubukoz.com")
      )
    )
  )
)

val compilerPlugins = List(
  compilerPlugin("org.typelevel" % "kind-projector"      % "0.11.0" cross CrossVersion.full),
  compilerPlugin("com.olegpy"    %% "better-monadic-for" % "0.3.1")
)

val commonSettings = Seq(
  scalaVersion := "2.13.1",
  scalacOptions ~= (_.filterNot(_ == "-Xfatal-warnings") ++ Seq(
    "-Yimports:" ++ List(
      "scala",
      "scala.Predef",
      "cats",
      "cats.implicits",
      "cats.effect",
      "cats.effect.implicits",
      "cats.effect.concurrent"
    ).mkString(",")
  )),
  fork in Test := true,
  name := "kafka-demo",
  updateOptions := updateOptions.value.withGigahorse(false),
  libraryDependencies ++= Seq(
    "dev.profunktor" %% "console4cats"         % "0.8.0",
    "ch.qos.logback" % "logback-classic"       % "1.2.3",
    "io.circe"       %% "circe-generic-extras" % "0.12.2",
    "com.olegpy"     %% "meow-mtl-core"        % "0.4.0",
    "com.olegpy"     %% "meow-mtl-effects"     % "0.4.0",
    "com.ovoenergy"  %% "fs2-kafka"            % "0.20.2",
    "org.scalatest"  %% "scalatest"            % "3.1.0" % Test
  ) ++ compilerPlugins
)

def app(name: String) = Project(name, file(s"applications/$name")).settings(commonSettings)

val stock   = app("stock")
val reports = app("reports")

val root =
  project.in(file(".")).settings(commonSettings).settings(skip in publish := true).aggregate(stock, reports)
