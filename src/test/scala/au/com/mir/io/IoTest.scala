package au.com.mir.io

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import au.com.mir.TestUtils.createSparkSession
import org.scalatest.{Matchers, WordSpec}

class IoTest extends WordSpec with Matchers {

  "read Csv" should {
    "read a csv file" in {
      //GIVEN
      implicit val session: SparkSession = createSparkSession()

      val file = "src/test/resources/Astronauts.csv"

      //WHEN
      val result = Io.readCsv(file)
      result.show(10)

      //THEN
      result.count() shouldEqual 357

    }
  }

  "write Csv" should {
    "write a dataframe to a csv file" in {
      //GIVEN
      implicit val session: SparkSession = createSparkSession()
      import session.implicits._

      val path = "target/writeCsv"
      FileUtils.deleteDirectory(new File(path))

      val df = Seq(
        ("test1", 1, 900),
        ("test2", 2, 400))
        .toDF("name", "col2", "col3")

      //WHEN
      Io.writeCsv(df, path)

      //THEN
      Files.exists(Paths.get(path)) shouldBe true
      Files.exists(Paths.get(path + "/_SUCCESS")) shouldBe true
      session.read.csv(path).count() should be(2)
    }
  }

}
