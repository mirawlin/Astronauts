package au.com.mir

import au.com.mir.TestUtils.createSparkSession
import org.apache.spark.sql.SparkSession
import org.scalatest.{Matchers, WordSpec}

class SparkJobTest extends WordSpec with Matchers {

  "SparkJobTest" should {

    "find gender diveristy in Astronaut data" in {
      implicit val session: SparkSession = createSparkSession()
      import session.implicits._

      //GIVEN
      val astronaut = Seq(
        ("Adam", "Male"),
        ("Andrew", "Male"),
        ("Judy", "Female"))
        .toDF("Name", "Gender")


      val expectedResult = 33.0

      //WHEN
      val result = SparkJob.findGenderDiversity(astronaut, "Female")

      //THEN
      result shouldEqual expectedResult
    }
  }

  "findBirthPlace" should {
    "find the total number of astronauts born in a specified location" in {
      implicit val session: SparkSession = createSparkSession()
      import session.implicits._

      //GIVEN
      val astronaut = Seq(
        ("Adam", "Australia"),
        ("Andrew", "Philadelphia"),
        ("Judy", "Philadelphia, PA"))
        .toDF("Name", "Birth Place")

      val expectedResult = 2

      //WHEN
      val result = SparkJob.findBirthPlace(astronaut, "Philadelphia")

      //THEN
      result shouldEqual expectedResult

    }
  }
}
