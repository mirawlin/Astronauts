package au.com.mir

import org.apache.spark.sql.SparkSession

object TestUtils {

  def createSparkSession(appName: String = getClass.getName): SparkSession = {
    SparkSession.clearActiveSession()
    val globalSession = SparkSession.getDefaultSession.getOrElse(SparkSession.builder()
      .appName("Global session")
      .master("local[*]")
      .config("avro.mapred.ignore.inputs.without.extension", value = false)
      .config("spark.debug.maxToStringFields", 5000)
      .config("spark.network.timeout", "200s")
      .config("spark.ui.enabled", "false")
      .config("spark.hadoop.fs.s3a.server-side-encryption-algorithm", "AES256")
      .getOrCreate())
    val session = globalSession.newSession()
    session.conf.set("spark.app.name", appName)
    SparkSession.setActiveSession(session)
    session
  }

}
