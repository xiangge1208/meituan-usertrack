import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


object SQLContextDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
    val sc = SparkContext.getOrCreate(conf)

    (0 to 10).foreach(i => {
      // -XX:PermSize=128M -XX:MaxPermSize=128M
      new HiveContext(sc)
      println(s"index:${i}")
    })

    Thread.sleep(100000)
  }
}
