import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object q2 { 

  def main(args: Array[String]) = {
    val cmd_arg = args(0)
    val y = cmd_arg.toInt
    val result = findHubs(getSC(), y)
    result.saveAsTextFile("sparkhw-q2")
  }

  def getSC() = {
    val conf = new SparkConf().setAppName("q2")
    val sc = new SparkContext(conf)
    sc
  }

  def findHubs(sc: SparkContext, y: Int) = {
    val input = sc.textFile("/datasets/flight")
    val parsed = input.map(_.split(","))
    val first_line_skipped = parsed.filter(line => line(0) != "ITIN_ID")
    val kvp = first_line_skipped.map{
      line=>
        val origin = line(3)
        val dest_state = line(6)
        (origin, dest_state)
    }
    val kvpd = kvp.distinct()
    val aggregated = kvpd.aggregateByKey(0)((acc, curr) => acc + 1, _+_) 
    val result = aggregated.filter(data=>data._2 >= y)
    result
  }
 
}
