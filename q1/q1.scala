import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object q1 { 

  def main(args: Array[String]) = {
    // call your code using spark-submit nameofjarfile.jar commandlinearg
    val cmd_arg = args(0) // this is the commandlinearg
    val x = cmd_arg.toInt
    val result = findPopular(getSC(), x)
    result.saveAsTextFile("sparkhw-q1")
  }

  def getSC() = {
    val conf = new SparkConf().setAppName("q1")
    val sc = new SparkContext(conf)
    sc
  }

  def findPopular(sc: SparkContext, x: Int) = {
    val input = sc.textFile("/datasets/flight")
    val parsed = input.map(_.split(","))
    val first_line_skipped = parsed.filter(line => line(0) != "ITIN_ID")
    val kvp = first_line_skipped.flatMap{
      line=>
        val origin = line(3)
        val dest = line(5)
        val count = line(7).toDouble.toInt
        Seq((origin,-count),(dest,count))
    }
    val aggregated = kvp.aggregateByKey(0)(_+_, _+_)
    val result = aggregated.filter(data=>data._2 >= x)
    result
  }
 
}
