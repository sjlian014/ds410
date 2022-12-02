import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object q3 { 

  def main(args: Array[String]) = {
    val x = args(0).toInt
    val y = args(1).toInt
    val result = findPopularAndHubs(getSC(), x, y)
    result.saveAsTextFile("sparkhw-q3")
  }

  def getSC() = {
    val conf = new SparkConf().setAppName("q2")
    val sc = new SparkContext(conf)
    sc
  }
 
  def findPopular(sc: SparkContext, x: Int) = {
    val input = sc.textFile("/datasets/flight")
    val parsed = input.map(_.split(",")).filter(line => line(0) != "ITIN_ID")
    val kvp = parsed.flatMap{
      line=>
        val origin = line(3)
        val dest = line(5)
        val count = line(7).toDouble.toInt
        Seq((origin,-count),(dest,count))
    }
    val aggregated = kvp.aggregateByKey((0,0))((acc, crr) => if(crr >= 0) (acc._1 + crr, acc._2) else (acc._1, acc._2 + (-crr)), 
      (n1, n2) => (n1._1+n2._1,n1._2+n2._2))
    val popular = aggregated.filter(data=>(data._2._1 - data._2._2)>= x)
    popular
  }

  def findHubs(sc: SparkContext, y: Int) = {
    val input = sc.textFile("/datasets/flight")
    val parsed = input.map(_.split(",")).filter(line => line(0) != "ITIN_ID")
    val kvp = parsed.map{
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
 
  def findPopularAndHubs(sc: SparkContext, x: Int, y: Int) = {
    val popular = findPopular(sc, x)
    val hubs = findHubs(sc, y)
    
    val result = popular.cogroup(hubs).filter{ case (k, (v1, v2)) => v1.nonEmpty && v2.nonEmpty }
                          .map{ case (k, (v1, v2)) => 
                                        val popularityStats = v1.reduce((x,_)=>x)
                                        val ttlInc = popularityStats._1
                                        val ttlOut = popularityStats._2
                                        val hubStats = v2.reduce((x,_)=>x)
                                        (k, (hubStats, ttlInc, ttlOut))}
    result
  }
}

