package uwaterloo
import org.apache.spark.{SparkConf,SparkContext}

/**
 * Hello world!
 *
 */
object App {
  val conf = new SparkConf().setAppName("FD Violation detection").setMaster("local[1]")
  val sc = new SparkContext(conf)
}
