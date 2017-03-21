package uwaterloo
import java.nio.file.Files
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._

import scala.io.Source
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming.dstream.InputDStream

import scala.collection.mutable
import scala.collection.immutable.Queue

case class TweetInfo (
                      date: String,
                      subject: String,
                      topic: String,
                      stance: String
                    ) extends Serializable

class FiniteQueue[A](q: Queue[A]) {
  def enqueueFinite[B >: A](elem: B): Queue[B] = {
    var ret = q.enqueue(elem)
    while (ret.size > 10) { ret = ret.dequeue._2 }
    ret
  }
}

/**
 * Hello world!
 *
 */
object App {

  def mappingFunction(key: (String,String), value: Option[(String,String)], state: State[Map[(String,String),(String,String)]])
  : Option[Map[(String,String),(String,String)]] = {
    // Use state.exists(), state.get(), state.update() and state.remove()
    // to manage state, and return the necessary string

    val existingTweets: Map[(String,String),(String,String)] =
      state
        .getOption()
        .getOrElse(Map[(String,String) , (String,String)]())

    val updatedHold: Map[(String,String),(String,String)] =
      value
        .map(x => existingTweets.updated(key,x))
        .getOrElse(existingTweets)

    state.update(updatedHold)
    Option(updatedHold)
  }


  def main(args: Array[String]): Unit ={
    val appName = "StreamingExample"
    val conf = new SparkConf().setAppName(appName).setMaster("local[1]")
    val batchDuration = Seconds(1)
    val ssc = new StreamingContext(conf,batchDuration)
    val checkpointDir = Files.createTempDirectory(appName).toString
    ssc.checkpoint(checkpointDir)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val lines = mutable.Queue[RDD[((String,String),(String,String))]]()
    val dstream = ssc.queueStream(lines)
    val filename = "../data/targetdata_stream.orig"

    try{
      for (line <- Source.fromFile(filename).getLines()) {
        val arr = line.split(",")
        lines += ssc.sparkContext.makeRDD(Seq((arr(1),arr(2))->(arr(0),arr(3))))
      }
    } catch {
      case ex: Exception => println("Bummer, an exception happened: " + ex.toString())
    }

    val spec = StateSpec.function(mappingFunction _)

    dstream.mapWithState(spec).foreachRDD( rdd => {
      if(!rdd.isEmpty()){
        rdd.foreach(state => println(state.get.mkString(",")))
      }
    })

    //dstream.print()
    ssc.start()
    ssc.awaitTerminationOrTimeout(15000)
    ssc.stop()

    //print("Queue length: " + states.length.toString)
  }
}
