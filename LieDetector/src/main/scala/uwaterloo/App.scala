package uwaterloo
import java.nio.file.Files
import java.nio.charset.Charset
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._

import scala.io.Source
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.collection.mutable
import scala.collection.immutable.Queue
import jep.Jep
import jep.JepConfig

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
 * Naive FD violation detection
  * @author Ripul
 */
object App {

  def mappingFunction(key: (String,String), value: Option[(String,String)], state: State[Map[(String,String),(String,String)]])
  //: Option[Map[(String,String),(String,String)]] = {
  : Option[String] = {

    // Retrieve the stored tweets from state
    val existingTweets: Map[(String,String),(String,String)] =
      state
        .getOption()
        .getOrElse(Map[(String,String) , (String,String)]())

    var mustStore = false
    val jepcon = new JepConfig()
    jepcon.addSharedModules("numpy")
    val jep = new Jep(jepcon)
    jep.runScript("../src/classify.py")
    var s = "John McCain idiot"
    val res = jep.invoke("classify", s).asInstanceOf[String].replace("']","")
    val classifyResult = res.replace("['","")

    // If the model says store, we store
    // this tweet
    if(classifyResult.equals("yes")){
      mustStore = true
    }
    jep.close()

    // For each new key, update the corresponding value in
    // updated hold
    val updatedHold: Map[(String,String),(String,String)] =
      value
        .map(x => if(mustStore) {
          existingTweets.updated(key,x)
        }
        else{
          existingTweets
        })
        .getOrElse(existingTweets)

    // This RDD wil be populated with
    // all FD violations
    val violationDetection: Option[String] =
      value
        .map(x => {
          if(existingTweets.contains(key)) {
            val v = existingTweets.get(key).get
            if (!v._2.contentEquals(x._2)) {
              key.toString() + " stance changed"
            }
            else{""}
          }
          else{
            ""
          }
        })

    // Solidify the updated hold as new state
    state.update(updatedHold)

    // Return the violations at this point
    violationDetection
  }

  def main(args: Array[String]): Unit ={
    val appName = "Naive FD detection"
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
        rdd.foreach(state => println(state.get))
      }
    })

    //dstream.print()
    ssc.start()
    ssc.awaitTerminationOrTimeout(15000)
    ssc.stop()

    //print("Queue length: " + states.length.toString)
  }
}
