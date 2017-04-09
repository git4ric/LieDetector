package uwaterloo
import java.nio.file.Files
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf}
import org.apache.spark.streaming._

import scala.io.Source
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.collection.mutable
import jep.Jep
import jep.JepConfig

/**
  * FD violation detection
  * @author Ripul
 */
object App {

  def mappingFunction(key: (String,String), value: Option[(String,String,String)],
                      state: State[Map[(String, String),(String,String)]])
  //: Option[Map[(String,String),(String,String)]] = {
  : Option[String] = {

    // Retrieve the stored tweets from state
    val existingTweets: Map[(String,String),(String,String)] =
      state
        .getOption()
        .getOrElse(Map[(String,String) , (String,String)]())

    // This RDD wil be populated with
    // all FD violations
    val violationDetection: Option[String] =
    value
      .map(x => {
        if(existingTweets.contains(key)) {
          val v = existingTweets(key)
          if (!v._2.contentEquals(x._3)) {
            key.toString() + " stance changed"
          }
          else{""}
        }
        else{
          ""
        }
      })

    var mustStore = false
    val jepcon = new JepConfig()
    jepcon.addSharedModules("numpy")
    val jep = new Jep(jepcon)
    jep.runScript("../src/classify.py")
    var currentTweet = value.get._3
    val res = jep.invoke("classify", currentTweet).asInstanceOf[String].replace("']","")
    val classifyResult = res.replace("['","")

    // If the model says store, we store
    // this tweet
    if(classifyResult.equals("yes")){
      mustStore = true
      println(key._1 + ", " + key._2 + ", stored " + " - " + currentTweet)
    }
    else{
      println(key._1 + ", " + key._2 + ", NOT stored " + " - " + currentTweet)
    }
    jep.close()

    // For each new key, update the corresponding value in
    // updated hold
    val updatedHold: Map[(String,String),(String,String)] =
      value
        .map(x =>
          if(mustStore) {
            existingTweets.updated(key,(x._1,x._2))
          }
          else{
            existingTweets
          })
        .getOrElse(existingTweets)

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

    val lines = mutable.Queue[RDD[((String,String),(String,String,String))]]()
    val dstream = ssc.queueStream(lines)
    val filename = "../data/targetdata_stream.orig"

    try{
      for (line <- Source.fromFile(filename).getLines()) {
        val arr = line.split(",")
        lines += ssc.sparkContext.makeRDD(Seq((arr(1),arr(2))->(arr(0),arr(4),arr(3))))
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
    ssc.awaitTerminationOrTimeout(60000)
    ssc.stop()
  }
}
