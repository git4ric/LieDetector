package uwaterloo
import java.nio.file.Files
import java.util.ArrayList

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
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
                      state: State[ArrayList[Map[(String, String),(String,String)]]])
  : Option[String] = {

    var existingTweets: Map[(String,String),(String,String)] = Map()
    val existingStateList = state
      .getOption()
      .getOrElse(new ArrayList[Map[(String,String) , (String,String)]](3))

    if(existingStateList.size() == 0){
      existingStateList.add(0,Map())
      existingStateList.add(1,Map())
      existingStateList.add(2,Map())
    }

    val category = key._1.trim()
    category match{

      case "Trump" => {
        // Retrieve the stored tweets from state
        existingTweets = existingStateList.get(0)

        // This RDD wil be populated with
        // all FD violations
        val violationDetection: Option[String] =
        value
          .map(x => {
            if(existingTweets.nonEmpty && existingTweets.contains(key)) {
              val v = existingTweets(key)
              if (!v._2.contentEquals(x._2)) {
                key.toString() + " stance changed to" + x._2
              }
              else{""}
            }
            else{
              ""
            }
          })

        var mustStore = false
        val jepcon = new JepConfig()

        // This is needed to mitigate
        // numpy related errors when opening/closing
        // multiple jep instances
        jepcon.addSharedModules("numpy")
        val jep = new Jep(jepcon)
        jep.runScript("../src/classify.py")
        var currentTweet = value.get._3
        val modelToUse = "T"
        val res = jep.invoke("classify", currentTweet, modelToUse)
                      .asInstanceOf[String].replace("']","")
        val classifyResult = res.replace("['","").trim()

        // If the model says store, we store
        // this tweet
        if(classifyResult.equals("yes")){
          mustStore = true
//          println(key._1 + ", " + key._2 + ", stored " + " - " + currentTweet)
        }
        else{
//          println(key._1 + ", " + key._2 + ", NOT stored " + " - " + currentTweet)
        }
        jep.close()

        // For each new key, update the corresponding value in
        // updated hold, if mustStore was set
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

        existingStateList.set(0,updatedHold)

        // Solidify the updated hold as new state
        state.update(existingStateList)

        // Return the violations at this point
        violationDetection
      }

      case "Bernie" => {
        //println("case Bernie matched")
        // Retrieve the stored tweets from state
        existingTweets = existingStateList.get(1)

        // This RDD wil be populated with
        // all FD violations
        val violationDetection: Option[String] =
        value
          .map(x => {
            if(existingTweets.nonEmpty && existingTweets.contains(key)) {
              val v = existingTweets(key)
              if (!v._2.contentEquals(x._2)) {
                key.toString() + " stance changed to" + x._2
              }
              else{""}
            }
            else{
              ""
            }
          })

        var mustStore = false
        val jepcon = new JepConfig()

        // This is needed to mitigate
        // numpy related errors when opening/closing
        // multiple jep instances
        jepcon.addSharedModules("numpy")
        val jep = new Jep(jepcon)
        jep.runScript("../src/classify.py")
        var currentTweet = value.get._3
        val modelToUse = "B"
        val res = jep.invoke("classify", currentTweet, modelToUse)
                      .asInstanceOf[String].replace("']","")
        val classifyResult = res.replace("['","").trim()

        // If the model says store, we store
        // this tweet
        if(classifyResult.equals("yes")){
          mustStore = true
//          println(key._1 + ", " + key._2 + ", stored " + " - " + currentTweet)
        }
        else{
//          println(key._1 + ", " + key._2 + ", NOT stored " + " - " + currentTweet)
        }
        jep.close()

        // For each new key, update the corresponding value in
        // updated hold, if mustStore was set
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

        existingStateList.set(1,updatedHold)

        // Solidify the updated hold as new state
        state.update(existingStateList)

        // Return the violations at this point
        violationDetection
      }

      case default => {
        // Retrieve the stored tweets from state
        existingTweets = existingStateList.get(2)

        // This RDD wil be populated with
        // all FD violations
        val violationDetection: Option[String] =
        value
          .map(x => {
            if(existingTweets.nonEmpty && existingTweets.contains(key)) {
              val v = existingTweets(key)
              if (!v._2.contentEquals(x._2)) {
                key.toString() + " stance changed to" + x._2
              }
              else{""}
            }
            else{
              ""
            }
          })

        // For each new key, update the corresponding value in
        // updated hold, if mustStore was set
        val updatedHold: Map[(String,String),(String,String)] =
        value
          .map(x => existingTweets.updated(key,(x._1,x._2)))
          .getOrElse(existingTweets)

        existingStateList.set(2,updatedHold)
        // Solidify the updated hold as new state
        state.update(existingStateList)

        // Return the violations at this point
        violationDetection
      }
    }
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

    ssc.start()
    ssc.awaitTerminationOrTimeout(60000)
    ssc.stop()
  }
}