package com.huawei.datasight.mllib.rec

import java.time.LocalDate
import java.time.temporal.ChronoUnit.DAYS
import java.time.format.DateTimeFormatter



import scala.collection.mutable.{ArrayBuffer, ListBuffer, WrappedArray}
import scala.util.Random
import scala.util.Try

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object Util {

  // Schemas of the 3 hive tables

  val userVideoSchemaHive = StructType(Array(
    StructField("up_id", StringType, true),
    StructField("video_id", StringType, true),
    StructField("play_date", StringType, true),
    StructField("device_name", StringType, true),
    StructField("video_second_class", StringType, true)
  ))

  val videoTagSchemaHive = StructType(Array(
    StructField("video_id", StringType, true),
    StructField("video_second_class", StringType, true)
  ))

  val userProfileSchemaHive = StructType(Array(
    StructField("id", StringType, true),
    StructField("reside_city", StringType, true),
    StructField("phone_type", StringType, true)
  ))

   /**
    * generate random videoTag hive table
    * @param nVideo: number of videos
    * @param nTag: number of tags
    * @param hivecontext
    */
  def videotag_gen_hive (nVideo: Int, nTag: Int, hivecontext: HiveContext, tableName: String): Unit = {

    var videoTagRows = new ListBuffer[Row]()

    for(i <- 0 to nVideo - 1) {
      var tags = new String
      for (j <- 1 to nTag - 1) {
        if(Random.nextBoolean() == true){
          tags += s"$j,"
        }
      }
      // Each video has at least one tag
      tags += s"0"
      videoTagRows += Row(i.toString, tags)
    }
    val videoTag = hivecontext.createDataFrame(hivecontext.sparkContext
      .parallelize(videoTagRows, 8), videoTagSchemaHive)
    videoTag.write.mode("overwrite").saveAsTable(tableName)
  }


  /**
    * generate random user_watched_video table.
    * We assume each user watched 5 videos during 2016/12/1 to 2016/12/31
    * @param nUser
    * @param nVideo
    * @param hivecontext
    */
  def uservideo_gen_Hive (nTag: Int, nUser: Int, nVideo: Int, hivecontext: HiveContext, tableName: String): Unit = {
    val userVideoRows = new ListBuffer[Row]()
    for(i <- 0 to nUser - 1) {
      for(j <- 0 to 4) {
        val watched = Random.nextInt(nVideo).toString
        val timestamp = randomDate(LocalDate.of(2016, 12, 1), LocalDate.of(2016, 12, 31)).toString
        val device = Random.nextInt(5).toString
        var tags = new String
        // Each video has at least one tag
        tags += s"0|"
        for (j <- 1 to nTag - 1) {
          if(Random.nextBoolean() == true){
            tags += s"$j|"
          }
        }
        userVideoRows += Row(i.toString, watched, timestamp, device, tags)
      }
    }
    val userVideo =  hivecontext.createDataFrame(hivecontext.sparkContext
      .parallelize(userVideoRows, 8), userVideoSchemaHive)
    userVideo.write.mode("overwrite").saveAsTable(tableName)
  }

  /**
    * generate random user profile table
    * @param nUser
    * @param hivecontext
    */

  def userprofile_gen_Hive(nUser: Int, hivecontext: HiveContext, tableName: String): Unit = {
    val userCityRows = new ListBuffer[Row]()

    for(i <- 0 to nUser - 1) {
      val rnd = Random.nextInt(3)
//      val city: String ="Beijing"
      val city: String = {
        rnd match {
          case 0 => "Beijing"
          case 1 => "Tianjin"
          case _ => "Shanghai"
        }
      }

      val phone: String = {
        rnd match {
          case 0 => "Mate1"
          case 1 => "Mate2"
          case _ => "Mate3"
        }
      }

      userCityRows += Row(i.toString, city, phone)
    }
    val userProfile = hivecontext.createDataFrame(hivecontext.sparkContext
      .parallelize(userCityRows, 8), userProfileSchemaHive)
    userProfile.write.mode("overwrite").saveAsTable(tableName)
  }

  case class hive_tables(userProfile: DataFrame, userVideo: DataFrame, videoTag: DataFrame)

  /** Read the hive tables and output corresponding dataframes for computing the distributions and
    * recommendation
    *
    * userProfile table(hive): id: string, reside_city: string, phone_type: string
    * example: 1122233, Beijing, Mate1
    *
    * userVideo table(hive): up_id: string, video_id: string, play_date: string
    * example: 1122233. 5746, 2017-07-18
    *
    * videoTag table(hive): video_id: string, second_class: string
    * example: 5746, action, comedy
    *
    * @param nTag: total number of video types or more
    * @param userCityTable:
    * @param userVideoTable
    * @param videoTagTable
    * @param hivecontext
    * @return
    * Schema of userProfile : up_id: string, reside_city:string, phone_type: string
    * Schema of user_watched_video: up_id: string, video_id: string, play_date: String
    * schema of video_tag: video_id: string, tags: Array[byte]. Each item in tags is 1 if the video contains that tag;
    * otherwise 0.
    */

  def read_hive_table_new(nTag: Int, userCityTable: String, userVideoTable: String, videoTagTable: String,
                      hivecontext: HiveContext): hive_tables = {

    // Need to add "phone_type" column to userPorfile table in Hbase
    // val userProfileHive = hivecontext.sql(s"SELECT id, reside_city, phone_type FROM $userCityTable")

    val userProfileHive = hivecontext.sql(s"SELECT id, reside_city FROM $userCityTable")


    // filter the videos without second_class information
    // another option is to give those default values
    val videoTagHive = hivecontext.sql(s"SELECT video_id, video_second_class FROM $videoTagTable").
      filter(col("video_second_class").isNotNull).filter(col("video_second_class").notEqual("")).
      filter(col("video_id").isNotNull).filter(col("video_id").notEqual(""))

    val videoTagSchema = StructType(StructField("video_id", StringType, false)::
      StructField("tags", ArrayType(ByteType, false), false)::Nil)

    // get all existing types
    var tags = Set[String]()
    videoTagHive.flatMap{
      row => Try(row(1).asInstanceOf[String].split(',')).getOrElse(null)
    }.filter(_!=null).filter(_!="").distinct().collect().foreach(tag => tags += tag)

    // assign each type an integer index
    val tagToIndex =  tags.toList.sortBy(v => v).zipWithIndex.map(pair => pair._1 -> pair._2).toMap

    //convert the video_tag table to required schema as required
    val videoTag = videoTagHive.
      map{
      row => row(1).asInstanceOf[String] match {
          case "" => null
          case _ => {
            val tagByte = Array.fill[Byte](nTag)(0)
            row(1).asInstanceOf[String].split(',').toSeq.foreach(x => tagByte(tagToIndex(x)) = 1)
            Row(row(0).asInstanceOf[String], tagByte)
          }
      }
    }.filter(_!=null)

    // remove "-" in play_data
    val dateParser = udf{
      (play_date: String) => play_date.replace("-", "")
    }

    val userVideoHive = hivecontext.sql(s"SELECT up_id, video_id, play_date FROM $userVideoTable").
      filter(col("play_date").isNotNull).filter(col("play_date")!== "").
      filter(col("up_id").isNotNull).filter(col("up_id")!== "").
      filter(col("video_id").isNotNull).filter(col("video_id")!== "").
      select(col("up_id"), col("video_id"), dateParser(col("play_date")).as("play_date"))

    hive_tables(
      userProfileHive,
      userVideoHive,
      hivecontext.createDataFrame(videoTag, videoTagSchema)
    )

  }

  // generate a random date between "from" and "to"
  def randomDate(from: LocalDate, to: LocalDate): LocalDate = {
    val diff = DAYS.between(from, to)
    from.plusDays(Random.nextInt(diff.toInt))
  }

  def getnDaysAgo(from: LocalDate, ndays: Int): LocalDate ={
    from.minusDays(ndays)
  }

  /**
    * Individually sample k numbers based the input distribuition
    * @param dist: the distribution for each index.
    *            each entry of the "dist" is of the format (index, probability), where the sum of the probabiliteis have
    *            to be 1
    * @return Array[(index, probability)]
    */

  def sampleNew(dist: Array[Double], k: Int): Array[(Int, Double)] = {

    val rnd = new ArrayBuffer[Double]
    for(i <- 0 to k-1){
      rnd += scala.util.Random.nextDouble
    }

    rnd.sortBy(v => v)
    val sampled =  new ArrayBuffer[(Int, Double)]
    var accum = 0.0
    var rnd_found = 0
    var i = 0
    while(i < dist.size && rnd_found < k) {
      accum += dist(i)
      for(j <- rnd_found to k-1) {

        if (accum > rnd(j)) {
          sampled.+=((i, dist(i)))
          rnd_found += 1
        }
      }
      i += 1
    }
    sampled.toArray.distinct
  }

  /** Compute KL divergence
   * https://en.wikipedia.org/wiki/Kullback-Leibler_divergence
    */
  def computeKL(p: Array[Double], q: Array[Double]): Double = {
    assert(p.length == q.length)

    val sigma = 0.0001

    var n_p_zero = 0
    p.foreach(v => if(v == 0) n_p_zero +=1)

    var n_q_zero = 0
    q.foreach(v => if(v == 0) n_q_zero +=1)

    val n_p_non_zero = p.size - n_p_zero
    val n_q_non_zero = q.size - n_q_zero

    val p_noZero = p.map(v => if( v == 0 ) sigma else v - n_p_zero * sigma / n_p_non_zero )
    val q_noZero = q.map(v => if( v == 0 ) sigma else v - n_q_zero * sigma / n_q_non_zero )

    val pq = p_noZero.zip(q_noZero).map(t => t._1 * math.log10(t._1/t._2)).foldLeft(0.0)(_ + _)

    pq
  }

  /**
    * Compute the KL- distance between each row of the first dataframe and the corresponding row of the second
    * dataframe
    * @param old
    * @param b
    * @return the average distance of the two dataframe
    */

  def computeDistributionDistance(old: DataFrame, b: DataFrame): String = {
    val compkl = udf((a_probs: WrappedArray[Double], b_probs: WrappedArray[Double]) =>
      computeKL(a_probs.toArray, b_probs.toArray))

    val result = old.join(b, "up_id").
      select(col("up_id"), old("prob").as("prob1"), b("prob").as("prob2")).
      select(col("up_id"), compkl(col("prob1"), col("prob2")).as("KLDistance")).
      agg(avg("KLDistance").as("avg"),
        min("KLDistance").as("min"),
        max("KLDistance").as("max"),
        avg(col("KLDistance") * col("KLDistance")).as("square")).
      select(col("min"), col("max"), col("avg"), (col("square")- col("avg") * col("avg")).as("var")).
      collect()(0).mkString(",")

    result
  }

  /**
    * Compute the accuracy of the recommendation results.
    * We use the videos watched within the lasest "nDays" days as the testing set; the others as the training set.
    * Let R be the set of videos to recommend based on the training set, and W be the videos in the testing set.
    * We compute the accuracy as | R \intersect W | / |R|
    */


  def computeAccuracy(label: String, kTags: Int, nVideo: Int,
                      userProfile: DataFrame, user_watched_video: DataFrame,
                      videoTag: DataFrame, toDate: String ,nDays: Int): Double = {

    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val dt = LocalDate.parse(toDate,formatter)

    val train_set = user_watched_video.filter(col("play_date").leq(toDate))
    val test_set = user_watched_video.filter(col("play_date").geq(toDate)).
      select(col("up_id"), col("video_id"))

    if(label == "city" || label == "phone") {
      val result = RecommBasedonProbDistribution.recommendForLabelNew(label, kTags, nVideo, userProfile, train_set, videoTag, toDate, nDays)
      val hit = result.select(col("up_id"), col("video_id")).intersect(test_set).count()
      val total_rec = result.count

      hit.toDouble / total_rec.toDouble

    } else {
      val result = RecommBasedonProbDistribution.recommendForUserNew(kTags, nVideo, train_set, videoTag, toDate, nDays)
      val hit = result.select(col("up_id"), col("video_id")).intersect(test_set).count()
      val total_rec = result.count

      hit.toDouble / total_rec.toDouble
    }
  }
}
