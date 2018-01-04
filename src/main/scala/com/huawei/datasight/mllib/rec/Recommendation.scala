package com.huawei.datasight.mllib.rec

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import scala.collection.mutable.{ArrayBuffer, WrappedArray}

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._


object RecommBasedonProbDistribution {

  /** Compute each video's click-count within the time-window of "fromDate" to "toDate"
    * and then compute each video's popularity as follows:
    * Let c_i be video_i's click count within the time-window and n be the total number of videos
    * pop_i = c_i / sum_{i =j to n} c_j
    * 0 <= pop_i <=1
    *
    * @param user_watched_video: Schema of user_watched_video:
    *   up_id: String, video_id: String, play_date: String
    * @param fromDate: begin of the time window, e.g., 2001-01-02
    * @param toDate: end of the time window, e.g., 2001-01-02
    * @return Dataframe: Schema: video_id: String, pop: Double
    */

  def computeVideoPoularity(user_watched_video: DataFrame, fromDate: String, toDate: String): DataFrame ={

    //get each video's click-count
    val video_count_table = user_watched_video.
      filter(col("play_date").geq(fromDate)).filter(col("play_date").leq(toDate)).
      groupBy(col("video_id")).
      count()

    //get the total click
    val total_click = video_count_table.agg(sum("count").as("total")).select("total").
      collect()(0).getLong(0)

    //calaulate the popularity
    video_count_table.select(col("video_id"), (col("count")/total_click).as("pop"))
  }

   /**
    * Obtain the tag probability distribution for a given label ("city" or "phone type") from user_profile table,
    * user_watrched_video table and video_tag table
    * The schema of tag distribution table is : label: String, tag_prob: Array[Double]
    *
    * @param label_id: for which label to get the tag distribution: "city", and "phone_type"
    * @param user_profile: Schema of user_profile : up_id: String, reside_city: String
    * @param user_watched_video: Schema of user_watched_video: up_id: String,
    *   video_id: String, play_date: String
    * @param videoTag: video_id: String tag: Array[Byte]
    * @param fromDate: obtain the tag distribution for the period between the "fromDate" to "toDate"
    * @param toDate: obtain the tag distribution for the period between the "fromDate" to "toDate"
    * @return label_tag_dist: DataFrame. Schema: Label_ID: String, prob: Array[Double]
    */
  def computeLabelTagDist(label_id: String,
                            user_profile: DataFrame, user_watched_video: DataFrame, videoTag: DataFrame,
                            fromDate: String, toDate: String): DataFrame = {

    val nTags = videoTag.first().getSeq(1).length

    val label = if (label_id == "city") "reside_city" else "phone_type"

    /** ComputeProb is a UDAF for computing the tag-distribution defined in Udaf
      * The input column is the tag array of byte (0/1). the output column is the probability array of double.
      * Each tag i's probability is computed as count_i/total_count, where count_i is the number of tag_i within the
      * group and total_count is the total number of tags within the group.
      *
      * example:
      * Input:
      * user1, video2, (0, 1, 1)
      * user1, video3, (1, 0, 1)
      * user1, video5, (0, 0, 1)
      * output:
      * user1, (0.2, 0.2, 0.6)
      */

    val cp = new ComputeProb(nTags)

    user_watched_video.sqlContext.udf.register("cp", cp)

    /**
      * 1. filter user_watched_video Table
      *       result table: (up_id, video_id)
      * 2. join(user_watched_video, user_profile) by "user" and select (city, video_id)
      *       result table: "city_video" (city, video_id)
      * 4. join (city_video, videotag) and select (city, tags)
      *     result: "city_tag" table: (city, tags)
      * 5. group "city_tag" table by "city" and compute the probability for each tag by the UDAF ComputeProb
      */

    user_watched_video
      .filter(col("play_date").geq(fromDate)).filter(col("play_date").leq(toDate))
      .select("up_id", "video_id")
      .join(user_profile, user_watched_video("up_id") === user_profile("id"))
      .select(label, "video_id")
      .join(videoTag, "video_id")
      .select(label, "tags")
      .groupBy(label)
      .agg(cp(col("tags")).as("prob"))

  }

  /** Similar to computeLabelTagTableNew, compute the tag-prob distribution for each user
    *
    * @param user_watched_video
    * @param videoTag
    * @param fromDate
    * @param toDate
    * @return
    */

  def computeUserTagDist(user_watched_video: DataFrame, videoTag: DataFrame,
                               fromDate: String, toDate: String): DataFrame = {

    val nTags = videoTag.first().getSeq(1).length

    val cp = new ComputeProb(nTags)

    user_watched_video.sqlContext.udf.register("cp", cp)

    user_watched_video
      .filter(col("play_date").geq(fromDate)).filter(col("play_date").leq(toDate))
      .select("up_id", "video_id")
      .join(videoTag, "video_id")
      .select("up_id","tags")
      .groupBy("up_id")
      .agg(cp(col("tags")).as("prob"))
  }

  /**
    * Recommend to each user "nVideos" videos based on "kTags" tags
    * @param label_id: "city" or "phone"
    * @param kTags
    * @param nVideo
    * @param videoTag : schema (video_id; tags: Array[Byte], e.g., [010101]; popularity: Double)
    * @param label_tag_dist: label; tag_prob: Array[Double]
    * @return Schema: (up_id: String, video_id: String, probability_sum: Double, popularity: Double, rank: Int)
    *         where probability_sum is the sum of the probabilities of tags contained in the video, and the popularity
    *         is the video's popularity. The videos is sort descending according to prob_sum, then pop.
    */

  def recommendForLabel (label_id: String, kTags: Int, nVideo: Int,
                         user_profile: DataFrame, video_pop: DataFrame, videoTag: DataFrame, label_tag_dist: DataFrame,
                         user_watched_video: DataFrame
                         ): DataFrame = {

    val label = if (label_id == "city") "reside_city" else "phone_type"

    // sample "kTags" tags according to the tag-prob distribution
    // Input: Array[Double], output: Array[Int, Double], each item is a pair of (tag_index, tag_prob)
    val sample_tag = udf((probs: WrappedArray[Double]) => Util.sampleNew(probs.toArray, kTags))

    // get the tag_index from the pair (tag_index, tag_prob)
    val get_tag = udf((tags: Row) => tags.getInt(0))
    // get the tag_prob from the pair (tag_index, tag_prob)
    val get_prob = udf((tags: Row) => tags.getDouble(1))


     /** Collect the tags sampled by the udf "sample_tag"
      * Input: tag array Array[Byte], e.g., [010101]
      * Output: Array[Int], e.g. [1, 3, 5]
      */
    val collectTags = udf((tags: WrappedArray[Byte]) =>
    {
      val tag_idx = new ArrayBuffer[Int]
      tags.zipWithIndex.foreach(v =>
        if(v._1 > 0) tag_idx += v._2
      )
      tag_idx.toArray
    })

     /** sample "kTags" for each label
      * Suppose the input is one row ("0123", (0.5, 0.2, 0.3)), where ((0.5, 0.2, 0.3)) is the tag probability
      * distribution.
      * After applying "sample_tag", suppose tag_1 and tag_3 are sampled, then the result dataframe is
      * ("0123", ((1, 0.5), (3, 0.3)))
      * After applying "explode", "get_tag", "get_prob", the final dataframe is two rows
      * ("0123", 1, 0.5)
      * ("0123", 3, 0.3)
      */
    val label_tag_sampled = label_tag_dist.
      select(col(label), sample_tag(col("prob")).as("sampled")).
      withColumn("tag_prob", explode(col("sampled"))).
      select(col(label), col("tag_prob")).
      withColumn("tag", get_tag(col("tag_prob"))).
      withColumn("prob", get_prob(col("tag_prob"))).
      select(col(label), col("tag"), col("prob"))

     /** Convert the video_tag dataframe to a dataframe with the same schema with that of the above dataframe
      * e.g. one row in the original video_tag dataframe
       * ("15", (0, 1, 1)) video "15" contains tags 1 and 3
       * The output will be two rows
       * ("15", 1)
       * ("15", 3)
      */
    val videotag_exploded = videoTag.
      select(col("video_id"), col("tags")).
      select(col("video_id"), collectTags(col("tags")).as("expended")).
      withColumn("tag", explode(col("expended"))).
      select(col("video_id"), col("tag"))

    /**
      * Join the above two tables on the column "tag",
      * calculate the probability for each "lable" and "vidoe_pair",
      * and join the table with "video_pop" to get the popularity of each video
      */

    val video_candidate = label_tag_sampled.
      join(videotag_exploded, label_tag_sampled("tag") === videotag_exploded("tag")).
      groupBy(col(label), col("video_id")).
      agg(sum(col("prob")).as("prob_sum")).
      join(video_pop, "video_id").
      join(user_profile, label).drop(label)

    // Rank the videos based on prob_sum and the popularity
    val w = Window.partitionBy(col("id")).orderBy(desc("prob_sum"), desc("pop"))

    // the videos have been watched
    val watched = user_watched_video.select(col("up_id").as("user"), col("video_id").as("video"))

    // filter the videos a user has watched
    video_candidate.join(watched, watched("user") === video_candidate("id")
      && watched("video") === video_candidate("video_id"), "left")
      .filter(col("user").isNull)
      .withColumn("rank", rank().over(w)).
      filter(col("rank").leq(nVideo)).
      select(col("id").as("up_id"), col("video_id"), col("prob_sum"), col("pop"))

  }

  /**Similar to recommendLabel
    *
    * @param kTags
    * @param nVideo
    * @param videoTag
    * @param user_tag_dist
    * @return
    */

  def recommendForUser(kTags: Int, nVideo: Int,
                        video_pop: DataFrame, videoTag: DataFrame, user_tag_dist: DataFrame,
                       user_watched_video: DataFrame): DataFrame = {

    val sample_tag = udf((probs: WrappedArray[Double]) => Util.sampleNew(probs.toArray, kTags))
    val get_tag = udf((tags: Row) => tags.getInt(0))
    val get_prob = udf((tags: Row) => tags.getDouble(1))
    val collectTags = udf((tags: WrappedArray[Byte]) =>
    {
      val tag_idx = new ArrayBuffer[Int]
      tags.zipWithIndex.foreach(v =>
        if(v._1 > 0) tag_idx += v._2
      )
      tag_idx.toArray
    })

    val user_tag_sampled =
      user_tag_dist.
        select(col("up_id"), sample_tag(col("prob")).as("sampled")).
        withColumn("tag_prob", explode(col("sampled"))).
        select(col("up_id"), col("tag_prob")).
        withColumn("tag", get_tag(col("tag_prob"))).
        withColumn("prob", get_prob(col("tag_prob"))).
        select(col("up_id"), col("tag"), col("prob"))

    val videotag_exploded =
      videoTag.join(video_pop, "video_id").
        select(col("video_id"), collectTags(col("tags")).as("expended")).
        withColumn("tag", explode(col("expended"))).
        select(col("video_id"), col("tag"))

    val w = Window.partitionBy(col("up_id")).orderBy(desc("prob_sum"), desc("pop"))

    val video_candidate = user_tag_sampled.
      join(videotag_exploded, user_tag_sampled("tag") === videotag_exploded("tag")).
      groupBy(col("up_id"), col("video_id")).
      agg(sum(col("prob")).as("prob_sum")).
      join(video_pop, "video_id")

    val watched = user_watched_video.select(col("up_id").as("user"), col("video_id").as("video"))

    video_candidate.join(watched, watched("user") === video_candidate("up_id")
      && watched("video") === video_candidate("video_id"), "left")
      .filter(col("user").isNull)
        .withColumn("rank", rank().over(w)).
      filter(col("rank").leq(nVideo)).
      select(col("up_id"), col("video_id"), col("prob_sum"), col("pop"))

  }

  /** The final recommended videos is from the following two results:
    * recommend_list_new: Recommendation based on last "windowSize" days's watch history
    * if the size of recommend_list_new is less than the number of video required, then we add the following videos
    * recommend_list_old: Recommendation based on the whole watch history recommend_list_new
    * the videos in recommend_list_new will be ranked lower than the videos in
    * Merge the above two recommendation list and choose the top-k video
    * @param kTags
    * @param nVideo
    * @param user_watched_video
    * @param videoTag
    * @param windowSize
    */

  def recommendForUserNew(kTags: Int, nVideo: Int,
                      user_watched_video: DataFrame, videoTag: DataFrame,
                          toDate: String, windowSize: Int): DataFrame = {

    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val dt = LocalDate.parse(toDate,formatter)

    val beginOfTime: String = "19000101"
    val nDaysAgo: String = dt.minusDays(windowSize).toString.replace("-", "")

    val user_tag_dist_New =  computeUserTagDist(user_watched_video, videoTag, nDaysAgo, toDate)
    val videoTag_pop_new = computeVideoPoularity(user_watched_video, nDaysAgo, toDate)
    val video_rec_new = recommendForUser(kTags, nVideo, videoTag_pop_new, videoTag, user_tag_dist_New,
      user_watched_video)


    val video_list_new = video_rec_new.select("video_id").collect().map(row => row.getString(0)).toList
    val w = Window.partitionBy(col("up_id")).orderBy(desc("prob_sum"), desc("pop"))

    if(video_rec_new.count() < nVideo){
      val user_tag_dist_Old =  computeUserTagDist(user_watched_video, videoTag, beginOfTime, toDate)
      val videoTag_pop_old = computeVideoPoularity(user_watched_video, beginOfTime, toDate)
      val watchedAndReced = video_rec_new.select(col("up_id"), col("video_id")).
        unionAll(user_watched_video.select(col("up_id"), col("video_id")))
      val video_rec_old =
        recommendForUser(kTags, nVideo - video_list_new.size, videoTag_pop_old, videoTag, user_tag_dist_Old,
          watchedAndReced).
          where(not(col("video_id").isin(video_list_new))).
          select(col("up_id"), col("video_id"), col("prob_sum") - 1, col("pop") - 1)

      video_rec_new.unionAll(video_rec_old).withColumn("rank", rank().over(w)).filter(col("rank").leq(nVideo)).
        select(col("up_id"), col("video_id"), col("rank"))
    } else{
      video_rec_new.withColumn("rank", rank().over(w)).filter(col("rank").leq(nVideo)).
        select(col("up_id"), col("video_id"), col("rank"))
    }
  }

  /**
    * Similar to recommendForUserNew
    * @param label_id
    * @param kTags
    * @param nVideo
    * @param user_profile
    * @param user_watched_video
    * @param videoTag
    * @param windowSize
    * @return
    */

  def recommendForLabelNew(label_id: String, kTags: Int, nVideo: Int,
                           user_profile: DataFrame, user_watched_video: DataFrame, videoTag: DataFrame,
                           toDate: String, windowSize: Int): DataFrame = {

    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val dt = LocalDate.parse(toDate,formatter)

    val beginOfTime: String = "19000101"
    val nDaysAgo: String = dt.minusDays(windowSize).toString.replace("-", "")

    val label_tag_dist_New =  computeLabelTagDist(label_id, user_profile, user_watched_video,
      videoTag, nDaysAgo, toDate)

    val videoTag_pop_new = computeVideoPoularity(user_watched_video, nDaysAgo, toDate)

    val video_rec_new = recommendForLabel (label_id, kTags, nVideo, user_profile, videoTag_pop_new, videoTag,
      label_tag_dist_New, user_watched_video)

    val video_list_new = video_rec_new.select("video_id").collect().map(row => row.getString(0)).toList

    val w = Window.partitionBy(col("up_id")).orderBy(desc("prob_sum"), desc("pop"))

    if(video_rec_new.count() < nVideo){

      val watchedAndReced = video_rec_new.select(col("up_id"), col("video_id")).
        unionAll(user_watched_video.select(col("up_id"), col("video_id")))

      val label_tag_dist_Old = computeLabelTagDist(label_id, user_profile, user_watched_video,
        videoTag, beginOfTime, toDate)
      val videoTag_pop_old = computeVideoPoularity(user_watched_video, beginOfTime, toDate)
      val video_rec_old =
        recommendForLabel(label_id, kTags, nVideo - video_list_new.size, user_profile, videoTag_pop_old, videoTag,
        label_tag_dist_Old, watchedAndReced).
          where(not(col("video_id").isin(video_list_new))).
          select(col("up_id"), col("video_id"), col("prob_sum") - 1, col("pop") - 1)

      video_rec_new.unionAll(video_rec_old).withColumn("rank", rank().over(w)).filter(col("rank").leq(nVideo)).
        select(col("up_id"), col("video_id"), col("rank"))
    } else{
      video_rec_new.withColumn("rank", rank().over(w)).filter(col("rank").leq(nVideo)).
        select(col("up_id"), col("video_id"), col("rank"))
    }

  }

  // generate random testing data for verification purpose only
  def genTestTables (totalNumTags: Int, hiveContext: HiveContext, usrProfileTName: String,
      watchedVTName: String, videoTagTName: String ): Unit = {
    val nUser: Int = 100
    val nVideo: Int  = 100

    Util.userprofile_gen_Hive(nUser, hiveContext, usrProfileTName)
    Util.uservideo_gen_Hive(totalNumTags, nUser, nVideo, hiveContext, watchedVTName)
    Util.videotag_gen_hive(nVideo, totalNumTags, hiveContext, videoTagTName)

  }
   /**
     *  args(0): recommendation Type, "city" for recommendation based on residential city,
     *  "phone" for recommendation based on phone type,
     *    "user" for recommendation based on user's individual interest (video tag distribution)
     *  args(1): total number of video tags
     *  args(2): number of requested Tag sampled based on which to conduct recommendation
     *  args(3): number of recommended videos (to return)
     *  args(4): starting date of video watching history, only watched video history after
     *    start date will be considered for computing video tag distribution. eg: 20170707
     *  args(5): the latest date in users' watching history. The watching history after the date won't be used as
     *  training data. eg: 20171010
     *  args(6): window size for learning user's latest interest distribution. Let begin_date be the earliest date in
     *  the watching history, and end_date be the latest date. The watching history between "begin_date" and "end_date"
     *  will be used for learning user's "accumulated" interest; while the the history between "end_date - windows size"
     *  and "end_date" will be used for learning user's latest interest.
     *  args(7): user profile hive table name
     *  args(8): user watched video hive table name
     *  args(9): video tag mapping hive table name
     *  args(10): "rec" for recommendation; "train" for testing the accuracy of the recommendation;
     *  "stat" for the KL divergence
     *  args(11): 1 generate random testing data set, 0 use existing hive input tables
     *  args(12): the number of weeks for which to compute KL distances
     *  args(13): window size for calculating the KL distance
     *
     *  return: Dataframe
     *  return a dataframe with schema (up_id: Long, video_id: Long, rank: Int)
     *  The prob_sum is the sum of the probabilities of tags contained in the video
     *  The pop is the popularity of the video
     *  The videos is sorted descending according to prob_sum, then pop
     *
     *  example: bin/spark-submit --class com.huawei.datasight.mllib.rec.RecommBasedonProbDistribution
     *  /path-to-rec-1.0.jar user 80 10 5 20161201 20161231 7 userprofile uservideo videotag rec 0 0 0
     *
     *  each video has less than 80 tags, based on 10 most popular tags, we recommend 5 videos based on the watching history
     *  between "20160101" and "20161010". We also recommend based on the watching
     *  history between "20161010" - 7 and "20161010" and merge them into the final recommending list.
    */

  def main(args: Array[String]): Unit = {
    require(args.length == 14)

    val conf = new SparkConf().setAppName("recommendation-1.5.1")
    val sc = new SparkContext(conf)

    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    // recommendation type
    val recommType: Int = {
      args(0).toString match {
        case "city" => 0
        case "phone" => 0
        case "user" => 1
      }
    }

    // Need to add "phone_type" column to userPorfile table in Hbase. Otherwise,
    // recommendation based on "phone_type" won't work
    // val label = args(0).toString
    val label = "city"

    // total number of video tags or more
    val totalNumTags = args(1).toInt

    // recommendation based on "nTagReq" most popular tags
    val nTagReq: Int = args(2).toInt

    // number of recommendation at most "nRec" videos will be returned
    val nRec: Int = args(3).toInt

    // compute the tag distribution based the watch history after "fromdate"
    val fromdate: String = args(4)

    val toDate: String = args(5).toString

    val window_size: Int = args(6).toInt

    // generating input data for modeling, otherwise,
    // use existing hive tables specified
    if (args(11).toInt == 1) {
      genTestTables(totalNumTags, hiveContext, args(7), args(8), args(9))
    }

    // read input hive tables
    val hivetables = Util.read_hive_table_new(totalNumTags, args(7), args(8), args(9), hiveContext)

    val user_profile = hivetables.userProfile
    val user_Video = hivetables.userVideo
    val video_Tag = hivetables.videoTag

    user_profile.cache()
    user_Video.cache()
    video_Tag.cache()

    /**
      * https://en.wikipedia.org/wiki/Kullback-Leibler_divergence
      * To measure users' interest-change over time, we compare each week's tag-probability distribution with
      * the tag-probability distribution of the watching history before that month.
      * Let P_i be the tag probability distribution of month i's watching history, and P'_i be the tag probability
      * distribution of the watching history before month i (not including i). We compute the KL divergence of the two
      * distribution KL(P_i, P'_i) as the "interest change". We compute the KL divergence for  watching
      * history as the code below. The output for each user is the average, variance, min and max value of
      * all user's KL divergence.
      *
      */

    args(10).toString match {

      case "stat" => {
        val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
        val dt = LocalDate.parse(toDate,formatter)

          val distance_stat = new ArrayBuffer[String]
          for (i <- 0 to args(12).toInt - 1) {

            val today = dt.minusWeeks(i + args(13).toInt).toString.replace("-", "")
            val nWeekAgo = dt.minusWeeks(i + args(13).toInt + 1).toString.replace("-", "")

            val interest_old =
              RecommBasedonProbDistribution.computeUserTagDist(user_Video, video_Tag, "19001010", nWeekAgo)
            val interest_new =
              RecommBasedonProbDistribution.computeUserTagDist(user_Video, video_Tag, nWeekAgo, today)

            distance_stat += Util.computeDistributionDistance(interest_old, interest_new)
          }
        distance_stat.foreach(println)
      }

      case "train" => {
        if (recommType == 0) {
          // compute the watched video tag distribution for city or phone_type based metrics
          computeLabelTagDist(label,
            user_profile, user_Video, video_Tag, fromdate, toDate).write.format("parquet")
            .mode("overwrite").saveAsTable(s"${label}TagDistribution")

          println(Util.computeAccuracy(label, nTagReq, nRec, user_profile, user_Video, video_Tag, toDate, window_size))

        } else {
          // compute the watched video tag distribution over user metrics
          computeUserTagDist(user_Video, video_Tag, fromdate, toDate).write.format("parquet").mode("overwrite")
            .saveAsTable("UserTagDistribution")

          /**
            * Compute the accuracy of the recommendation results for users.
            * We use the videos watched within the lasest window_size days as the testing set;
            * the others as the training set.Let R be the set of videos to recommend based on the training set,
            * and W be the videos in the testing set.
            * We compute the accuracy as | R \intersect W | / |R|
            */
          println(Util.computeAccuracy("user", nTagReq, nRec, user_profile, user_Video, video_Tag, toDate, window_size))
        }
      }

      case "rec" => {
        // recommendation to a user based on city/phone tag distribution
        if (recommType == 0) {
          val recList =
            recommendForLabelNew(label, nTagReq, nRec, user_profile, user_Video, video_Tag, toDate, window_size)

//          recList.join(video_Tag, "video_id").show()
          recList.join(video_Tag, "video_id").write.format("parquet").mode("overwrite").
            saveAsTable(s"RecommendationBasedOn$label")

        } else {
          val recList = recommendForUserNew(nTagReq, nRec, user_Video, video_Tag, toDate, window_size)
//          recList.join(video_Tag, "video_id").show()
          recList.join(video_Tag, "video_id").write.format("parquet").mode("overwrite").
            saveAsTable(s"RecommendationBasedOnUserInterest")
        }
      }
    }

    user_profile.unpersist()
    user_Video.unpersist()
    video_Tag.unpersist()
  }
}
