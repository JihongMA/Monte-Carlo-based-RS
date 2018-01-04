/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.datasight.mllib.rec

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.types._
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}

class recSuite extends org.scalatest.FunSuiteLike with org.scalatest.BeforeAndAfterAll {
  var sc: SparkContext = _
  var sqlContext: SQLContext = _
  val nUser: Int = 10
  val nVideo: Int = 10
  val nTag: Int = 5
  val nCity: Int = 5
  val nPhone: Int = 5
  val nTagReq: Int = 3
  val nRec: Int = 1
  val fromdate: String = "20161001"
  val todate: String = "20181001"

  val userVideoSchema = StructType(Array(
    StructField("up_id", StringType, true),
    StructField("video_id", StringType, true),
    StructField("play_date", StringType, true),
    StructField("device_name", StringType, true),
    StructField("video_second_class", StringType, true)
  ))

  val videoTagSchema = StructType(Array(
    StructField("video_id", StringType, true),
    StructField("video_second_class", StringType, true)
  ))

  val userProfileSchema = StructType(Array(
    StructField("id", StringType, true),
    StructField("reside_city", StringType, true),
    StructField("phone_type", StringType, true)
  ))

  val cityTagSchema = StructType(Array(
    StructField("reside_city", StringType, true),
    StructField("prob", ArrayType(DoubleType), true)
  ))

  val userTagSchema = StructType(Array(
    StructField("up_id", StringType, true),
    StructField("prob", ArrayType(DoubleType), true)
  ))

  var userVideoRows = new ListBuffer[Row]()
  userVideoRows += Row("1", "1", "2017-01-02", "Mate1", "0")
  userVideoRows += Row("1", "2", "2017-01-02", "Mate1", "1")
  userVideoRows += Row("2", "2", "2017-01-02", "Mate1", "1")
  userVideoRows += Row("2", "3", "2017-01-02", "Mate1", "0,1,2")
  userVideoRows += Row("3", "3", "2017-01-02", "Mate1", "0,1,2")
  userVideoRows += Row("3", "1", "2017-01-02", "Mate1", "0")
  userVideoRows += Row("3", "1", "2016-01-02", "Mate1", "0")
  userVideoRows += Row("2", "1", "2016-01-05", "Mate1", "0")

  var videoTagRows = new ListBuffer[Row]()

  videoTagRows += Row("1", "0")
  videoTagRows += Row("2", "1")
  videoTagRows += Row("3", "0,1,2")
  videoTagRows += Row("4", "")

  var userProfileRows = new ListBuffer[Row]()
  userProfileRows += Row("1", "Beijing", "Mate1")
  userProfileRows += Row("2", "Beijing", "Mate1")
  userProfileRows += Row("3", "Beijing", "Mate1")
  userProfileRows += Row("4", "", "Mate1")

  val userVideo = TestHive.createDataFrame(TestHive.sparkContext.parallelize(userVideoRows), userVideoSchema)
  val videoTag = TestHive.createDataFrame(TestHive.sparkContext.parallelize(videoTagRows), videoTagSchema)
  val userProfile = TestHive.createDataFrame(TestHive.sparkContext.parallelize(userProfileRows), userProfileSchema)

  TestHive.sql("CREATE TABLE IF NOT EXISTS userProfile (id string, reside_city string, phone_type string)")
  TestHive.sql("CREATE TABLE IF NOT EXISTS userVideo (up_id varchar(32), video_id varchar(128), play_date varchar(10), " +
    "device_name string, video_second_class varchar(256))")
  TestHive.sql("CREATE TABLE IF NOT EXISTS videoTag (video_id varchar(128), video_second_class varchar(256))")

  userProfile.write.mode("overwrite").saveAsTable("userProfile")
  videoTag.write.mode("overwrite").saveAsTable("videoTag")
  userVideo.write.mode("overwrite").saveAsTable("userVideo")

  val hivetables = Util.read_hive_table_new(3, "userProfile", "userVideo", "videoTag", TestHive)

  val user_City = hivetables.userProfile
  val user_Video = hivetables.userVideo
  val video_Tag = hivetables.videoTag

  override def beforeAll() {
    super.beforeAll()

    sc = TestHive.sparkContext
    sqlContext = new SQLContext(sc)
  }

  test("read_hive_table"){
    Util.read_hive_table_new(3, "userProfile", "userVideo", "videoTag", TestHive)
    user_City.show()
    user_Video.show()
    video_Tag.show()
  }

  test("get_city_tag_distribution") {
    var cityTagRows = new ListBuffer[Row]()
    cityTagRows += Row("Beijing", Seq(0.4, 0.4, 0.2))
    assert(RecommBasedonProbDistribution.computeLabelTagDist("city", user_City, user_Video, video_Tag,
      fromdate, todate).collect() === cityTagRows)
  }

  test("get_user_tag_distribution") {
    var userTagRows = new ListBuffer[Row]()
    userTagRows += Row("1", Seq(0.5, 0.5, 0.0))
    userTagRows += Row("2", Seq(0.25, 0.5, 0.25))
    userTagRows += Row("3", Seq(0.5, 0.25, 0.25))
    assert(RecommBasedonProbDistribution.computeUserTagDist(user_Video, video_Tag,
          fromdate, todate).collect().toSet === userTagRows.toSet)
  }

  test("get_video_popularity") {
    var videoPop = new ListBuffer[Row]()
    videoPop += Row("1", 1.0/3)
    videoPop += Row("2", 1.0/3)
    videoPop += Row("3", 1.0/3)
   assert(RecommBasedonProbDistribution.computeVideoPoularity(user_Video, fromdate, todate).
      collect().toSet === videoPop.toSet)
  }

    test("recommend_city") {
      var userVideoRows = new ListBuffer[Row]()
      userVideoRows += Row("1", "3", 1.0, 1.0/3)

      var citytagRows = new ListBuffer[Row]()
      citytagRows += Row("Beijing", Seq(0.0, 0.0, 1.0))
      val citytag = TestHive.createDataFrame(TestHive.sparkContext.parallelize(citytagRows), cityTagSchema)

      val videopop = RecommBasedonProbDistribution.computeVideoPoularity(user_Video, fromdate, todate)

      assert(RecommBasedonProbDistribution.recommendForLabel("city", 3, nRec, user_City, videopop, video_Tag, citytag,
        user_Video).collect().toSet === userVideoRows.toSet)
  }

  test("recommend_user") {
    var userVideoRows = new ListBuffer[Row]()
    userVideoRows += Row("1", "3", 1.0, 1.0/3)
    userVideoRows += Row("3", "2", 1.0, 1.0/3)

    var usertagRows = new ListBuffer[Row]()
    usertagRows += Row("1", Seq(0.0, 0.0, 1.0))
    usertagRows += Row("2", Seq(0.0, 0.0, 1.0))
    usertagRows += Row("3", Seq(0.0, 1.0, 0.0))
    val usertag = TestHive.createDataFrame(TestHive.sparkContext.parallelize(usertagRows), userTagSchema)

    val videopop = RecommBasedonProbDistribution.computeVideoPoularity(user_Video, fromdate, todate)

    assert(RecommBasedonProbDistribution.recommendForUser(3, nRec, videopop, video_Tag, usertag, user_Video).
      collect().toSet === userVideoRows.toSet)
  }

  test("compute KL-distance") {
    var usertagRows1 = new ListBuffer[Row]()
    usertagRows1 += Row("1", Seq(0.1, 0.1))
    usertagRows1 += Row("2", Seq(0.2, 0.2))


    var usertagRows2 = new ListBuffer[Row]()
    usertagRows2 += Row("1", Seq(0.1, 0.01))
    usertagRows2 += Row("2", Seq(0.2, 0.02))


    val usertag1 = TestHive.createDataFrame(TestHive.sparkContext.parallelize(usertagRows1), userTagSchema)
    val usertag2 = TestHive.createDataFrame(TestHive.sparkContext.parallelize(usertagRows2), userTagSchema)
    val stat = Util.computeDistributionDistance(usertag1, usertag2).split(",")

    assert(stat(0) === "0.1" && stat(1) === "0.2" &&
      math.abs(stat(2).toDouble - 0.15) < 0.01 &&
      math.abs(stat(3).toDouble - 0.0025) < 0.0001)
//    assert(stat(0) === "0.1,0.2,0.15000000000000002,0.0024999999999999988")
  }
//
//  test("genDataset") {
//    val nUser: Int = 10
//    val nVideo: Int  = 10
//
//    Util.userprofile_gen_Hive(nUser, TestHive, "usrprofile")
//    Util.uservideo_gen_Hive(nTag, nUser, nVideo, TestHive, "uservideo")
//    Util.videotag_gen_hive(nVideo, nTag, TestHive, "videotag")
//
//    val hivetables = Util.read_hive_table_new(nTag, "userprofile", "uservideo", "videotag", TestHive)
//
//    val user_profile = hivetables.userProfile
//    val user_Video = hivetables.userVideo
//    val video_Tag = hivetables.videoTag
//
//    user_profile.show()
//    user_Video.show()
//    video_Tag.show()
//  }

}
