# Apache Spark dependency

Built based on Apache Spark 1.5.1 && Scala 2.10.4 && Java 1.8

you should perform your normal Hadoop, Spark installation and configuration
including Hive metastore setup since the input data is represented as hive table.

## Build the package and run unit test

mvn package

## Build the package without unit test

mvn package -DskipTests

## Example execution command
 input args description: 

 - args(0): recommendation Type, "city" for recommendation based on residential city, "phone" for recommendation based on phone type, and"user" for recommendation based on user's individual interest
 - args(1): total number of video tags or more
 - args(2): number of requested tag sampled based on which to conduct recommendation
 - args(3): number of recommended videos
 - args(4): starting date of video watching history, only watched video history after start date will be considered for computing video tag distribution. eg: 20170707
 - args(5): the latest date in users' watching history. The watching history after the date won't be used as training data. eg: 20171010
 - args(6): the window size for learning user's latest interest distribution. Let begin_date be the earliest date in the watching history, and end_date be the latest date. The watching history between "begin_date" and "end_date" will be used for learning user's "whole" interest; while the the history between "end_date - windows size" and "end_date" will be used for learning user's latest interest.
 if args(10) = test, then the argument is the number of days used for "testing set"
 - args(7): user profile hive table name
 - args(8): user's watching history hive table name
 - args(9): video second-class(tag) mapping hive table name
 - args(10): "rec" for recommendation; "train" for testing the accuracy of the recommendation; "stat" for computing the KL divergence of the tag distributions
 - args(11): 1 generate random testing data set, 0 use existing hive input tables
 - args(12): the number of weeks for which to compute KL distances
 - args(13): window size for calculating the KL distance

# Recommendation based on city tag distribution with generated data
bin/spark-submit --class com.huawei.datasight.mllib.rec.RecommBasedonProbDistribution ../rs-city-1.5.1/target/rec-1.0.jar city 80 10 5 20160101 20161010 7 userProfile userVideo videoTag rec 0 0 0 
- each video has less than 80 tags, based on 10 most popular tags, we recommend 5 videos based on the watching history between "20161010 -7 " and "20161010". We also recommend based on the watching
  history between "20160101" and "20161010" and merge them into the final recommending list.

# Recommendation based on user tag distribution with generated data
bin/spark-submit --class com.huawei.datasight.mllib.rec.RecommBasedonProbDistribution ../rs-city-1.5.1/target/rec-1.0.jar user 80 10 5 20160101 20161010 7 userProfile userVideo videoTag rec 0 0 0

# Compute KL divergence
bin/spark-submit --class com.huawei.datasight.mllib.rec.RecommBasedonProbDistribution ../rs-city-1.5.1/target/rec-1.0.jar user 80 10 5 20160101 20161010 7 userProfile userVideo videoTag stat 0 8 7
- compute the 8 weeks' tag distribution before 2016-10-10 and the KL distance between each of the 8 week's distribution and the distribution before that week
# Compute the accuracy of the recommendation results
bin/spark-submit --class com.huawei.datasight.mllib.rec.RecommBasedonProbDistribution ../rs-city-1.5.1/target/rec-1.0.jar user 80 10 5 20160101 20161010 7 userProfile userVideo videoTag train 0 0 0
- train and recommend based on the watching history before "2016-10-10 - 7" and test with the watching history after "2016-10-10 - 7"