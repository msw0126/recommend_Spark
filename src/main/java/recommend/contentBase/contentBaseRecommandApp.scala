package recommend.contentBase

/**
  * 根据电影内容属性相似-推荐
  * 原理：https://gitbook.cn/gitchat/column/5b4ee9df01d18a561f3430d9/topic/5b4ef0b001d18a561f343553
  * 经过每一个电影的属性与其他的经过过滤过的电影的属性相比较，返回的是
  * movieid(当前观众正在看的电影)、movieid_re(可以推荐的电影)、score(分数)  三个字段，可以根据score进行取topN来返回给用户
  */

import com.hankcs.hanlp.HanLP
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.SparkConf
import utils.movieYearRegex

import scala.collection.JavaConversions._
import scala.util.control.Breaks


object contentBaseRecommandApp {
  def main(args : Array[String]): Unit = {

    //设置hive访问host以及端口  hadoop9
    //注意：实际使用的时候，替换成自己服务器集群中，hive的host以及访问端口
//    val HIVE_METASTORE_URIS = "thrift://node1:10000"  // ,thrift://node2:10000
//    System.setProperty("hive.metastore.uris", HIVE_METASTORE_URIS)

    //构建一个通用的sparkSession
    // 本地模式
//    val sparkConf = new SparkConf().setAppName("content-base-Re").setMaster("local[*]")
//    val sparkSession = SparkSession.builder()
//      .config(sparkConf)
//      .enableHiveSupport()
//      .getOrCreate()

    // yarn模式
    val sparkSession = SparkSession
      .builder()
      .appName("content-base-Re")
      .enableHiveSupport()
      .getOrCreate()

    val sc = sparkSession.sparkContext
    //DataFrame格式化申明
    val schemaString = "movieid tag"
    val schema = StructType(schemaString.split(" ").map(fieldName=>StructField(fieldName,StringType,true)))

    //获取rating评分数据集(movie,avg_rate)
    val movieAvgRate = sparkSession.sql("select movieid, round(avg(rate),1) as avg_rate  from contentbase.cb_ratings group by movieid").rdd.map{
      f=>
        (f.get(0),f.get(1))
    }
    // 打印RDD
//    movieAvgRate.take(10).foreach {println}
//    println("-----------------------------------------")

    //获取电影的基本属性数据
    val moviesData = sparkSession.sql("select movieid,title,genre from contentbase.cb_movies").rdd
    // 打印RDD
//    moviesData.take(10).foreach{println}
//    println("-----------------------------------------")

    //获取电影tags数据
    val tagsData = sparkSession.sql("select movieid,tag from contentbase.cb_tags").rdd
    // 打印RDD
//    tagsData.collect().foreach{println}

    System.out.println("=================001 GET DATA===========================")

    //进行tags标签的处理,包括分词，去除停用词等等
    val tagsStandardize = tagsData.map{
      f =>
        val movieid = f.get(0)
        //进行逻辑判断，size>3的进行标准化处理
        val tag = if (f.get(1).toString.split(" ").size <= 3) {
          f.get(1)
        }else{  // 如果tag有超过3个词，会把主题提取出来
          //进行主题词抽取(能屏蔽掉停用词)
          HanLP.extractKeyword(f.get(1).toString, 20).toSet.mkString(" ")
        }
        (movieid,tag)
    }
    // 打印RDD
//    tagsStandardize.take(10).foreach{println}

    System.out.println("=================002 HANLP===========================")

    //进行相似tag合并操作，最终返回依然是(mvieid,tag)集合，但tag会做预处理
    val tagsStandardizeTmp = tagsStandardize.collect()
    // 打印RDD
//    tagsStandardizeTmp.foreach(println)
    val tagsSimi = tagsStandardize.map{
      f=>
        var retTag = f._2
        if (f._2.toString.split(" ").size == 1) {
          var simiTmp = ""
          // 得到tag长度大于当前tag的，比如：当前tag是blood，只要tag是bloods、bloody都可以
          val tagsTmpStand = tagsStandardizeTmp
                        .filter(_._2.toString.split(" ").size != 1 )
                        .filter(f._2.toString.size < _._2.toString.size)
                        .sortBy(_._2.toString.size)
//          println("----------------------------------------")
//          tagsTmpStand.foreach {println}
//          println("----------------------------------------")
          var x = 0
          val loop = new Breaks

          tagsTmpStand.map{
            tagTmp=>
              // getEditSize 是计算两个词的编辑距离，当编辑距离在一定阈值的时候，进行两个词的合并.
              // 使用当前循环到的tag与tagsTmpStand（tag>1）的rdd的所有词进行比较，如果相近，就把当前的tag替换成相同的tag
              val flag = getEditSize(f._2.toString,tagTmp._2.toString)
              if (flag == 1){
                retTag = tagTmp._2
                loop.break()
              }
          }
          ((f._1,retTag),1)
        } else {
          ((f._1,f._2),1)
        }
    }
    // 打印RDD
//    tagsSimi.collect()
//    println("--------------------------------------------------------")
//    tagsSimi.collect().foreach{println}

    System.out.println("=================003 SIMI===========================")
//    println(tagsSimi.collect().length)
//    println(tagsSimi.reduceByKey(_+_).collect().length)
//    tagsSimi.reduceByKey(_+_).collect().foreach{println}

    //先将预处理之后的movie-tag数据进行，统计频度，作为tag权重,形成(movie,tagList(tag,score))这种数据集
    val movieTag = tagsSimi.reduceByKey(_+_).groupBy(k=>k._1._1).map{
      f=>
        (f._1,f._2.map{
          ff=>
            (ff._1._2,ff._2)  // (tag,score)
        }.toList.sortBy(_._2).reverse.take(10).toMap)  // tagList(tag,score) 取出score最高的10个
    }
//    movieTag.collect().foreach(println)
    System.out.println("=================004 MOVIE-TAG OK===========================")

    //处理类别、年份、名称.数据格式：(movieid,tagList(genres,titleWorlds,year))
    val moviesGenresTitleYear = moviesData.map{
      f=>
        val movieid = f.get(0)
        val title = f.get(1)
        val genres = f.get(2).toString.split("|").toList.take(10)
        val titleWorlds = HanLP.extractKeyword(title.toString, 10).toList
        val year = movieYearRegex.movieYearReg(title.toString)
        (movieid,(genres,titleWorlds,year))
    }

    System.out.println("=================005 MOVIE-GENRE/TITLE/YEAR OK===========================")
    //关联合并数据，通过movieid，进行属性汇总
    // 通过 join 进行数据合并，生成一个以电影 id 为核心的属性集合
    val movieContent = movieTag.join(movieAvgRate).join(moviesGenresTitleYear).map{
      f=>
        //(movie,tagList,titleList,year,genreList,rate)
        (f._1,f._2._1._1,f._2._2._2,f._2._2._3,f._2._2._1,f._2._1._2)
    }

//    movieContent.collect().foreach(println)
    //进行相似计算，计算之前进行预处理，降低计算代价
    val movieConetentTmp = movieContent.filter( f=> scala.math.BigDecimal( f._6.toString).doubleValue() < 3.5).collect()
    val movieContentBase = movieContent.map{
      f=>
        val currentMoiveId = f._1
        val currentTagList = f._2  //[(tag,score)]
        val currentTitleWorldList = f._3
        val currentYear = f._4
        val currentGenreList = f._5
        val currentRate = scala.math.BigDecimal(f._6.toString).doubleValue()  // 控制小数点
        val recommandMovies = movieConetentTmp.map{
          ff=>
            val tagSimi = getCosTags(currentTagList,ff._2)
            val titleSimi = getCosList(currentTitleWorldList,ff._3)
            val genreSimi = getCosList(currentGenreList,ff._5)
            val yearSimi = getYearSimi(currentYear,ff._4)
            val rateSimi = getRateSimi(scala.math.BigDecimal(ff._6.toString).doubleValue())
            val score = 0.4*genreSimi + 0.25*tagSimi + 0.1*yearSimi + 0.05*titleSimi + 0.2*rateSimi
            (ff._1,score)
        }.toList.sortBy(k=>k._2).reverse.take(20)
        (currentMoiveId,recommandMovies)
    }.flatMap(f=>f._2.map(k=>(f._1,k._1,k._2))).map(f=>Row(f._1,f._2,f._3))
//    movieContentBase.collect().foreach(println(_))

    System.out.println("=================006 SIMI COUNT===========================")

    //DataFrame格式化申明
    val schemaString2 = "movieid movieid_re score"
    val schemaContentBase = StructType(schemaString2.split(" ")
      .map(fieldName=>StructField(fieldName,if (fieldName.equals("score")) DoubleType else  IntegerType,true)))

    val movieContentBaseDataFrame = sparkSession.createDataFrame(movieContentBase,schemaContentBase)
    //将结果存入hive
    movieContentBaseDataFrame.repartition(1).toDF().write.mode(SaveMode.Overwrite).saveAsTable("contentbase.mite_content_base_re")

    System.out.println("=================007 SAVE===========================")

    // spark停止
    sc.stop()
  }

  //计算年份的相似度
  def getRateSimi(rate2: Double): Double ={
    if (rate2 >= 5) {
      1
    } else {
      rate2 / 5
    }
  }


    //计算年份的相似度
  def getYearSimi(year1: Int,year2: Int): Double ={
    val count = Math.abs(year1-year2)
    if (count > 10 ){
      0
    } else {
      (1-count)/10
    }
  }

  //计算两个MapTag的余弦值
  def getCosList(listTags1:List[String],listTags2:List[String]): Double = {

    //分子累和部分
    var xySum : Double = 0
    //分母开方前的Ai平方累和部分
    var aSquareSum : Double = 0
    //分母开方前的Bi平方累和部分
    var bSquareSum : Double = 0

    listTags1.union(listTags2).map{
      f=>
        if (listTags1.contains(f)) aSquareSum += 1
        if (listTags2.contains(f)) bSquareSum += 1
        if (listTags1.contains(f) && listTags2.contains(f)) xySum += 1
    }

    if (aSquareSum != 0 && bSquareSum != 0){
      xySum/(Math.sqrt(aSquareSum)*Math.sqrt(bSquareSum))
    } else {
      0
    }

  }

    //计算两个MapTag的余弦值
  def getCosTags(listTagsCurrent:Map[Any,Int],listTagsTmp:Map[Any,Int]): Double = {

    //分子累和部分
    var xySum : Double = 0
    //分母开方前的Ai平方累和部分
    var aSquareSum : Double = 0
    //分母开方前的Bi平方累和部分
    var bSquareSum : Double = 0

    val tagsA = listTagsCurrent.map(f=>f._1).toList
    val tagsB = listTagsTmp.map(f=>f._1).toList
    tagsA.union(tagsB).map{
      f=>
        if (listTagsCurrent.contains(f)) (aSquareSum += listTagsCurrent.get(f).get*listTagsCurrent.get(f).get)
        if (listTagsTmp.contains(f)) (bSquareSum += listTagsTmp.get(f).get*listTagsTmp.get(f).get)
        if (listTagsCurrent.contains(f) && listTagsTmp.contains(f)) (xySum += listTagsCurrent.get(f).get*listTagsTmp.get(f).get)
    }

    if (aSquareSum != 0 && bSquareSum != 0) {
      xySum/(Math.sqrt(aSquareSum)*Math.sqrt(bSquareSum))
    } else {
      0
    }

  }

    //合并tag，合并原则：长度=1(单个词)；前缀相似度>=2/7，
  def getEditSize(str1:String,str2:String): Int ={
    if (str2.size > str1.size){
      0
    } else {
      //计数器
      var count = 0
      val loop = new Breaks
      //以较短的str2进行遍历，并逐个比较
      val lengthStr2 = str2.getBytes().length
      var i = 0
      for ( i <- 1 to lengthStr2 ){
        if (str2.getBytes()(i) == str1.getBytes()(i)) {
          //逐个匹配字节，相等则计数器+1
          count += 1
        } else {
          //一旦出现前缀不一致则中断循环，开始计算重叠度
          loop.break()
        }
      }

      //计算重叠度,当前缀重叠度大于等于2/7时，进行合并
      if (count.asInstanceOf[Double]/str1.getBytes().size.asInstanceOf[Double] >= (1-0.286)){
        1
      }else{
        0
      }
    }
  }

}
