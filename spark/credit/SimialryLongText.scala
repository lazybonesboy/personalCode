package com.credit

import org.apache.spark.{SparkConf, SparkContext}

import scala.math.{BigInt, _}
import scala.util.control.Breaks._
import java.text.SimpleDateFormat
import java.util.{Date, Calendar}

/**
  * Created by ellisonliu on 2016/9/18.
  * dws_credit_pub_account_groupsend_repeat_article_num_user_d
  * dws_credit_pub_account_groupsend_repeat_article_num_user_m
  * dws_credit_pub_account_groupsend_repeat_article_sameday_user_pair_d
  * dws_credit_pub_account_groupsend_repeat_article_sameday_user_pair_m
  * dws_credit_pub_account_groupsend_repeat_article_user_pair_d
  * dws_credit_pub_account_groupsend_repeat_article_user_pair_m
  * dws_credit_pub_account_groupsend_simhash_d
  * dws_credit_pub_account_groupsend_simhash_simi_result_d
  */
object SimialryLongText {

  //reduce时指定partition数；
  var reducePartiionNum : Int = 300;
  //汉明距离默认阈值
  var distanceDefault : Int = 5;
  // 指定计算N天前数据
  var nBeforeDefault : Int = 6;
  // 指定计算key的长度，控制数据倾斜；
  val controlKeyNumDefault : Int = 20;
  val controlValueNumDefault : Int = 1000;


  //sum 二进制中1的个数，支持正负数
  def statNumOfBinary(v: Int) :Int ={
    var num=0
    var value= v
    if (v<0)
      value=v.unary_~
    while(value != 0){
      value &= (value-1);
      num += 1;
    }
    if (v<0)
      return 8-num
    return num
  }

  // 汉明距离 > 5 直接返回
  def hammingDistance( item1 : String, item2 : String) : Int ={
    var artile1 = BigInt( item1 )
    var artile2 = BigInt( item2 )
    //对两个数据进行异或操作，转换成二进制
    var byteArr = (artile1^artile2).toByteArray
    var hammingDistances = 0

    breakable {
      for (byt <- byteArr){
        hammingDistances += statNumOfBinary(byt)
        if (hammingDistances >=distanceDefault){
          break()
        }
      }
    }
    hammingDistances
  }

  // 计算数组中两两的相似度
  def getComputePairs( items: String, curDay : String) : List[String] = {
    var pairs : List[String] = List();
    var itemList: Array[String] = items.split(" ");
    breakable {
      for(  i <- 0 to (itemList.length -2) ){
          if ( ! itemList(i).contains("#"+curDay))
            break()
          for (j <- i + 1 to (itemList.length - 1)) {
            //计算汉明距离，只取距离较小的
            if(hammingDistance(itemList(i).split("_")(0), itemList(j).split("_")(0) ) < distanceDefault)
                pairs = (itemList(i) + "-" + itemList(j)) :: pairs
          }
      }
    }
    pairs
  }

  //对比较长的有相同二进制分区的数组进行拆分，解决数据倾斜的问题
  def splitLongerPair(items: String, curDay : String) : List[String] = {
    if ( ! items.contains(curDay))
      return Nil

    var itemList: Array[String] = items.split(" ");
    if ( itemList.length < 1000 )
      return List(items)

    var newItems : List[String] = List();
    val pos = items.lastIndexOf("#"+curDay)
    val firstItems = items.substring(0, pos+9)
    val secondItems = items.substring(pos+9)
    val firstItemsArr : Array[String] = firstItems.split(" ")
    val secondItemsArr : Array[String] = secondItems.split(" ")
    if (firstItemsArr.length <= controlKeyNumDefault && secondItemsArr.length <= controlValueNumDefault)
      return List(items)
    val firstLastNum : Int = firstItemsArr.length % controlKeyNumDefault
    val secondLastNum : Int = secondItemsArr.length % controlValueNumDefault
    for ( i <- 1 to firstItemsArr.length/controlKeyNumDefault + (if(firstLastNum == 0) 0 else 1)){
      for( j<- 1 to secondItemsArr.length/controlValueNumDefault + (if(secondLastNum == 0) 0 else 1) ){
        var firstSlice : String = ""
        var secondSlice : String = ""
        if ( i == firstItemsArr.length/controlKeyNumDefault + 1){
          firstSlice = firstItemsArr.slice( controlKeyNumDefault*(i-1), firstItemsArr.length - 1).mkString(" ").trim
        }else{
          firstSlice = firstItemsArr.slice(controlKeyNumDefault*(i-1), controlKeyNumDefault*i-1).mkString(" ").trim
        }
        if ( j == secondItemsArr.length/controlValueNumDefault + 1){
          secondSlice = secondItemsArr.slice(controlValueNumDefault*(j-1), secondItemsArr.length - 1).mkString(" ").trim
        }else{
          secondSlice = secondItemsArr.slice(controlValueNumDefault*(j-1), controlValueNumDefault*j-1).mkString(" ").trim
        }
        val newItem = firstSlice + " " + secondSlice
        newItems =  newItems :+ newItem.trim
      }
    }
    newItems = newItems :+ firstItems.trim

    newItems
  }

  //获取指定日期的n天前日期
  def getBeforeDay( curDay : String, before: Int): String ={
    var df : SimpleDateFormat  = new SimpleDateFormat("yyyyMMdd");
    var cal : Calendar = Calendar.getInstance();
    if (cal == "today"){
      return  df.format(cal.getTime())
    }
    val curDate : Date = df.parse(curDay)
    cal.setTime(curDate)
    cal.add(Calendar.DAY_OF_MONTH, before)
    return df.format(cal.getTime())
  }

  def main(args: Array[String]) {
    var curDay : String = ""
    var nBeforeDay : Int = nBeforeDefault
    if (args.length == 3) {
      curDay = args(0)
      nBeforeDay = args(1).toInt
      reducePartiionNum = args(2).toInt
    } else if (args.length == 2) {
      curDay = args(0)
      nBeforeDay = args(1).toInt
    }else if (args.length == 1){
      curDay = args(0)
    }else {
      curDay = getBeforeDay("today", 0)
    }

    val partFileName = "hdfs://cluster-infosec-base/dws/credit/dws_credit_pub_account_groupsend_simhash_d/dt="
    var fullFileName = partFileName+curDay+"/";
    for ( before <- -nBeforeDay to -1) {
      fullFileName += "," + partFileName + getBeforeDay(curDay, before) +"/"
    }

    val conf = new SparkConf()
    val sc = new SparkContext(conf.setAppName("PubAccountArticleSimilary"))

    val fileName = sc.textFile(fullFileName, 10)
    val haspart = fileName.map( line => line.split("\\t") )
      //过滤掉simhash为0的值
      .filter( line => line(1).length()>0 && BigInt(line(1)) > 0 )
      //对simhash值进行拆分, 32位二进制分为4段；
      .flatMap( line => {
          Iterator( ( "1_" + (BigInt(line(1)) >> 48).toString(), line(1) + "_" + line(0) + "#" + line(2)),
            ( "2_" + ((BigInt(line(1)) >> 32).longValue % pow(2, 16)).toInt.toString(), line(1) + "_" + line(0) + "#" + line(2) ),
            ( "3_" + ((BigInt(line(1)) >> 16).longValue % pow(2, 16)).toInt.toString(), line(1) + "_" + line(0) + "#" + line(2) ),
            ( "4_" + ((BigInt(line(1))) % 65536).toInt.toString(), line(1) + "_" + line(0) + "#" + line(2) )
          )
        })
      // 合并时，去除相同的数据
      .reduceByKey( (destStr, addStr) => {
        if (destStr.contains(addStr))
          destStr
        //指定日期的在前，其它日期的在后面
        else if (addStr.contains("#"+curDay))
          addStr + " " + destStr
        else
          destStr +" " + addStr
        }, reducePartiionNum )

//      .map(line=> (line._2.split(" ").length, line._2))
//      .sortByKey(false, 2)
      //只取所有可能相似的simhash对
      .map(line=> line._2)
      //过滤掉没有相似文章的simhash
      .filter( _.split("_").length>2)
      .flatMap( splitLongerPair(_, curDay) )
      .cache()
      .repartition(reducePartiionNum+ reducePartiionNum/2)
      //展开所有需要计算相似度的simhash对
      .flatMap( getComputePairs(_, curDay) )
      // 去重后只对key
      .map( (_, 1) )
      .reduceByKey( _ + _, reducePartiionNum + reducePartiionNum/2)
      .keys
//      //计算汉明距离，并过滤掉距离较大的
//      .filter( x => {
//          val items : Array[String] = x.split("-");
//          x.contains("#"+curDay) && hammingDistance(items(0).split("_")(0),items(1).split("_")(0) ) < distanceDefault
//        })
      //按指定格式输出 uin1 simhash1 dt1 uin2 simhash2 dt2
      .map( x => {
          val items = x.split("-|_|#");
          if (items.length != 6) (Nil)
          if ( items(2) > items(5))
            (items(1) + "\t" + items(0) + "\t" + items(2) + "\t" + items(4) + "\t" + items(3) + "\t" + items(5))
          else
            (items(4) + "\t" + items(3) + "\t" + items(5) + "\t" + items(1) + "\t" + items(0) + "\t" + items(2) )
      })

    val timestamp: Long = System.currentTimeMillis / 1000
    haspart.coalesce(50, false).saveAsTextFile("hdfs://cluster-infosec-base/dws/credit/dws_credit_pub_account_groupsend_simhash_simi_result_d/dt="+curDay + "/"+timestamp )
    sc.stop()
  }
}
