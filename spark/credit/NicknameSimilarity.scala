package com.credit

import org.apache.spark.{SparkConf, SparkContext}

import scala.math.{BigInt, _}
import scala.util.control.Breaks._
import java.text.SimpleDateFormat
import java.util.{Date, Calendar}
import scala.collection.mutable.ListBuffer

/**
  * Created by ellisonliu on 2016/10/9.
  * compute similarity of two short text, such as pub accout nickname, weixin username;
  * don't compute similarity:
  *     1. if both is number, don't compute;
  *     2. the distance of both's length is more than 3;
  *     3. if both is letters, the short text's length must be larger than 5.
  */
object NicknameSimilarity {

    def isAlapha(x: Char) = Character.isDigit(x) || Character.isLetter(x)

    def isAllDigits(x: String) = (x != "") && (x forall Character.isDigit)

    def isAllLetters(x: String) = (x != "") && (x forall isAlapha)

    def editDist[A](a: Iterable[A], b: Iterable[A]) =
        ((0 to b.size).toList /: a) ((prev, x) =>
            (prev zip prev.tail zip b).scanLeft(prev.head + 1) {
                case (h, ((d, v), y)) => math.min(math.min(h + 1, v + 1), d + (if (x == y) 0 else 1))
            }) last

    //快排算法
    def sort(ls: List[(Int, Int)]): List[(Int, Int)] = {
        ls match {
            case Nil => Nil
            case base :: tail => {
                val (left, rigth) = tail.partition(_._2 < base._2)
                sort(left) ::: base :: sort(rigth)
            }
        }
    }


    def main(args: Array[String]) {
        //输入文件格式：accountId nickname
        val inputPath = args(0)
        //输出文件格式：accountId1 nickname1 accountId2 nickname2 editdistance
        //        val outputPath=args(1)
        //取前缀长度
        val subSize = 3
        //允许的字符差异
        val edDiff = 2
        val notComputeNick = ListBuffer[String]()

        val conf = new SparkConf()
        val sc = new SparkContext(conf.setAppName("NicknameSimilarity"))
        val fileNameRdd = sc.textFile(inputPath, 10)
        val paisRdd = fileNameRdd.map(line => line.trim.split("\t"))
            /*
            过滤条件：
            1. 没有昵称的过滤掉；
            2. 昵称长度必需大于3
            3. 不能是以新注册公众号和微信公众号开头， 通过查看数据得到，这两个容易引起数据倾斜；
            4. 不能用QQ邮箱
             */
            .filter(item => item.length == 2 && item(1).trim.length > 3 && !item(1).trim.startsWith("新注册公众号") && !item(1).trim.startsWith("微信公众号") && !isAllDigits(item(1).trim.replace("@qq.com","")))
            .map(item => (item(1).trim.substring(0, subSize).toLowerCase, item(0).trim + ":" + item(1).replace(" ", "")
                .toLowerCase()
                .trim))
            //用空格区分各个节点，昵称中忽略空格
            .reduceByKey((first, second) => first + " " + second, 500)
        //             .map( item => {val n = item._2.split(" ").length; (n,item._1)})
        //        查看是否哪些特殊特征需要过滤
        //            .filter( item => item._1 > 400)

        //        val sum = paisRdd.map( n => n*(n-1)/2).sum()
        //        val largeArr = paisRdd.top(50)
        //        val resultStr = sum+" "+largeArr.mkString(" ")
        //        val result = sc.parallelize(largeArr, 1)
        //        result.saveAsTextFile(outputPath)
        // 对所有昵称长度按从小到大排
        val notRdd = paisRdd.map(item => {
            val n = item._2.split(" ").length
            (n, item._1, item._2)
        })
            .filter(item => item._1 > 400)
        var timestamp: Long = System.currentTimeMillis / 1000
        notRdd.coalesce(1, false).saveAsTextFile("/tmp/ellisonliu/spark/result_" + timestamp)

        val resultRdd = paisRdd.flatMap(item => {
            var it = ListBuffer[String]()
            val elementArrs = item._2.split(" ")

            val arrs = new ListBuffer[(String, String)]()
            for (i <- 0 to elementArrs.length - 1) {
                val element = elementArrs(i).split(":")
                if (element.length == 2)
                    arrs += Tuple2(element(0), element(1))
            }

            if (arrs.length < 400) {
                for (i <- 0 to arrs.length - 2) {
                    breakable {
                        for (j <- i + 1 to arrs.length - 1) {
                            /*
                         两两字符串比较条件：
                         1.两两字符串长度差异不能超过3
                         2.字符不能同时为数字
                         3.两两都是英文字母数字时长度必需要大于5
                         */
                            val nickname1 = arrs(i)._2
                            val nickname1length = nickname1.length
                            val nickname2 = arrs(j)._2
                            val nickname2length = nickname2.length

                            //如果两个字符之间已经差距edDiff字符以上，就不用再往后，两两计算相似度了
                            if ((nickname2length - nickname1length).abs > edDiff) {
                                break()
                            }
                            //字符不能同时为数字
                            if (isAllDigits(nickname1) && isAllDigits(nickname2)) {
                                break()
                            }

                            //都是英文字母数字时长度必需要大于5
                            if (isAllLetters(nickname1) && isAllLetters(nickname2)) {
                                if (nickname1length <= 5 && nickname2length <= 5)
                                    break()
                            }

                            if ((nickname2length - nickname1length).abs < 3) {
                                val ed = editDist(nickname1, nickname2)
                                if (ed <= edDiff) {
                                    val pairs = arrs(i)._1 + "\t" + arrs(i)._2 + "\t" + arrs(j)._1 + "\t" + arrs(j)._2 + " " + ed.toString
                                    it += pairs
                                }
                            }
                        }
                    }
                }
            }
            it
        })
        timestamp = System.currentTimeMillis / 1000
        resultRdd.coalesce(50, true).saveAsTextFile("/tmp/ellisonliu/spark/result_" + timestamp)

        sc.stop()
    }
}
