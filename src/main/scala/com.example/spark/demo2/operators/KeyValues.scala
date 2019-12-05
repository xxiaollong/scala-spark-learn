package com.example.spark.demo2.operators

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}


/**
  * K-V 类型算子
  *
  */
object KeyValues {

  def main(args: Array[String]): Unit = {
    // 配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("KeyValues")
    // 上下文对象
    val sc: SparkContext = new SparkContext(sparkConf)

    //    my_partitionBy(sc)
    //    my_groupByKey(sc)
    my_reduceByKey(sc)


    sc.stop()

  }

  /**
    *
    */
  def my_reduceByKey(sc: SparkContext): Unit = {
    val array: Array[String] = Array("aa", "bb", "cc", "aa", "bb", "bb")
    val mapRdd: RDD[(String, Int)] = sc.makeRDD(array, 2).map(word => (word, 1))
    val rdd_1: RDD[(String, Int)] = mapRdd.reduceByKey(_ + _)
    rdd_1.collect().foreach(item => println(item._1 + " : " + item._2))
  }


  /**
    * groupByKey: 将相同的所有的键值对分组到一个集合序列当中，其顺序是不确定的。
    * 注意: 若一个键对应值太多，则易导致内存溢出。
    */
  def my_groupByKey(sc: SparkContext): Unit = {

    val array: Array[String] = Array("aa", "bb", "cc", "aa", "bb", "bb")
    val mapRdd: RDD[(String, Int)] = sc.makeRDD(array, 2).map(word => (word, 1))
    val rdd_1: RDD[(String, Iterable[Int])] = mapRdd.groupByKey()
    rdd_1.collect().foreach(item => println(item._1 + " : " + item._2.toList + " : " + item._2.size))

  }

  /**
    * partitionBy: partitionBy函数对RDD进行分区操作，操作的数据必须是k-v格式
    * 注意：如果原有RDD的分区器和现有分区器（partitioner）不一致，则根据分区器生成一个新的ShuffledRDD，产生shuffle操作。
    *
    */
  def my_partitionBy(sc: SparkContext): Unit = {

    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "aa"), (2, "bb"), (3, "cc"), (4, "dd")), 3)
    rdd.glom().collect().foreach(item => println(item.toList))

    // 使用HashPartitioner分区器
    val rdd_2: RDD[(Int, String)] = rdd.partitionBy(new HashPartitioner(2))
    rdd_2.glom().collect().foreach(item => println(item.toList))

    // 使用自定义分区器
    val rdd_3: RDD[(Int, String)] = rdd.partitionBy(new MyPartitioner(3))
    rdd_3.glom().collect().foreach(item => println(item.toList))

  }

}


/**
  * 自定义分区器
  */
class MyPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = {
    partitions
  }

  override def getPartition(key: Any): Int = {
    1 //代表所有数据放一个分区，但是分区个数依然是partitions，只不过其他分区无数据
  }
}