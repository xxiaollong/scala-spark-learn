package com.example.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  *
  */
object SparkSQL {

  /**
    * 读取JSON文件
    *
    */
  def readJson(spark: SparkSession): Unit ={

    val df: DataFrame = spark.read.json("E:/Spark/user_json.txt")
    df.show()

  }

  def main(args: Array[String]): Unit = {
    // 配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")

    // 上下文对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    readJson(spark)

    // 关闭资源
    spark.stop()
  }

}
