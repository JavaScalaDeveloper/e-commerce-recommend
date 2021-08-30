package com.huat.huangjiahao


import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}
import org.jblas.DoubleMatrix


case class ProductRating(userId: Int, productId: Int, score: Double, timestamp: BigInt)

case class MongoConfig(uri: String, db: String)

// 定义标准推荐对象
case class Recommendation(productId: Int, score: Double)

// 定义用户的推荐列表
case class UserRecs(userId: Int, recs: Seq[Recommendation])

// 定义商品相似度列表
case class ProductRecs(productId: Int, recs: Seq[Recommendation])

object OfflineRecommender {
  def printFoZu() = {
    System.out.println("\033[92;1m.....................南无阿弥陀佛......................" + ".................南无释迦牟尼佛......................" + "..............南无药师琉璃光王佛......................");
    System.out.println("\033[92;0m                       _oo0oo_                      " + "                       _oo0oo_                      " + "                       _oo0oo_                      ");
    System.out.println("\033[92;0m                      o8888888o                     " + "                      o8888888o                     " + "                      o8888888o                     ");
    System.out.println("\033[92;0m                      88\" . \"88                     " + "                      88\" . \"88                     " + "                      88\" . \"88                     ");
    System.out.println("\033[92;0m                      (| -_- |)                     " + "                      (| -_- |)                     " + "                      (| -_- |)                     ");
    System.out.println("\033[92;0m                      0\\  =  /0                     " + "                      0\\  =  /0                     " + "                      0\\  =  /0                     ");
    System.out.println("\033[92;0m                    ___/‘---’\\___                   " + "                    ___/‘---’\\___                   " + "                    ___/‘---’\\___                   ");
    System.out.println("\033[92;0m                  .' \\|       |/ '.                 " + "                  .' \\|       |/ '.                 " + "                  .' \\|       |/ '.                 ");
    System.out.println("\033[92;0m                 / \\\\|||  :  |||// \\                " + "                 / \\\\|||  :  |||// \\                " + "                 / \\\\|||  :  |||// \\                ");
    System.out.println("\033[92;0m                / _||||| -卍-|||||_ \\               " + "                / _||||| -卍-|||||_ \\               " + "                / _||||| -卍-|||||_ \\               ");
    System.out.println("\033[92;0m               |   | \\\\\\  -  /// |   |              " + "               |   | \\\\\\  -  /// |   |              " + "               |   | \\\\\\  -  /// |   |              ");
    System.out.println("\033[92;0m               | \\_|  ''\\---/''  |_/ |              " + "               | \\_|  ''\\---/''  |_/ |              " + "               | \\_|  ''\\---/''  |_/ |              ");
    System.out.println("\033[92;0m               \\  .-\\__  '-'  ___/-. /              " + "               \\  .-\\__  '-'  ___/-. /              " + "               \\  .-\\__  '-'  ___/-. /              ");
    System.out.println("\033[92;0m             ___'. .'  /--.--\\  '. .'___            " + "             ___'. .'  /--.--\\  '. .'___            " + "             ___'. .'  /--.--\\  '. .'___            ");
    System.out.println("\033[92;0m          .\"\" ‘<  ‘.___\\_<|>_/___.’ >’ \"\".          " + "          .\"\" ‘<  ‘.___\\_<|>_/___.’ >’ \"\".          " + "          .\"\" ‘<  ‘.___\\_<|>_/___.’ >’ \"\".          ");
    System.out.println("\033[92;0m         | | :  ‘- \\‘.;‘\\ _ /’;.’/ - ’ : | |        " + "         | | :  ‘- \\‘.;‘\\ _ /’;.’/ - ’ : | |        " + "         | | :  ‘- \\‘.;‘\\ _ /’;.’/ - ’ : | |        ");
    System.out.println("\033[92;0m         \\  \\ ‘_.   \\_ __\\ /__ _/   .-’ /  /        " + "         \\  \\ ‘_.   \\_ __\\ /__ _/   .-’ /  /        " + "         \\  \\ ‘_.   \\_ __\\ /__ _/   .-’ /  /        ");
    System.out.println("\033[92;0m     =====‘-.____‘.___ \\_____/___.-’___.-’=====     " + "     =====‘-.____‘.___ \\_____/___.-’___.-’=====     " + "     =====‘-.____‘.___ \\_____/___.-’___.-’=====     ");
    System.out.println("\033[92;0m                       ‘=---=’                      " + "                       ‘=---=’                      " + "                       ‘=---=’                      ");
    System.out.println("\033[92;1m...................佛祖开光 ,永无BUG..................." + "................佛祖保佑 ,永不宕机..................." + "...............佛祖开光 ,降低延迟...................");
  }

  // 定义mongodb中存储的表名
  val MONGODB_RATING_COLLECTION = "Rating"

  val USER_RECS = "UserRecs"
  val PRODUCT_RECS = "ProductRecs"
  val USER_MAX_RECOMMENDATION = 20


  def main(args: Array[String]): Unit = {
    printFoZu()
    for (n <- 1 to Integer.MAX_VALUE) {
      System.err.println("正在刷新第 " + n + " 次推荐结果...")
      run()
      System.out.println("\n完成第 " + n + " 次推荐结果刷新，可短期内查看推荐结果！")
    }
  }


  def run(): Unit = {
    val startTime1: Long = System.currentTimeMillis()
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://linux:27017/recommender",
      "mongo.db" -> "recommender"
    )
    // 创建一个spark config
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")
    // 创建spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加载数据
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .rdd
      .map(rating => (rating.userId, rating.productId, rating.score)).cache()

    // 提取出所有用户和商品的数据集
    val userRDD = ratingRDD.map(_._1).distinct()
    val productRDD = ratingRDD.map(_._2).distinct()

    // 核心计算过程
    // 1. 训练隐语义模型
    val trainData = ratingRDD.map(x => Rating(x._1, x._2, x._3))
    // 定义模型训练的参数，rank隐特征个数，iterations迭代词数，lambda正则化系数
    val (rank, iterations, lambda) = (5, 10, 0.01)
    val model = ALS.train(trainData, rank, iterations, lambda)

    // 2. 获得预测评分矩阵，得到用户的推荐列表
    // 用userRDD和productRDD做一个笛卡尔积，得到空的userProductsRDD表示的评分矩阵
    val userProducts = userRDD.cartesian(productRDD)
    val preRating = model.predict(userProducts)

    // 从预测评分矩阵中提取得到用户推荐列表
    val userRecs = preRating.filter(_.rating > 0)
      .map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()
      .map { case (userId, recs) => UserRecs(userId, recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2))) }
      .toDF()


    // 3. 利用商品的特征向量，计算商品的相似度列表
    val productFeatures = model.productFeatures.map {
      case (productId, features) => (productId, new DoubleMatrix(features))
    }
    // 两两配对商品，计算余弦相似度
    val productRecs = productFeatures.cartesian(productFeatures)
      .filter { case (a, b) => a._1 != b._1 }
      // 计算余弦相似度
      .map { case (a, b) => val simScore = consinSim(a._2, b._2)
      (a._1, (b._1, simScore))
    }
      .filter(_._2._2 > 0.4)
      .groupByKey()
      .map { case (productId, recs) => ProductRecs(productId, recs.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2))) }
      .toDF()
    val userResult: DataFrameWriter[Row] = userRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
    val productResulst: DataFrameWriter[Row] = productRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", PRODUCT_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
    //    Thread.sleep(20000)
    val startTime2: Long = System.currentTimeMillis()
    userResult.save()
    val endTime: Long = System.currentTimeMillis()
    productResulst.save()
    print("\n推荐展示时间：" + (startTime2 - startTime1) + "毫秒，这期间系统在根据用户的实时评分行为重新计算推荐结果。\n" + "更新推荐结果延迟：" + (endTime - startTime2) + "毫秒，这期间系统在将更新后的推荐结果覆盖写入到数据库。")
    spark.stop()
  }


  def consinSim(product1: DoubleMatrix, product2: DoubleMatrix): Double = {
    product1.dot(product2) / (product1.norm2() * product2.norm2())
  }
}
