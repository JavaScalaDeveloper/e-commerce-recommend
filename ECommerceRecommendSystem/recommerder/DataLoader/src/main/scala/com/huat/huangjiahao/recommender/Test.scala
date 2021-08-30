package com.huat.huangjiahao.recommender


class Test {
/*
  // 定义一个HashingTF工具
  val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(200)

  // 用 HashingTF 做处理
  val featurizedData = hashingTF.transform(wordsData)

  // 定义一个IDF工具
  val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")

  // 将词频数据传入，得到idf模型（统计文档）
  val idfModel = idf.fit(featurizedData)

  // 用tf-idf算法得到新的特征矩阵
  val rescaledData = idfModel.transform(featurizedData)

  // 从计算得到的 rescaledData 中提取特征向量
  val productFeatures = rescaledData.map {
    case row => (row.getAs[Int]("productId"), row.getAs[SparseVector]("features").toArray)
  }
    .rdd.map(x => {
    (x._1, new DoubleMatrix(x._2))
  })
  */

}
