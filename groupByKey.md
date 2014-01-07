# 不要过早使用 groupByKey
一开始就把数据 groupByKey, 后续处理会方便很多，例如如下用24行代码就实现了一个朴素贝叶斯分类器(Naive Bayes classifer)：

	/**
     * Naive bayes classifer for descrete data.
     * @param C number of labels.
     * @param D dimension of feture vectors.
     * @param data trainning data.
	def run(C: Int, D: Int, data: RDD[LabeledPoint]): NaiveBayesModel = {
	  val groupedData = data.map(p => p.label.toInt -> p.features).groupByKey()
	 
	  val countPerLabel = groupedData.mapValues(_.size)
	  val logDenominator = math.log(data.count() + C * lambda)
	  val weightPerLabel = countPerLabel.mapValues {
	    count => math.log(count + lambda) - logDenominator
	  }
	 
	  val summedObservations = groupedData.mapValues(_.reduce {
	    (lhs, rhs) => lhs.zip(rhs).map(pair => pair._1 + pair._2)
	  })
	 
	  val weightsMatrix = summedObservations.mapValues { weights =>
	    val sum = weights.sum
	    val logDenom = math.log(sum + D * lambda)
	    weights.map(w => math.log(w + lambda) - logDenom)
	  }
	 
	  val labelWeights = weightPerLabel.collect().sorted.map(_._2)
	  val weightsMat = weightsMatrix.collect().sortBy(_._1).map(_._2)
	 
	  new NaiveBayesModel(labelWeights, weightsMat)
	}

这样的确代码简化很多，但是性能不好，因为 `groupByKey` 会导致数据在多套机器之间 shuffle，网络IO会很多，程序整体性能反而更慢。

正确的做法是，先尽可能的reduce数据，在shuffle。

如下是一个高效的朴素贝叶斯实现：

	/**
	 * Run the algorithm with the configured parameters on an input RDD of LabeledPoint entries.
	 *
	 * @param data RDD of (label, array of features) pairs.
	 */
	def run(data: RDD[LabeledPoint]) = {
	  // Aggregates all sample points to driver side to get sample count and summed feature vector
	  // for each label.  The shape of `zeroCombiner` & `aggregated` is:
	  //
	  //    label: Int -> (count: Int, featuresSum: DoubleMatrix)
	  val zeroCombiner = mutable.Map.empty[Int, (Int, DoubleMatrix)]
	  val aggregated = data.aggregate(zeroCombiner)({ (combiner, point) =>
	    point match {
	      case LabeledPoint(label, features) =>
	        val (count, featuresSum) = combiner.getOrElse(label.toInt, (0, DoubleMatrix.zeros(1)))
	        val fs = new DoubleMatrix(features.length, 1, features: _*)
	          combiner += label.toInt -> (count + 1, featuresSum.addi(fs))
	    }
	  }, { (lhs, rhs) =>
	    for ((label, (c, fs)) <- rhs) {
	      val (count, featuresSum) = lhs.getOrElse(label, (0, DoubleMatrix.zeros(1)))
	      lhs(label) = (count + c, featuresSum.addi(fs))
	    }
	    lhs
	  })
	
	  // Kinds of label
	  val C = aggregated.size
	  // Total sample count
	  val N = aggregated.values.map(_._1).sum
	
	  val pi = new Array[Double](C)
	  val theta = new Array[Array[Double]](C)
	  val piLogDenom = math.log(N + C * lambda)
	
	  for ((label, (count, fs)) <- aggregated) {
	    val thetaLogDenom = math.log(fs.sum() + fs.length * lambda)
	    pi(label) = math.log(count + lambda) - piLogDenom
	    theta(label) = fs.toArray.map(f => math.log(f + lambda) - thetaLogDenom)
	  }
	
	  new NaiveBayesModel(pi, theta)
	}

这个代码，实际上就是我和@连城的拙作，已被merge，见 [standard Naive Bayes classifier](https://github.com/apache/incubator-spark/pull/292)，代码量比上一个版本多了一点点，不过性能上却又巨大提升。它是先把数据预处理，即进行reduce，然后再 groupByKey 一下。
