# RDD的API所引用的所有对象，都必须是可序列化的

在RDD的API里所引用的所有对象，都必须是可序列化的，因为RDD分布在多台机器是，代码和所引用的对象会序列化，然后复制到多台机器，所以凡是被引用的数据，都必须是可序列化的。

例如，如下代码，就会报 `java.lang.NotSerializableException: scala.util.Random` 异常：

	val rnd = new Random()
	rdd.mapPartition { x=>
	  // ...
	  val i = rnd.nextInt(10)
	  // ...
	}

因为`mapPartition`里引用了`rnd`, 而`Random`对象没有继承自`Serialize`，是不可序列化的，所以会报异常。把 `val rnd = new Random()` 移动到 `mapPartion()`里面才行。