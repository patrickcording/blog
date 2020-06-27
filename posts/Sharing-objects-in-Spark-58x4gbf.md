---
title: Sharing objects in Spark
date: 2020-06-27T21:56:34.472089Z

tags: Scala, Spark
---

When running a Spark job, the driver will create tasks and send them to executors on the worker nodes. The task contains the information needed for executor to run except the actual data it is processing. Before sending the task it is serialized. If an object is referenced from the code that defines the transformation of the data, then the object needs to be sent along with the task. If the object it is not serializable, you will get the following error.

```
org.apache.spark.SparkException: Task not serializable
```

You may solve this by making the class serializable but if the class is defined in a third-party library this is a demanding task.

This post describes when and how to avoid sending objects from the master to the workers. To do this we will use the following running example.

```
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.sql.SparkSession
import scala.util.Random

class Test { // add extends Serializable to make the example work
   println(s"Creating Test instance on ${SparkEnv.get.executorId} for partition ${TaskContext.getPartitionId()}")
   def print(): Unit = println(s"${SparkEnv.get.executorId} is invoking method on Test for partition ${TaskContext.getPartitionId()}")
}

object SparkSharingObjectExample1 {
   def main(implicit args: Array[String]): Unit = {
      val spark = SparkSession.builder.getOrCreate()
      import spark.implicits._

      val data = (1 to 10000).iterator.map(_ => Random.nextInt(10000)).toList.toDF("c0").repartition(4)

      val test = new Test()

      data.foreachPartition(_ => test.print())
   }
}
```

If you want to run the example locally, you need to run Spark in standalone mode. You will not get the desired results if you run it locally in `local[*]` mode because this spawns both the driver and executors in the same JVM.

Besides making the `Test` class serializable, we have the following three other options, which we will discuss in the rest of this post:

1. Create one instance per executor
2. Create one instance per partition of the data frame
3. Create one instance per row in the data frame

## **One instance per executor**

An executor is a process running in its own JVM instance on a worker node. If we create an instance of a class in an executor it will be shared between the tasks handled by that executor. This is useful if we want to create a connection pool.

To create one instance per executor we need to instantiate the class in an object that is outside of the scope of the transformation code. The Scala `object` is equivalent to a static class in Java. The code for the `object` is executed when the executor is first started, so all its contents is created in the executor and there's no need for serializing and sending objects between the driver and executors. An example is seen below.

```
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.sql.SparkSession
import scala.util.Random

class Test {
   println(s"Creating Test instance on ${SparkEnv.get.executorId} for partition ${TaskContext.getPartitionId()}")
   def print(): Unit = println(s"${SparkEnv.get.executorId} is invoking method on Test for partition ${TaskContext.getPartitionId()}")
}

object Test {
   println(s"Initializing static Test instance on ${SparkEnv.get.executorId}")
   val instance = new Test()
}

object SparkSharingObjectExample2 {
   def main(implicit args: Array[String]): Unit = {
      val spark = SparkSession.builder.getOrCreate()
      import spark.implicits._

      val data = (1 to 10000).iterator.map(_ => Random.nextInt(10000)).toList.toDF("c0").repartition(4)

      data.foreachPartition(_ => Test.instance.print())
   }
}
```

With this approach the instance is shared among threads in the executor and therefore needs to be thread-safe and non-blocking.

## **One instance per partition**

Sparks RDD API offers the methods `foreachPartition` and `mapPartition` which lets you iterate or transform, respectively, the partitions of your data frame instead of each row. A partition can be seen as a chunk of work, and if we create an instance in the scope of the aforementioned methods, that instance will only be available while processing that chunk of work. Below is an example.

```
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.sql.SparkSession
import scala.util.Random

class Test {
   println(s"Creating Test instance on ${SparkEnv.get.executorId} for partition ${TaskContext.getPartitionId()}")
   def print(): Unit = println(s"${SparkEnv.get.executorId} is invoking method on Test for partition ${TaskContext.getPartitionId()}")
}

object SparkSharingObjectExample3 {
   def main(implicit args: Array[String]): Unit = {
      val spark = SparkSession.builder.getOrCreate()
      import spark.implicits._

      val data = (1 to 10000).iterator.map(_ => Random.nextInt(10000)).toList.toDF("c0").repartition(4)

      data.foreachPartition(_ => {
         val test = new Test()
         test.print()
      })
   }
}
```

Since a partition is handled by one thread, this approach has the advantage that the object does not have to be thread-safe and we avoid the overhead from creating one object per row in the data frame.

## **One instance per row**

This is your last resort and should only be used if data from a row is needed in the initialization of the instance. Your application will suffer from performance issues if you decide on this approach so maybe you want to consider if one of the two other options can be used.

---

## **References**

* [Basic overview of Spark and glossary](https://spark.apache.org/docs/latest/cluster-overview.html)

---

_This post first appeared January 12, 2019 on Medium._