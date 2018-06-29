package org.apache.gettingstartedkudu.examples

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._
import org.apache.kudu.spark.kudu._
import org.apache.kudu.client._
import org.apache.spark.sql.SparkSession

/**
  * Spark on Kudu Example
  *
  * Performs insert, update and scan operations on a Kudu table
  * in Spark.
  *
  */
object SparkKuduGettingStarted {

  // Case class defined with the same schema (column names and types) as the
  // Kudu table we will be writing into
  // Define your case class *outside* your main method
  case class Customer(name:String, age:Int, city:String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Spark on Kudu Getting Started")
      .enableHiveSupport().getOrCreate()

    // Comma-separated list of Kudu masters with port numbers
    val master1 = "mladen-secure-kudu-8.gce.cloudera.com"
    val master2 = "mladen-secure-kudu-9.gce.cloudera.com"
    val master3 = "mladen-secure-kudu-10.gce.cloudera.com"
    val kuduMasters = Seq(master1, master2, master3).mkString(",")

    // Create an instance of a KuduContext
    val kuduContext = new KuduContext(kuduMasters, spark.sparkContext)

    // This allows us to implicitly convert RDD to DataFrame
    import spark.implicits._

    // Specify a table name
    var kuduTableName = "spark_kudu_tbl"

    // Define Kudu options used by various operations
    val kuduOptions: Map[String, String] = Map(
      "kudu.table"  -> kuduTableName,
      "kudu.master" -> kuduMasters)

    ///
    /// TABLE EXISTS AND DROP
    ///

    // Check if the table exists, and drop it if it does
    if (kuduContext.tableExists(kuduTableName)) {
      kuduContext.deleteTable(kuduTableName)
    }

    ///
    /// CREATE TABLE
    ///

    // 1. Give your table a name
    kuduTableName = "spark_kudu_tbl"

    // 2. Define a schema
    val kuduTableSchema = StructType(
        //        column name   type       nullable
        StructField("name", StringType , false) ::
        StructField("age" , IntegerType, true ) ::
        StructField("city", StringType , true ) :: Nil)

    // 3. Define the primary key
    val kuduPrimaryKey = Seq("name")

    // 4. Specify any further options
    val kuduTableOptions = new CreateTableOptions()
    kuduTableOptions.
      setRangePartitionColumns(List("name").asJava).
      setNumReplicas(3)

    // 5. Call create table API
    kuduContext.createTable(
      // Table name, schema, primary key and options
      kuduTableName, kuduTableSchema, kuduPrimaryKey, kuduTableOptions)

    ///
    /// WRITING TO TABLE - KUDU CONTEXT
    ///

    // Ideally, we prepare the set of content we want to write to the kudu
    // table by preparing a DataFrame with content to be written.
    //
    // DataFrames can be constructed from structured data files, Hive tables,
    // external databases, or existing RDDs.
    //
    // For the sake of simplicity, we will create a simple RDD, then
    // convert it into a DataFrame which we will use to write to the table.

    // Define a list of customers based on the case class already defined above
    val customers = Array(
      Customer("jane", 30, "new york"),
      Customer("jordan", 18, "toronto"))

    // Create RDD out of the customers Array
    val customersRDD = spark.sparkContext.parallelize(customers)

    // Now, using reflection, this RDD can easily be converted to a DataFrame
    // Ensure to do the :
    //     import sqlContext.implicits._
    // above to have the toDF() function available to you
    val customersDF = customersRDD.toDF()

    ///
    /// INSERT DATA
    ///

    // 1. Specify your Kudu table name
    kuduTableName = "spark_kudu_tbl"

    // 2. Insert our customer DataFrame data set into the Kudu table
    kuduContext.insertRows(customersDF, kuduTableName)

    // 3. Read back the records from the Kudu table to see them dumped
    spark.read.options(kuduOptions).kudu.show

    ///
    /// DELETE DATA - KUDU CONTEXT
    ///

    // 1. Specify your Kudu table name
    kuduTableName = "spark_kudu_tbl"

    // 2. Let’s register our customer dataframe as a temporary table so we
    // refer to it in Spark SQL
    customersDF.createOrReplaceTempView("customers")

    // 3. Filter and create a keys-only DataFrame to be deleted from our table
    val deleteKeysDF = spark.sql("select name from customers where age > 20")

    // 4. Delete the rows from our Kudu table
    kuduContext.deleteRows(deleteKeysDF, kuduTableName)

    // 5. Read data from Kudu table
    spark.read.options(kuduOptions).kudu.show

    ///
    /// UPSERT DATA - KUDU CONTEXT
    ///

    // 1. Specify your Kudu table name
    kuduTableName = "spark_kudu_tbl"

    // 2. Define the dataset we want to upsert
    val newAndChangedCustomers = Array(
      Customer("michael", 25, "chicago"),
      Customer("denise" , 43, "winnipeg"),
      Customer("jordan" , 19, "toronto"))

    // 3. Create our dataframe
    val newAndChangedRDD = spark.sparkContext.parallelize(newAndChangedCustomers)
    val newAndChangedDF  = newAndChangedRDD.toDF()

    // 4. Call upsert with our new and changed customers DataFrame
    kuduContext.upsertRows(newAndChangedDF, kuduTableName)

    // 5. Show contents of Kudu table
    spark.read.options(kuduOptions).kudu.show

    ///
    /// UPDATE DATA - KUDU CONTEXT
    ///

    // 1. Specify your Kudu table name
    kuduTableName = "spark_kudu_tbl"

    // 2. Create a DataFrame of updated rows
    val modifiedCustomers = Array(Customer("michael", 25, "toronto"))

    val modifiedCustomersRDD = spark.sparkContext.parallelize(modifiedCustomers)
    val modifiedCustomersDF  = modifiedCustomersRDD.toDF()

    // 3. Call update with our new and changed customers DataFrame
    kuduContext.updateRows(modifiedCustomersDF, kuduTableName)

    // 4. Show contents of Kudu table
    spark.read.options(kuduOptions).kudu.show

    ///
    /// READING FROM TABLE : NATIVE RDD
    ///

    // We can read from our table by simply making an RDD, which will,
    // under the covers make use of the native kudu reader, rather than
    // an input format typically used in MapReduce jobs

    // 1. Specify a table name
    kuduTableName = "spark_kudu_tbl"

    // 2. Specify the columns you want to project
    val kuduTableProjColumns = Seq("name", "age")

    // 3. Read table, represented now as RDD
    val custRDD = kuduContext.kuduRDD(spark.sparkContext, kuduTableName,
      kuduTableProjColumns)

    // We get a RDD[Row] coming back to us. Lets send through a map to pull
    // out the name and age into the form of a tuple
    val custTuple = custRDD.map { case Row(name: String, age: Int) => (name, age) }

    // Print it on the screen just for fun
    custTuple.collect().foreach(println(_))

    ///
    /// READING FROM TABLE : DATA FRAME
    ///

    // Read our table into a DataFrame - reusing kuduOptions specified
    // above
    val customerReadDF = spark.read.options(kuduOptions).kudu

    // Show our table to the screen.
    customerReadDF.show()


    ///
    /// WRITING TO TABLE - DATAFRAME
    ///

    // We create a DataFrame, and this time write to the table using
    // the DataFrame API directly, treating the kudu table as a data source.

    // Create a small dataset to write (append) to the Kudu table
    val customersAppend = Array(
      Customer("bob", 30, "boston"),
      Customer("charlie", 23, "san francisco"))

    // Create our DataFrame our of our dataset
    val customersAppendDF = spark.sparkContext.parallelize(customersAppend).toDF()

    // Specify the table name
    kuduTableName = "spark_kudu_tbl"

    // Call the write method on our DataFrame directly in "append" mode
    customersAppendDF.write.options(kuduOptions).mode("append").kudu

    // See results of our append
    spark.read.options(kuduOptions).kudu.show()

    ///
    /// Spark SQL INSERT
    ///

    // Quickly prepare a Kudu table we will use as our source table in Spark
    // SQL.
    // First, some sample data
    val srcTableData = Array(
      Customer("enzo", 43, "oakland"),
      Customer("laura", 27, "vancouver"))

    // Create our DataFrame
    val srcTableDF = spark.sparkContext.parallelize(srcTableData).toDF()

    // Register our source table
    srcTableDF.createOrReplaceTempView("source_table")

    // Specify Kudu table name we will be inserting into
    kuduTableName = "spark_kudu_tbl"

    // Register your table as a Spark SQL table.
    // Remember that kuduOptions stores the kuduTableName already as well as
    // the list of Kudu masters.
    spark.read.options(kuduOptions).kudu.registerTempTable(kuduTableName)

    // Use Spark SQL to INSERT (treated as UPSERT by default) into Kudu table
    spark.sql(s"INSERT INTO TABLE $kuduTableName SELECT * FROM source_table")

    // See results of our insert
    spark.read.options(kuduOptions).kudu.show()

    ///
    /// Predicate pushdown
    ///

    // Kudu table name
    kuduTableName = "spark_kudu_tbl"

    // Register Kudu table as a Spark SQL temp table
    spark.read.options(kuduOptions).kudu.
      registerTempTable(kuduTableName)

    // Now refer to that temp table name in our Spark SQL statement
    val customerNameAgeDF = spark.
      sql(s"""SELECT name, age FROM $kuduTableName WHERE age >= 30""")

    // Show the results
    customerNameAgeDF.show()
  }
}
