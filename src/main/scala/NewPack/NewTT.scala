package NewPack


object NewTT {
  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.SparkContext
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.expressions.Window
  import org.apache.spark.sql.DataFrame
  import org.apache.hadoop.fs.{FileSystem, Path}



  //val sc = new SparkContext("local[*]" , "ScalaProj2")
  import org.apache.spark.sql.SparkSession
  val spark = SparkSession
    .builder()
    .appName("ScalaProj2")
    .master("local")
    .config("spark.some.config.option", "some-value")
//    .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
//    .enableHiveSupport()
    .getOrCreate()
  import spark.implicits._

//  val current_time = spark.range(1).select(unix_timestamp as "current_timestamp").first().get(0).toString

  def newETL(dateFrom: String, dateTo: String): String ={


//    put client data from Database into DataFrame
      val clientData = spark.sqlContext.read.format("jdbc")
        .option("driver", "org.sqlite.JDBC")
        .options(
          Map(
            "url" -> "jdbc:sqlite:src\\other\\bank.db",
            "dbtable" -> "clients")).load().cache()



//    put currency data from Database
      val currencyData = spark.sqlContext.read.format("jdbc")
        .option("driver", "org.sqlite.JDBC")
        .options(
          Map(
            "url" -> "jdbc:sqlite:src\\other\\bank.db",
            "dbtable" -> "currency")).load().cache()



//    put transactions' data from Database into DataFrame
      val transactionsData = spark.sqlContext.read.format("jdbc")
        .option("driver", "org.sqlite.JDBC")
        .options(
          Map(
            "url" -> "jdbc:sqlite:src\\other\\bank.db",
            "dbtable" -> "transactions"))
            .load()
            .select(col ("IBAN").as("IBAN_T"),col("Amount"),col("CurrencyId"),col("inp_date"),col("ID"))
            .filter(to_date(col("inp_date"),"MM/dd/yyyy")
            .between(dateFrom, dateTo)).cache()



//    calling allTransactionData to create new dataframe from all passed DFs
      val allTransactionData = ProcessAllTransactions(transactionsData: DataFrame, clientData: DataFrame, currencyData: DataFrame )

      allTransactionData.show()



//    calling ProcessTransactionsAgg to create new DF by aggregating transaction data
      val transAggr = ProcessTransactionsAgg(transactionsData: DataFrame, clientData: DataFrame)
      transAggr.show()



 //     sc.stop()
      spark.stop()

      return "successfully finished"
    }

  def ProcessAllTransactions(transactionsData: DataFrame, clientData: DataFrame, currencyData: DataFrame): DataFrame = {

    //    create new dataframe from all joined DFs
    val allTransactionData = transactionsData.as("t")
      .join(clientData.as("c"), transactionsData("IBAN_T") ===  clientData("IBAN"),  "inner")
      .join(currencyData.as("ccy"), transactionsData("CurrencyID") === currencyData("ID"), "inner")
      .select($"t.inp_date", $"t.IBAN_T", $"t.AMOUNT", $"ccy.CCYFrom", $"ccy.rate", $"c.FirstName",$"c.LastName",$"c.Age")
      .withColumn("TransactionCount", count($"t.IBAN_T").over(Window.partitionBy($"t.IBAN_T")))
      .orderBy($"t.inp_date", $"t.IBAN_T", $"ccy.CCYFrom")


    //    save DF to Hive
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val srcPath=new Path("spark-warehouse\\FullTransactionData\\")
    if(fs.exists(srcPath))
      fs.delete(srcPath,true)

    allTransactionData.repartition(1).write.mode("Overwrite").saveAsTable("FullTransactionData")

    return allTransactionData
  }

  def ProcessTransactionsAgg(transactionsData: DataFrame, clientData: DataFrame): DataFrame = {
    //    create new DF by aggregating transaction data
    val transAggr = transactionsData.join(clientData, transactionsData("IBAN_T") ===  clientData("IBAN"),  "inner")
      .groupBy($"IBAN",$"FirstName",$"LastName").agg(round(avg($"Amount"),2) as "avg_amount")
      .orderBy(desc("avg_amount"))
    transAggr.show()

    //    write into CSV file
    transAggr.repartition(1).write.mode("Overwrite").format("csv").save("spark-warehouse\\"+"OutputCSV" )

    return transAggr
  }



//Main call
  def main(args : Array[String]): Unit ={

//  calling new function with test values
    val testNewETL = newETL("2020-07-01","2020-10-22")
    println(testNewETL)

//    val newdf= spark.sql("select inp_date, IBAN_T, AMOUNT, CCYFrom, TransactionCount from FullTransactionData");
//    newdf.show()

  }

}
