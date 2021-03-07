package NewPack


object NewTT {
  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.SparkContext
  import org.apache.spark.sql.functions._


  def newETL(dateFrom: String, dateTo: String): String ={
//    def main(args : Array[String]): Unit ={
      val sc = new SparkContext("local[*]" , "ScalaProj2")
      import org.apache.spark.sql.SparkSession
      val spark = SparkSession
        .builder()
        .appName("ScalaProj2")
        .master("local")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()
      import spark.implicits._



      val date_from =dateFrom
      val date_to   =dateTo


//    put client data from Database into DataFrame
      val clientData = spark.sqlContext.read.format("jdbc")
        .options(
          Map(
            "url" -> "jdbc:sqlite:src\\other\\bank.db",
            "dbtable" -> "clients")).load()//.filter(to_date(col("effect_date"),"MM/dd/yyyy").between(date_from, date_to))



//    put currency data from Database
      val currencyData = spark.sqlContext.read.format("jdbc")
        .options(
          Map(
            "url" -> "jdbc:sqlite:src\\other\\bank.db",
            "dbtable" -> "currency")).load()



//    put transactions' data from Database into DataFrame
      val transactionsData = spark.sqlContext.read.format("jdbc")
        .options(
          Map(
            "url" -> "jdbc:sqlite:src\\other\\bank.db",
            "dbtable" -> "transactions"))
            .load()
            .select(col ("IBAN").as("IBAN_T"),col("Amount"),col("CurrencyId"),col("inp_date"),col("ID"))
            .filter(to_date(col("inp_date"),"MM/dd/yyyy")
            .between(date_from, date_to))


//    calculate counts for IBANs from transactions - will use in next operation
      val transactionsCountByIban = transactionsData
        .groupBy($"IBAN_T").agg(count($"ID") as "transaction_count")


//    create new dataframe from all joined DFs
      val allTransactionData = transactionsData.as("t")
        .join(clientData.as("c"), transactionsData("IBAN_T") ===  clientData("IBAN"),  "inner")
        .join(currencyData.as("ccy"), transactionsData("CurrencyID") === currencyData("ID"), "inner")
        .join(transactionsCountByIban.as("cnt"),transactionsCountByIban("IBAN_T") ===transactionsData("IBAN_T") ,  "inner")
        .select($"t.inp_date", $"t.IBAN_T", $"t.AMOUNT", $"ccy.CCYFrom", $"ccy.rate", $"cnt.transaction_count", $"c.FirstName",$"c.LastName",$"c.Age")
        .orderBy($"t.inp_date", $"t.IBAN_T", $"ccy.CCYFrom")

      allTransactionData.show()

//    save DF to Hive
      allTransactionData.write.mode("overwrite").saveAsTable("FullTransactionData")


//    create new DF by aggregating transaction data
      val transAggr = transactionsData.join(clientData, transactionsData("IBAN_T") ===  clientData("IBAN"),  "inner")
        .groupBy($"IBAN",$"FirstName",$"LastName").agg(round(avg($"Amount"),2) as "avg_amount")
        .orderBy(desc("avg_amount"))
      transAggr.show()

//    write into CSV file
      transAggr.write.format("csv").save("spark-warehouse\\OutputCSV")

      sc.stop()
      spark.stop()

      return "successfully finished"
    }


  def main(args : Array[String]): Unit ={

//  call new function with testing values
    val testNewETL = newETL("2020-07-01","2020-10-22")
    println(testNewETL)

  }

}
