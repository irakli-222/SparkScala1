package NewPack

import java.io.FileWriter


object NewTT {
  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.SparkContext
  import org.apache.spark.sql.functions._
  import java.sql.Connection
  import java.sql.DriverManager
  import java.sql.ResultSet
  import java.sql.SQLException
  import java.sql.Statement
  import java.text.SimpleDateFormat
  import java.util.Locale





    def main(args : Array[String]): Unit ={
      val sc = new SparkContext("local[*]" , "ScalaProj2")

      import org.apache.spark.sql.SparkSession
      val spark = SparkSession
        .builder()
        .appName("ScalaProj2")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()
      import spark.implicits._

      val format = new SimpleDateFormat("MM-dd-yyyy")

      val date_from ="2020-07-01"
      val date_to   ="2020-10-22"

      //      Logger.getRootLogger.setLevel(Level.OFF)

      val clientData = spark.sqlContext.read.format("jdbc")
        .options(
          Map(
            "url" -> "jdbc:sqlite:src\\other\\bank.db",
            "dbtable" -> "clients")).load().filter(to_date(col("inp_date"),"MM/dd/yyyy").between(date_from, date_to))

      clientData.show()

      val currencyData = spark.sqlContext.read.format("jdbc")
        .options(
          Map(
            "url" -> "jdbc:sqlite:src\\other\\bank.db",
            "dbtable" -> "currency")).load()

      currencyData.show()
      //currency  transactions clients


      val transactionsData = spark.sqlContext.read.format("jdbc")
        .options(
          Map(
            "url" -> "jdbc:sqlite:src\\other\\bank.db",
            "dbtable" -> "transactions"))
            .load()
            .select(col ("IBAN").as("IBAN_T"),col("Amount"),col("CurrencyId"),col("inp_date"))
            .filter(to_date(col("inp_date"),"MM/dd/yyyy")
            .between(date_from, date_to))

      transactionsData.show()



      val transAggr = transactionsData.join(clientData, transactionsData("IBAN_T") ===  clientData("IBAN"),  "inner")
                      .groupBy($"IBAN",$"FirstName",$"LastName").agg(round(avg($"Amount"),2) as "avg_amount")
      transAggr.show()

//      tableData.select(to_date(col("inp_date"),"MM/dd/yyyy").as("inp_date")).show()
//      tableData.withColumn("FirstName",to_date(col("inp_date"),"MM/dd/yyyy").as("inp_date") )
//      tableData.show()


    //        val filteredData = tableData
    //          .select(tableData("FirstName"),  to_date(col("inp_date"),"MM/dd/yyyy").alias("date"))
    //          .filter($"date".between(date_from, date_to))
    //

      transAggr.write.format("csv").save("src\\other\\newoutput")




////////////////////////////////////////
//      val url = "jdbc:sqlite:D:\\ScalaProj2\\src\\other\\bank.db"
//      val conn = DriverManager.getConnection(url)
//      val statement = conn.createStatement()
//      val rs =  statement.executeQuery("select * from clients");
//      val fw = new FileWriter("D:\\scalaProj2\\src\\other\\newoutput.txt", true) ;
//      while (rs.next) {
//        val colval = rs.getString("FirstName")+"\n"
//        println(colval)
//        fw.write(colval) ;
//      }
//      fw.close()
//      conn.close()
////////////////////////////////////////

//      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//      val myTableNames = metaData.select("clients").map(_.getString(0)).collect()
//
//      for (t <- myTableNames) {
//        println(t.toString)
//
//        val tableData = sqlContext.read.format("jdbc")
//          .options(
//            Map(
//              "url" -> "jdbc:sqlite:/D:\\ScalaProj2\\src\\other\\bank.db",
//              "dbtable" -> t)).load()
//
//        tableData.show()
//      }


//      val lines = sc.textFile("D:\\scalaProj2\\src\\other\\test.txt");
//      val words = lines.flatMap(line => line.split(' '))
//      val wordsKVRdd = words.map(x => (x,1))
//      val count = wordsKVRdd.reduceByKey((x,y) => x + y).map(x => (x._2,x._1)).sortByKey(false).map(x => (x._2, x._1)).take(10)
//      count.foreach(println)
//        lines.foreach(println)
//        words.saveAsTextFile("D:\\scalaProj2\\src\\other\\newoutput.txt")
//
        sc.stop()



    }

}
