# dsl-filter-(equal===)-(or ||)-(isin)-(isnull)-(and &&) -(not  !)




package pack

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object obj {
  
  def main(args:Array[String]) : Unit = {
        
   val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
   val sc = new SparkContext(conf)
   sc.setLogLevel("ERROR")
   val spark = SparkSession.builder().getOrCreate()
   import spark.implicits._
   
   
   val df = spark.read.format("csv").option("header","true")
   .load("file:///c:/data/dt.txt")
  
   println
   println("====raw data=====")
   println
   
   df.show()
   
   println
   println("====1.select id & tdate=====")
   println
   
   val sdf = df.select("id","tdate")
   
   sdf.show()
   
   println
   println("====2.catagory having Gymnastics=====")
   println
   
   val pdf = df.filter( col("catagory") === "Gymnastics")
   
   pdf.show()
    
   println
   println("====2.1.catagory(Gymnastics) but show only id and tdate=====")
   println
   
   val ndf = df.filter( col("catagory")=== "Gymnastics")
                 .select ("id","tdate")
   ndf.show()    
   
   println
   println("====3.catagory(Gymnastics) and spendby(cash)=====")
   println
                        
   val adf= df.filter(
       col("catagory") === "Gymnastics"  
       &&
       col("spendby") === "cash")
       
       adf.show()
       
  println
  println("====4.catagory as  Gymnastics or spendby as cash=====")
  println
  
   val cdf = df.filter(
                   col("catagory")
                   ==="Gymnastics" 
                   || 
                   col("spendby") 
                   ===("cash"))
   cdf.show()
   
   println
  println("====5.product contain Gymnastics=====")
  println
  
  val condf = df.filter(
      col("product") like "%Gymnastics%")

  condf.show()
  
  println
  println("====6.catagory having Gymnastics,Exercise=====")
  println
  
  val mdf = df.filter(col("catagory") isin ("Gymnastics","Exercise"))
  
  mdf.show()
  
  println
  println("====7.catagory of non Gymnastics=====")
  println
  
  val nonedf = df.filter(
      !(col("catagory")==="Gymnastics"))
  
nonedf.show()

  println
  println("====8.catagory of non Gymnastics but spendby is cash=====")
  println

val scdf = df.filter(
    !(col("catagory")==="Gymnastics") 
    && 
    col("spendby")===("cash"))

scdf.show()

 println
  println("====9.show id null=====")
  println


val nsdf = df.filter(col("id").isNull)

nsdf.show()


println
  println("====9.show id not null=====")
  println

val nsndf = df.filter(!(col("id").isNull))

nsndf.show()

println
  println("====10.show product like Gymnastics=====")
  println

val fdf = df.filter(col("product") like "%Gymnastics%")
         
    fdf.show()












   
  }
  
  
}
