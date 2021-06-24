package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{

  // ST_Contains is the user defined function to check if the given set of points lie within the given rectangle
  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
    if (queryRectangle == null || queryRectangle.isEmpty() || pointString == null || pointString.isEmpty()) {
      return false
    }
    val rect_arr = queryRectangle.split(",").map(_.toDouble)
    val point_arr = pointString.split(",").map(_.toDouble)
    if (rect_arr(0) <= point_arr(0) && rect_arr(2) >= point_arr(0) && rect_arr(1) <= point_arr(1) && rect_arr(3) >= point_arr(1)) {
      return true
    }
    else {
      return false
    }
  }

  // ST_Within is the user defined function to check if the given set of points are within a given distance of each other
  def ST_Within(pointString1: String, pointString2: String, givenDistance: Double): Boolean = {
    if (pointString1 == null || pointString1.isEmpty() || pointString2 == null || pointString2.isEmpty() || givenDistance <= 0.00) {
      return false
    }
    val point1 = pointString1.split(",").map(_.toDouble)
    val point2 = pointString2.split(",").map(_.toDouble)

    // Calculate distance
    val dist = Math.sqrt(Math.pow((point1(0) - point2(0)),2) + Math.pow((point1(1) - point2(1)),2))
    if (dist <= givenDistance) {
      return true
    }
    else {
      return false
    }
  }
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=> {
      ST_Contains(queryRectangle, pointString)
    })

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=> {
      ST_Contains(queryRectangle, pointString)
    })

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=> {
      ST_Within(pointString1, pointString2, distance)
    })

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=> {
      ST_Within(pointString1, pointString2, distance)
    })
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
