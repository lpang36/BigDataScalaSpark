package timeusage

import java.nio.file.Paths

import org.apache.spark.sql._
import org.apache.spark.sql.types._

/** Main class */
object TimeUsage {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Time Usage")
      .config("spark.master", "local")
      .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  /** Main function */
  def main(args: Array[String]): Unit = {
    timeUsageByLifePeriod()
  }

  def timeUsageByLifePeriod(): Unit = {
    val (columns, initDf) = read("/timeusage/atussum.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns, initDf)
    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    val finalDf = timeUsageGrouped(summaryDf)
    finalDf.show()
  }

  /** @return The read DataFrame along with its column names. */
  def read(resource: String): (List[String], DataFrame) = {
    val rdd = spark.sparkContext.textFile(fsPath(resource))

    val headerColumns = rdd.first().split(",").to[List]
    // Compute the schema based on the first line of the CSV file
    val schema = dfSchema(headerColumns)

    val data =
      rdd
        .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
        .map(_.split(",").to[List])
        .map(row)

    val dataFrame =
      spark.createDataFrame(data, schema)

    (headerColumns, dataFrame)
  }

  /** @return The filesystem path of the given resource */
  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString

  /** @return The schema of the DataFrame, assuming that the first given column has type String and all the others
    *         have type Double. None of the fields are nullable.
    * @param columnNames Column names of the DataFrame
    */
  def dfSchema(columnNames: List[String]): StructType = {
	  var args = Seq(StructField(columnNames(0),StringType,false))
	  columnNames.drop(0).foreach{ col => args = args:+(StructField(col,DoubleType,false)) }
	  return StructType(args)
  }


  /** @return An RDD Row compatible with the schema produced by `dfSchema`
    * @param line Raw fields
    */
  def row(line: List[String]): Row = {
	  val args = Seq(line(0))
	  line.drop(0).foreach { args+_ }
	  return Row.fromSeq(args)
  }

  /** @return The initial data frame columns partitioned in three groups: primary needs (sleeping, eating, etc.),
    *         work and other (leisure activities)
    *
    * @see https://www.kaggle.com/bls/american-time-use-survey
    *
    * The dataset contains the daily time (in minutes) people spent in various activities. For instance, the column
    * "t010101"contains the time spent sleeping, the column "t110101"contains the time spent eating and drinking, etc.
    *
    * This method groups related columns together:
    * 1. "primary needs"activities (sleeping, eating, etc.). These are the columns starting with "t01", "t03", "t11",
    *    "t1801"and "t1803".
    * 2. working activities. These are the columns starting with "t05"and "t1805".
    * 3. other activities (leisure). These are the columns starting with "t02", "t04", "t06", "t07", "t08", "t09",
    *    "t10", "t12", "t13", "t14", "t15", "t16"and "t18"(those which are not part of the previous groups only).
    */
  def classifiedColumns(columnNames: List[String], df: DataFrame): (List[Column], List[Column], List[Column]) = {
	var needs = List[Column]()
	var work = List[Column]()
	var other = List[Column]()
	val needsList = List("t01", "t03", "t11", "t1801", "t1803")
	val workList = List("t05", "t1805")
	val otherList = List("t02", "t04", "t06", "t07", "t08", "t09", "t10", "t12", "t13", "t14", "t15", "t16", "t18")
	def startsWith(word: String, prefix: List[String]): Boolean = {
		prefix.foreach {
      item =>
			if (word.substring(0,item.length())!=item)
				return false
		}
		return true
	}		
  columnNames.foreach{
    column =>
		if (startsWith(column, needsList))
			needs = needs:+df(column)
		if (startsWith(column, workList))
			work = work:+df(column)
		if (startsWith(column, otherList))
			other = other:+df(column)
	}
	return (needs,work,other)
  }

  /** @return a projection of the initial DataFrame such that all columns containing hours spent on primary needs
    *         are summed together in a single column (and same for work and leisure). The "teage"column is also
    *         projected to three values: "young", "active", "elder".
    *
    * @param primaryNeedsColumns List of columns containing time spent on "primary needs"
    * @param workColumns List of columns containing time spent working
    * @param otherColumns List of columns containing time spent doing other activities
    * @param df DataFrame whose schema matches the given column lists
    *
    * This methods builds an intermediate DataFrame that sums up all the columns of each group of activity into
    * a single column.
    *
    * The resulting DataFrame should have the following columns:
    * - working: value computed from the "telfs"column of the given DataFrame:
    *   - "working" if 1 <= telfs < 3
    *   - "not working" otherwise
    * - sex: value computed from the "tesex"column of the given DataFrame:
    *   - "male" if tesex = 1, "female" otherwise
    * - age: value computed from the "teage"column of the given DataFrame:
    *   - "young" if 15 <= teage <= 22,
    *   - "active" if 23 <= teage <= 55,
    *   - "elder" otherwise
    * - primaryNeeds: sum of all the `primaryNeedsColumns`, in hours
    * - work: sum of all the `workColumns`, in hours
    * - other: sum of all the `otherColumns`, in hours
    *
    * Finally, the resulting DataFrame should exclude people that are not employable (ie telfs = 5).
    *
    * Note that the initial DataFrame contains time in ''minutes''. You have to convert it into ''hours''.
    */
  def timeUsageSummary(
    primaryNeedsColumns: List[Column],
    workColumns: List[Column],
    otherColumns: List[Column],
    df: DataFrame
  ): DataFrame = {
    val workingStatusProjection: Column = df.select("telfs").rdd.map(row => row(0).asInstanceOf[Double]).map(item => item match { case _ if (item>=1 && item<3) => "working" case _ => "not working" }).toDF()("telfs")
    val sexProjection: Column = df.select("tesex").rdd.map(row => row(0).asInstanceOf[Double]).map((item: Double) => item match { case 1 => "male" case _ => "female" }).toDF()("tesex")
    val ageProjection: Column = df.select("teage").rdd.map(row => row(0).asInstanceOf[Double]).map((item: Double) => item match { case _ if (item>=15 && item<=22) => "young" case _ if (item>=23 && item<55) => "active" case _ => "elder" }).toDF()("teage")
    
    val primaryNeedsProjection: Column = primaryNeedsColumns.reduce(_+_)/60
    val workProjection: Column = workColumns.reduce(_+_)/60
    val otherProjection: Column = otherColumns.reduce(_+_)/60
    df
      .select(workingStatusProjection, sexProjection, ageProjection, primaryNeedsProjection, workProjection, otherProjection)
      .where($"telfs" <= 4) // Discard people who are not in labor force
  }

  /** @return the average daily time (in hours) spent in primary needs, working or leisure, grouped by the different
    *         ages of life (young, active or elder), sex and working status.
    * @param summed DataFrame returned by `timeUsageSumByClass`
    *
    * The resulting DataFrame should have the following columns:
    * - working: the "working"column of the `summed` DataFrame,
    * - sex: the "sex"column of the `summed` DataFrame,
    * - age: the "age"column of the `summed` DataFrame,
    * - primaryNeeds: the average value of the "primaryNeeds"columns of all the people that have the same working
    *   status, sex and age, rounded with a scale of 1 (using the `round` function),
    * - work: the average value of the "work"columns of all the people that have the same working status, sex
    *   and age, rounded with a scale of 1 (using the `round` function),
    * - other: the average value of the "other"columns all the people that have the same working status, sex and
    *   age, rounded with a scale of 1 (using the `round` function).
    *
    * Finally, the resulting DataFrame should be sorted by working status, sex and age.
    */
  def timeUsageGrouped(summed: DataFrame): DataFrame = {
    var temp = summed.groupBy("workingStatusProjection","sexProjection","ageProjection").agg(round(avg("primaryNeedsProjection")),round(avg("workProjection")),round(avg("otherProjection")))
    val names = Seq("working","sex","age","primaryNeeds","work","other")
    temp.select("workingStatusProjection","sexProjection","ageProjection","primaryNeedsProjection","workProjection","otherProjection").orderBy("working","sex","age").toDF(names: _*)
  }

  /**
    * @return Same as `timeUsageGrouped`, but using a plain SQL query instead
    * @param summed DataFrame returned by `timeUsageSumByClass`
    */
  def timeUsageGroupedSql(summed: DataFrame): DataFrame = {
    val viewName = s"summed"
    summed.createOrReplaceTempView(viewName)
    spark.sql(timeUsageGroupedSqlQuery(viewName))
  }

  /** @return SQL query equivalent to the transformation implemented in `timeUsageGrouped`
    * @param viewName Name of the SQL view to use
    */
  def timeUsageGroupedSqlQuery(viewName: String): String =
    s"SELECT AVG(primaryNeedsProjection) AS primaryNeeds, AVG(workProjection) AS work, AVG(otherProjection) AS other, workingStatusProjection AS working, sexProjection AS sex, ageProjection AS age FROM "+viewName+" GROUP BY working,sex,ageORDER BY working,sex,age"

  /**
    * @return A `Dataset[TimeUsageRow]` from the "untyped"`DataFrame`
    * @param timeUsageSummaryDf `DataFrame` returned by the `timeUsageSummary` method
    *
    * Hint: you should use the `getAs` method of `Row` to look up columns and
    * cast them at the same time.
    */
  def timeUsageSummaryTyped(timeUsageSummaryDf: DataFrame): Dataset[TimeUsageRow] =
    timeUsageSummaryDf.as[TimeUsageRow]

  /**
    * @return Same as `timeUsageGrouped`, but using the typed API when possible
    * @param summed Dataset returned by the `timeUsageSummaryTyped` method
    *
    * Note that, though they have the same type (`Dataset[TimeUsageRow]`), the input
    * dataset contains one element per respondent, whereas the resulting dataset
    * contains one element per group (whose time spent on each activity kind has
    * been aggregated).
    *
    * Hint: you should use the `groupByKey` and `typed.avg` methods.
    */
  def timeUsageGroupedTyped(summed: Dataset[TimeUsageRow]): Dataset[TimeUsageRow] = {
    import org.apache.spark.sql.expressions.scalalang.typed
	summed
      .groupByKey(row => (row.working, row.sex, row.age))
      .agg( 
        round(avg($"primaryNeeds")).as[Double],
        round(avg($"work")).as[Double],
        round(avg($"other")).as[Double]
      ).map {
      case ((working, sex, age), primaryNeeds, work, other) => TimeUsageRow(working.asInstanceOf[String], sex.asInstanceOf[String], age.asInstanceOf[String], primaryNeeds.asInstanceOf[Double], work.asInstanceOf[Double], other.asInstanceOf[Double])
    }.orderBy('working, 'sex, 'age)
  }
                                                                            }

/**
  * Models a row of the summarized data set
  * @param working Working status (either "working" or "not working")
  * @param sex Sex (either "male" or "female")
  * @param age Age (either "young", "active" or "elder")
  * @param primaryNeeds Number of daily hours spent on primary needs
  * @param work Number of daily hours spent on work
  * @param other Number of daily hours spent on other activities
  */
case class TimeUsageRow(
  working: String,
  sex: String,
  age: String,
  primaryNeeds: Double,
  work: Double,
  other: Double
)