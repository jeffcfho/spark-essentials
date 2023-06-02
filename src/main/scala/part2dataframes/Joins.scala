package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, max, col, desc}

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  // inner joins
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner")

  // outer joins
  // left outer = everything in the inner join + all the rows in the LEFT table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "left_outer")

  // right outer = everything in the inner join + all the rows in the RIGHT table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "right_outer")

  // outer join = everything in the inner join + all the rows in BOTH tables, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "outer")

  // semi-joins = everything in the left DF for which there is a row in the right DF satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_semi")

  // anti-joins = everything in the left DF for which there is NO row in the right DF satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_anti")


  // things to bear in mind
  // guitaristsBandsDF.select("id", "band").show // this crashes

  // option 1 - rename the column on which we are joining
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  // option 2 - drop the dupe column
  guitaristsBandsDF.drop(bandsDF.col("id"))

  // option 3 - rename the offending column and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("bandId"))

  // using complex types
  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"))

  // 1. show all employees and their max salary

  // Reading from a remote DB
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.employees")
    .load()

//  println(employeesDF.count())

  val salariesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.salaries")
    .load()

  val max_salaries = salariesDF
    .groupBy("emp_no")
    .max("salary")

  val joined1 = employeesDF.join(max_salaries, "emp_no")
//  joined1.show()
//  println(joined1.count())

  // 2. show all employees who were never managers

  val managersDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.dept_manager")
    .load()

  //val joined2 = employeesDF.join(managersDF, "emp_no", "left_anti") // this doen't work
  val joined2 = employeesDF.join(managersDF, employeesDF.col("emp_no") === managersDF.col("emp_no"), "left_anti")
//  joined2.show()

  // 3. latest job titles of the best paid 10 employees

  val titlesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.titles")
    .load()

  val max_salaries_with_date = salariesDF.join(max_salaries,
    max_salaries("emp_no") === salariesDF("emp_no") && max_salaries("max(salary)") === salariesDF("salary"),
    "left_semi")
  max_salaries_with_date.show()

  val title_joinCond = max_salaries_with_date("emp_no") === titlesDF("emp_no") && max_salaries_with_date("from_date") === titlesDF("from_date")
  val joined3 = max_salaries_with_date.join(titlesDF, title_joinCond).orderBy(desc("salary"))
  joined3.show()
  
}

