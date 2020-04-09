package com.example

class BostonCrimesMap (path_to_facts: String, path_to_dim: String, path_to_output_folder: String) {

      import org.apache.spark.sql.SparkSession
      import org.apache.spark.sql.functions._
      val spark: SparkSession = SparkSession.builder.master("local").getOrCreate

      import spark.implicits._


      //////////////////////////////////////////////////////////////facts_raw

      val facts_raw = spark
        .read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path_to_facts)
        .cache()


      //////////////////////////////////////////////////////////////dim

      val dim_raw = spark
        .read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path_to_dim)

      dim_raw.createOrReplaceTempView("dim_raw")

      val dim = spark.sql("select CODE, NAME, if(instr(NAME, '-') = 0, NAME, left(NAME, instr(NAME, '-')-1)) as CRIME_TYPE, row_number() over (partition by CODE order by NAME) as Rnk_d from dim_raw").cache()


      //////////////////////////////////////////////////////////////facts_by_districts_pt1

      facts_raw.createOrReplaceTempView("facts_raw")

      val facts_by_districts_months = spark.sql("select DISTRICT, YEAR, MONTH, count(*) as Qty, avg(Lat) as Lat, avg(Long) as Long from facts_raw group by DISTRICT, YEAR, MONTH")
      facts_by_districts_months.createOrReplaceTempView("facts_by_districts_months")

      val facts_by_districts_pt1 = spark.sql("select DISTRICT as DISTRICT_pt1, sum(Qty) as crimes_total, percentile_approx(Qty, 0.5) as crimes_monthly, avg(Lat) as lat, avg(Long) as lng from facts_by_districts_months group by DISTRICT").cache()


      //////////////////////////////////////////////////////////////facts_by_districts_pt2

      val facts_by_districts_code = facts_raw.groupBy($"DISTRICT", $"OFFENSE_CODE").count()
      facts_by_districts_code.createOrReplaceTempView("facts_by_districts_code")

      val facts_by_districts_rank_code = spark.sql("select DISTRICT, OFFENSE_CODE, count, row_number() over (partition by DISTRICT order by count desc) as Rnk_f from facts_by_districts_code").cache()

      val facts_by_districts_top3crimes_1 = facts_by_districts_rank_code
        .filter('Rnk_f === 1)
        .join(broadcast(dim.filter('Rnk_d === 1)), 'CODE === 'OFFENSE_CODE)
        .select($"DISTRICT", $"CRIME_TYPE", $"Rnk_f")
        .cache()

      val facts_by_districts_top3crimes_2 = facts_by_districts_rank_code
        .filter('Rnk_f === 2)
        .join(broadcast(dim.filter('Rnk_d === 1)), 'CODE === 'OFFENSE_CODE)
        .withColumnRenamed("DISTRICT", "DISTRICT_2")
        .withColumnRenamed("CRIME_TYPE", "CRIME_TYPE_2")
        .cache()

      val facts_by_districts_top3crimes_3 = facts_by_districts_rank_code
        .filter('Rnk_f === 3)
        .join(broadcast(dim.filter('Rnk_d === 1)), 'CODE === 'OFFENSE_CODE)
        .withColumnRenamed("DISTRICT", "DISTRICT_3")
        .withColumnRenamed("CRIME_TYPE", "CRIME_TYPE_3")
        .cache()

      val facts_by_districts_pt2 = facts_by_districts_top3crimes_1
        .join(facts_by_districts_top3crimes_2, 'DISTRICT === 'DISTRICT_2)
        .join(facts_by_districts_top3crimes_3, 'DISTRICT === 'DISTRICT_3)
        .withColumn("frequent_crime_types", concat_ws(", ", $"CRIME_TYPE", $"CRIME_TYPE_2", $"CRIME_TYPE_3"))
        .select($"DISTRICT", $"frequent_crime_types")
        .withColumnRenamed("DISTRICT", "DISTRICT_pt2")
        .cache()


      //////////////////////////////////////////////////////////////facts_by_districts

      val facts_by_districts = facts_by_districts_pt1
        .join(facts_by_districts_pt2, 'DISTRICT_pt1 === 'DISTRICT_pt2)
        .select($"DISTRICT_pt1", $"crimes_total", $"crimes_monthly", $"frequent_crime_types", $"lat", $"lng")
        .withColumnRenamed("DISTRICT_pt1", "DISTRICT")

      facts_by_districts.write.parquet(path_to_output_folder)
      println("OK")

  }
