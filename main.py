import calendar
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import StringType,IntegerType
from pyspark.sql.functions import collect_list, split, regexp_replace, col, round,concat,lit,avg

if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("""
        Usage: spark-submit main.py <locations_path> <recordings_path> <output_path>
        """, file=sys.stderr)
        sys.exit(-1)

    locations_path = sys.argv[1]
    recordings_path = sys.argv[2]
    output_path = sys.argv[3]

    spark = SparkSession.builder.master("local").appName('CS235').getOrCreate()
    #Reading the locations file
    locations = spark.read.format("csv").options(inferschema='true',header='true').load(locations_path).filter("CTRY = 'US' and STATE is not null")
    locations.createOrReplaceTempView("locations_vw")
    
    #Reading the recordings files
    recordings = spark.read.text(recordings_path)
    #Filtering the headers in recordings
    recordings_df = recordings.filter("value not like 'STN--%'")
    
    #Replacing multiple spaces with single space so that they can be split easily
    recordings_df = recordings_df.withColumn("value", regexp_replace(col("value"), " +", " "))

    #User defined function for calculating precipitation
    def calc_prec(prec):
        code = prec[-1]
        if code.isdigit():
            return float(prec)
        else:
            value = float(prec[0:-1])
            if code == 'A':
                value = value*4
            if code == 'C':
                value = value*(4/3)
            elif code == 'B' or code == 'E':
                value = value*2
            return value

    calc_prec = UserDefinedFunction(calc_prec, StringType())
    spark.udf.register("calc_prec",calc_prec)

    #Creating a new dataframe with only required columns and ignoring the rest
    recordings_t = split(recordings_df['value'], ' ')
    recordings_df = recordings_df.withColumn('USAF', recordings_t.getItem(0))
    recordings_df = recordings_df.withColumn('WBAN', recordings_t.getItem(1))
    recordings_df = recordings_df.withColumn('TEMP', recordings_t.getItem(3))
    recordings_df = recordings_df.withColumn('MONTH', recordings_t.getItem(2).substr(5, 2).cast("integer"))
    recordings_df = recordings_df.withColumn('PRCP', calc_prec(recordings_t.getItem(19)))

    #Removing unnecessary columns  
    recordings_df = recordings_df.drop("value").filter("TEMP is not null")
    recordings_df.createOrReplaceTempView("recordings_vw")

    #Finding the state's average temperature recorded for each month
    state_month_df = spark.sql("""select STATE, MONTH, avg(TEMP) as AVG_TEMP, avg(case when PRCP=99.99 then null else PRCP end) as AVG_PRCP
    from locations_vw loc inner join recordings_vw rec
    on rec.USAF = loc.USAF
    group by STATE, MONTH""")
    state_month_df.createOrReplaceTempView("state_month_vw")

    #Finding months with the highest and lowest averages for each state
    state_month_avg_df = spark.sql("""select STATE, MONTH, AVG_TEMP, AVG_PRCP from(
    select STATE, MONTH, AVG_TEMP, AVG_PRCP,
    rank() over (partition by STATE order by AVG_TEMP desc) as HIGHEST,
    rank() over (partition by STATE order by AVG_TEMP asc) as LOWEST
    from state_month_vw)t WHERE (HIGHEST=1 OR LOWEST=1) ORDER BY AVG_TEMP, MONTH""")

    #User defined function to get month name
    get_month = UserDefinedFunction(lambda x: calendar.month_name[x], StringType())
    spark.udf.register("get_month",get_month)

    #using collect_list to collect the highest and lowest month into a list for each state
    result_df = state_month_avg_df.groupby('STATE').agg(avg(state_month_avg_df.AVG_PRCP).alias('AVG_PRCP'),collect_list('AVG_TEMP').alias("TEMPS"),collect_list('MONTH').alias("MONTHS"))

    #Printing the results to output file
    result_df.select('STATE',
    concat(round(result_df['TEMPS'][1],2),lit(", "),get_month(result_df['MONTHS'][1])).alias("HIGHEST"),
    concat(round(result_df['TEMPS'][0],2),lit(", "),get_month(result_df['MONTHS'][0])).alias("LOWEST"),
    (round(result_df['TEMPS'][1],2)-round(result_df['TEMPS'][0],2)).alias("DIFFERENCE"),
    round(result_df['AVG_PRCP'],2).alias('AVG_PRCP'))\
    .orderBy('DIFFERENCE')\
    .coalesce(1)\
    .write\
    .mode("overwrite")\
    .option("header", "true")\
    .csv(output_path)

    