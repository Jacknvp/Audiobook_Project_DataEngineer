from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import when


CLUSTER_STAGING_NAME = "dataproc-staging-asia-east2-xxxxxxxxxxxx-xxxxxxxx"
# create spark session
spark = SparkSession.builder.master("local[*]").getOrCreate()

# read output.csv from Dataproc staging bucket
dt = spark.read.csv(f'gs://{CLUSTER_STAGING_NAME}/data/output.csv', header = True, inferSchema = True)

## cleaning timestamp column change string --> timestamp
dt = dt.withColumn("timestamp", f.to_timestamp(dt.timestamp,'yyyy-MM-dd HH:mm:ss'))
dt.select("timestamp").show(5,False)

## cleaning double quote : Book title column
# "Are There Really Only Four Spiritual Laws?: The Four ""L""S of Life"
dt = dt.withColumn( "Book_Title", 
                   when(dt["Book_Title"]=='"Are There Really Only Four Spiritual Laws?: The Four ""L""S of Life"', "Are There Really Only Four Spiritual Laws?: The Four 'L'S of Life").otherwise(dt["Book_Title"]) )
# "You Have Downloaded a Virus!: Click ""OK"" to Remove"
dt = dt.withColumn( "Book_Title", 
                   when(dt["Book_Title"]=='"You Have Downloaded a Virus!: Click ""OK"" to Remove"', "You Have Downloaded a Virus!: Click 'OK' to Remove").otherwise(dt["Book_Title"]) )

## cleaning Book_Subtitle column
# """The Power of"" Series"
dt = dt.withColumn( "Book_Subtitle", 
                   when(dt["Book_Subtitle"]=='"""The Power of"" Series"', "'The Power of' Series").otherwise(dt["Book_Subtitle"]) )
# """The Power"" of Series"
dt = dt.withColumn( "Book_Subtitle", 
                   when(dt["Book_Subtitle"]=='"""The Power"" of Series"', "'The Power' of Series").otherwise(dt["Book_Subtitle"]) )

## cleaning 3 : Book_Narrator column
# "Curtis ""50 Cent"" Jackson"
dt = dt.withColumn( "Book_Narrator", 
                   when(dt['Book_Narrator'] == '"Curtis ""50 Cent"" Jackson"', "Curtis '50 Cent' Jackson").otherwise(dt['Book_Narrator']) )


## cleaning Rating column
# change null --> Not rated yet
dt = dt.withColumn("Rating", 
                   when( dt["Rating"].isNull(), "Not rated yet").otherwise(dt["Rating"]) )
# change 4, 4.5, 5 out of 5 ---> 4, 4.5, 5
dt = dt.withColumn("Rating", 
                   when( dt["Rating"]=="4 out of 5 stars", "4").otherwise(dt["Rating"]) )
dt = dt.withColumn("Rating", 
                   when( dt["Rating"]=="4.5 out of 5 stars", "4.5").otherwise(dt["Rating"]) )
dt = dt.withColumn("Rating", 
                   when( dt["Rating"]=="5 out of 5 stars", "5").otherwise(dt["Rating"]) )

## cleaning Total_No_of_Ratings column
# change null --> 0
dt = dt.withColumn( "Total_No_of_Ratings", when(dt["Total_No_of_Ratings"].isNull(),0).otherwise(dt["Total_No_of_Ratings"]) )

# save clean data to Dataproc staging bucket
dt.coalesce(1).write.csv(f'gs://{CLUSTER_STAGING_NAME}/data/cleaned_output.csv', header = True)
print("output cleaned!")
