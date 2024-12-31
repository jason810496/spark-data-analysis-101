from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc
import matplotlib.pyplot as plt

# 1. init SparkSession
spark = SparkSession.builder.appName("RetailAnalysis").getOrCreate()

# 2. read data
file_path = "./datasets/retail_sales_dataset.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# 3. preview data
df.show(5)
"""
+--------------+----------+-----------+------+---+----------------+--------+--------------+------------+
|Transaction ID|      Date|Customer ID|Gender|Age|Product Category|Quantity|Price per Unit|Total Amount|
+--------------+----------+-----------+------+---+----------------+--------+--------------+------------+
|             1|2023-11-24|    CUST001|  Male| 34|          Beauty|       3|            50|         150|
|             2|2023-02-27|    CUST002|Female| 26|        Clothing|       2|           500|        1000|
|             3|2023-01-13|    CUST003|  Male| 50|     Electronics|       1|            30|          30|
|             4|2023-05-21|    CUST004|  Male| 37|        Clothing|       1|           500|         500|
|             5|2023-05-06|    CUST005|  Male| 30|          Beauty|       2|            50|         100|
+--------------+----------+-----------+------+---+----------------+--------+--------------+------------+
"""

# 4. data cleaning
# 4.1 drop null values
df = df.dropna(subset=["Total Amount", "Price per Unit", "Quantity"])
# 4.2 data type casting
df = df.withColumn("Date", col("Date").cast("date"))
df = df.withColumn("Total Amount", col("Total Amount").cast("int"))
df = df.withColumn("Price per Unit", col("Price per Unit").cast("int"))
df = df.withColumn("Quantity", col("Quantity").cast("int"))
# 4.1 filter out values < 0
df = df.filter((col("Total Amount") > 0) & (col("Price per Unit") > 0) & (col("Quantity") > 0))

# 5. aggregate data
# 5.1 calculate total sales
df_sales = df.withColumn("Sales", col("Quantity") * col("Price per Unit"))
df_sales.show(5)
df_aggregated = df_sales.groupBy("Product Category").agg(sum("Sales").alias("Total Sales"))


# 6. sort data
top_categories = df_aggregated.sort(desc("Total Sales")).limit(10)
top_categories.show()

# 7. visualize data
top_categories_pd = top_categories.toPandas()
top_categories_pd.plot(kind="bar", x="Product Category", y="Total Sales", color="skyblue")
plt.title("Top 10 Product Categories by Sales")
plt.ylabel("Total Sales")
plt.xlabel("Product Category")
plt.tight_layout()
plt.show()