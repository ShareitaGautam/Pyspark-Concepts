# Day 20 PySpark Practice
# Simple code with comments based only on the rough notes
if __name__ == "__main__":
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, max, min, mean, count, broadcast

# Create Spark session
spark = SparkSession.builder.appName("Day20_PySpark_Practice").getOrCreate()

# Spark context
sc = spark.sparkContext


# =========================================================
# 1. DISTINCT AND DROP DUPLICATES
# =========================================================

# Sample data with duplicate row
data = [
    ("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),   # duplicate row
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
]

# Column names
columns = ["employee_name", "department", "salary"]

# Create DataFrame
df = spark.createDataFrame(data=data, schema=columns)

print("Original DataFrame")
df.show(truncate=False)

print("Schema")
df.printSchema()


# distinct() removes full duplicate rows
print("Using distinct()")
df1 = df.distinct()
df1.show(truncate=False)


# dropDuplicates() also removes full duplicate rows
print("Using dropDuplicates()")
df2 = df.dropDuplicates()
df2.show(truncate=False)


# dropDuplicates() on selected columns
# It keeps only one row for each unique department + salary
print('Using dropDuplicates(["department","salary"])')
dropDisDF = df.dropDuplicates(["department", "salary"])
dropDisDF.show(truncate=False)


# dropDuplicates() on one column
# It keeps only one row for each department
print('Using dropDuplicates(["department"])')
dropDisDF = df.dropDuplicates(["department"])
dropDisDF.show(truncate=False)


# =========================================================
# 2. SORTING
# =========================================================

simpleData = [
    ("James", "Sales", "NY", 90000, 34, 10000),
    ("Michael", "Sales", "NY", 86000, 56, 20000),
    ("Robert", "Sales", "CA", 81000, 30, 23000),
    ("Maria", "Finance", "CA", 90000, 24, 23000),
    ("Raman", "Finance", "CA", 99000, 40, 24000),
    ("Scott", "Finance", "NY", 83000, 36, 19000),
    ("Jen", "Finance", "NY", 79000, 53, 15000),
    ("Jeff", "Marketing", "CA", 80000, 25, 18000),
    ("Kumar", "Marketing", "NY", 91000, 50, 21000)
]

columns = ["employee_name", "department", "state", "salary", "age", "bonus"]

df = spark.createDataFrame(data=simpleData, schema=columns)

print("New DataFrame for sorting and groupBy")
df.show(truncate=False)

print("Schema")
df.printSchema()


# Sort by column names
print('Sort using strings: df.sort("department","state")')
df.sort("department", "state").show(truncate=False)


# Sort using col()
print('Sort using col("department"), col("state")')
df.sort(col("department"), col("state")).show(truncate=False)


# Sort using asc()
print('Sort using col("department").asc(), col("state").asc()')
df.sort(col("department").asc(), col("state").asc()).show(truncate=False)


# orderBy() is same as sort()
print('Order by using orderBy()')
df.orderBy(col("department"), col("state")).show(truncate=False)


# =========================================================
# 3. TEMP VIEW AND SQL
# =========================================================

# Create temp view so that we can run SQL on DataFrame
df.createOrReplaceTempView("EMP")

print('SQL query on temp view "EMP"')
spark.sql("""
    SELECT employee_name, department, state, salary, age, bonus
    FROM EMP
    ORDER BY department ASC
""").show(truncate=False)


# =========================================================
# 4. GROUP BY AND AGGREGATIONS
# =========================================================

# Group by department and sum salary
print('Group by department and sum("salary")')
df.groupBy("department").sum("salary").show(truncate=False)


# Import aggregate functions already done above
# Here we use multiple aggregations together
print("Group by department with multiple aggregations")
df.groupBy("department") \
    .agg(
        sum("salary").alias("sum_salary"),
        avg("salary").alias("avg_salary"),
        sum("bonus").alias("sum_bonus"),
        max("bonus").alias("max_bonus")
    ) \
    .show(truncate=False)


# Filter after aggregation
# In PySpark DataFrame API, this works like HAVING in SQL
print("Filter after groupBy and agg() using where()")
df.groupBy("department") \
    .agg(
        sum("salary").alias("sum_salary"),
        avg("salary").alias("avg_salary"),
        sum("bonus").alias("sum_bonus"),
        max("bonus").alias("max_bonus")
    ) \
    .where(col("sum_bonus") >= 50000) \
    .show(truncate=False)


# =========================================================
# 5. JOINS
# =========================================================

# Employee data
emp = [
    (1, "Smith", -1, "2018", "10", "M", 3000),
    (2, "Rose", 1, "2010", "20", "M", 4000),
    (3, "Williams", 1, "2010", "10", "M", 1000),
    (4, "Jones", 2, "2005", "10", "F", 2000),
    (5, "Brown", 2, "2010", "40", "", -1),
    (6, "Brown", 2, "2010", "50", "", -1)
]

empColumns = [
    "emp_id",
    "name",
    "superior_emp_id",
    "year_joined",
    "emp_dept_id",
    "gender",
    "salary"
]

empDF = spark.createDataFrame(data=emp, schema=empColumns)

print("Employee DataFrame")
empDF.show(truncate=False)

print("Employee Schema")
empDF.printSchema()


# Department data
dept = [
    ("Finance", 10),
    ("Marketing", 20),
    ("Sales", 30),
    ("IT", 40)
]

deptColumns = ["dept_name", "dept_id"]

deptDF = spark.createDataFrame(data=dept, schema=deptColumns)

print("Department DataFrame")
deptDF.show(truncate=False)

print("Department Schema")
deptDF.printSchema()


# Join condition
join_condition = empDF.emp_dept_id == deptDF.dept_id


# Inner join
print('Inner Join')
empDF.join(deptDF, join_condition, "inner").show(truncate=False)


# Outer join
print('Outer Join')
empDF.join(deptDF, join_condition, "outer").show(truncate=False)


# Full join
print('Full Join')
empDF.join(deptDF, join_condition, "full").show(truncate=False)


# Full outer join
print('Full Outer Join')
empDF.join(deptDF, join_condition, "fullouter").show(truncate=False)


# Left join
print('Left Join')
empDF.join(deptDF, join_condition, "left").show(truncate=False)


# Left outer join
print('Left Outer Join')
empDF.join(deptDF, join_condition, "leftouter").show(truncate=False)


# Right join
print('Right Join')
empDF.join(deptDF, join_condition, "right").show(truncate=False)


# =========================================================
# 6. BROADCAST VARIABLE
# =========================================================

# Small dictionary
# Broadcast sends this small data to all worker machines
my_dict = {
    "Finance": 10,
    "Marketing": 20,
    "Sales": 30,
    "IT": 40
}

bc_dict = sc.broadcast(my_dict)

print("Broadcast variable value")
print(bc_dict.value)


# =========================================================
# 7. BROADCAST JOIN
# =========================================================

# Broadcast join is useful when one table is small
# Here deptDF is small, so we broadcast it
print("Broadcast Join")
empDF.join(broadcast(deptDF), join_condition, "inner").show(truncate=False)


# =========================================================
# 8. UNION
# =========================================================

data1 = [
    ("A", 1),
    ("B", 2),
    ("C", 3)
]

data2 = [
    ("C", 3),
    ("D", 4),
    ("E", 5)
]

df1 = spark.createDataFrame(data1, ["name", "value"])
df2 = spark.createDataFrame(data2, ["name", "value"])

print("DataFrame 1")
df1.show()

print("DataFrame 2")
df2.show()


# In Spark, union() works like UNION ALL
# It keeps duplicates
print("Union")
df1.union(df2).show()


# If we want unique rows after union, use distinct()
print("Union with distinct()")
df1.union(df2).distinct().show()


# We can also use dropDuplicates()
print("Union with dropDuplicates()")
df1.union(df2).dropDuplicates().show()


# =========================================================
# 9. MAP FUNCTION WITH RDD
# =========================================================

# map() works element by element
rdd = sc.parallelize([1, 2, 3, 4])

result = rdd.map(lambda x: x * 2)

print("RDD map result")
print(result.collect())


# Stop Spark session
spark.stop()
