# Pyspark-Concepts
This repository contains simple PySpark practice code based on classroom rough notes.
The goal of this project is to practice PySpark DataFrame operations and understand distributed data processing concepts in a simple and easy way.

All examples are written in simple Python code with comments so that beginners can understand easily.

Topics Covered

This practice file includes examples of the following PySpark concepts.

1. Removing Duplicate Data

Used to remove duplicate rows from a DataFrame.

Commands used:

distinct()

dropDuplicates()

Example idea:

Remove duplicate employee records from a dataset.

2. Sorting Data

Sorting is used to arrange rows in order.

Commands used:

sort()

orderBy()

col()

asc()

Example idea:

Sort employees by department and state.

3. Temporary Views and Spark SQL

Spark allows running SQL queries on DataFrames.

Commands used:

createOrReplaceTempView()

spark.sql()

Example idea:

Create a temporary table and run SQL queries on it.

4. Group By and Aggregations

Used to group rows and perform calculations.

Commands used:

groupBy()

sum()

avg()

max()

min()

count()

mean()

agg()

alias()

Example idea:

Calculate total salary and bonus for each department.

5. Filtering Aggregated Data

After grouping data we can filter results.

Commands used:

where()

filter()

This works similar to the HAVING clause in SQL.

6. Joins in PySpark

Joins are used to combine data from two DataFrames.

Join types covered:

Inner Join

Left Join

Right Join

Outer Join

Full Join

Full Outer Join

Example idea:

Join employee data with department data.

7. Broadcast Variables

Broadcast is used to send small data to all worker machines.

Command used:

sc.broadcast()

Example idea:

Broadcast a small dictionary to all Spark workers.

This helps avoid repeated data transfer in distributed systems.

8. Broadcast Join

Broadcast join is used when one table is small.

Instead of shuffling large data, Spark copies the small table to all worker nodes.

Command used:

broadcast()

Benefit:

Broadcast join is faster than shuffle join.

9. Union

Union is used to combine two DataFrames.

Command used:

union()

Important:

Spark union() behaves like SQL UNION ALL, which means duplicates are not removed automatically.

To remove duplicates we use:

distinct()

dropDuplicates()

10. Map Function

Map is used in RDD to process data element by element.

Command used:

map()

Example idea:

Multiply every number in an RDD by 2.

File in this Repository
day20_pyspark_practice.py

This file contains all the examples for:

duplicates

sorting

Spark SQL

groupBy

filtering aggregated data

joins

broadcast variables

broadcast joins

union

RDD map transformation

How to Run

Run the script using Spark.

spark-submit day20_pyspark_practice.py

Or run inside PySpark.

exec(open("day20_pyspark_practice.py").read())
Purpose of this Repository

This repository is created for:

practicing PySpark

understanding Spark DataFrame operations

learning distributed data processing concepts

