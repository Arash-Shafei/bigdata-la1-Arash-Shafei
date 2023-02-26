import csv
import os
import sys
# Spark imports
from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
# Dask imports
import dask.bag as db
import dask.dataframe as df  # you can use Dask bags or dataframes
from csv import reader


'''
INTRODUCTION

The goal of this assignment is to implement a basic analysis of textual 
data using Apache Spark (http://spark.apache.org) and 
Dask (https://dask.org). 
'''

'''
DATASET

We will study a dataset provided by the city of Montreal that contains 
the list of trees treated against the emerald ash borer 
(https://en.wikipedia.org/wiki/Emerald_ash_borer). The dataset is 
described at 
http://donnees.ville.montreal.qc.ca/dataset/frenes-publics-proteges-injection-agrile-du-frene 
(use Google translate to translate from French to English). 

We will use the 2015 and 2016 data sets available in directory `data`.
'''

'''
HELPER FUNCTIONS

These functions are here to help you. Instructions will tell you when
you should use them. Don't modify them!
'''



#Initialize a spark session.
def init_spark():
    import findspark
    findspark.init()
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark

#Useful functions to print RDDs and Dataframes.
def toCSVLineRDD(rdd):
    '''
    This function convert an RDD or a DataFrame into a CSV string
    '''
    a = rdd.map(lambda row: ",".join([str(elt) for elt in row]))\
           .reduce(lambda x,y: os.linesep.join([x,y]))
    return a + os.linesep

def toCSVLine(data):
    '''
    Convert an RDD or a DataFrame into a CSV string
    '''
    if isinstance(data, RDD):
        return toCSVLineRDD(data)
    elif isinstance(data, DataFrame):
        return toCSVLineRDD(data.rdd)
    return None

'''
Plain PYTHON implementation

To get started smoothly and become familiar with the assignment's 
technical context (Git, GitHub, pytest, GitHub actions), we will implement a 
few steps in plain Python.
'''

#Python answer functions
def count(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the number of trees (non-header lines) in
    the data file passed as first argument.
    Test file: tests/test_count.py
    Note: The return value should be an integer
    '''

    num_trees = 0
    with open(filename, "r") as file:
        lines = len(file.readlines())
    return lines-1
    #raise NotImplementedError

def parks(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks.py
    Note: The return value should be an integer
    '''

    num_trees_inPark = 0
    with open(filename, "r") as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            if row[6]:
                num_trees_inPark += 1
    return num_trees_inPark
    #raise NotImplementedError

def uniq_parks(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the list of unique parks where trees
    were treated. The list must be ordered alphabetically. Every element in the list must be printed on
    a new line.
    Test file: tests/test_uniq_parks.py
    Note: The return value should be a string with one park name per line
    '''

    unique_parks = set()
    with open(filename, "r", encoding="utf-8") as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            if row[6]:
                unique_parks.add(row[6] + "\n")
    sorted_unique_parks = sorted(unique_parks)
    sorted_unique_parks_string = "".join(sorted_unique_parks)
    return sorted_unique_parks_string
    #raise NotImplementedError

def uniq_parks_counts(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that counts the number of trees treated in each park
    and prints a list of "park,count" pairs in a CSV manner ordered
    alphabetically by the park name. Every element in the list must be printed
    on a new line.
    Test file: tests/test_uniq_parks_counts.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    # create an empty dictionary to store the count of trees treated in each park
    tree_counts = {}
    # open the CSV file and read its content
    with open(filename, 'r', encoding="utf-8") as file:
        reader = csv.DictReader(file)
        # iterate over each row in the CSV file
        for row in reader:
            # get the name of the park from the current row
            park_name = row["Nom_parc"]
            # check if the park is already in the dictionary
            if park_name in tree_counts:
                # if it is, increment the count of trees treated by 1
                tree_counts[park_name] += 1
            else:
                # if it is not, add it to the dictionary with a count of 1
                tree_counts[park_name] = 1

    # sort the dictionary by the park name
    sorted_tree_counts = sorted(tree_counts.items())
    sorted_tree_counts.pop(0)

    # create a string with the park name and its count, separated by a comma, for each park in the dictionary
    result = ''
    for park, count in sorted_tree_counts:
        result += f'{park},{count}\n'
    return result
    #raise NotImplementedError

def frequent_parks_count(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the list of the 10 parks with the
    highest number of treated trees. Parks must be ordered by decreasing
    number of treated trees and by alphabetical order when they have similar number.
    Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    # create an empty dictionary to store the count of trees treated in each park
    tree_counts = {}
    # open the CSV file and read its content
    with open(filename, 'r', encoding="utf-8") as file:
        reader = csv.DictReader(file)
        # iterate over each row in the CSV file
        for row in reader:
            # get the name of the park from the current row
            park_name = row["Nom_parc"]
            # check if the park is already in the dictionary
            if park_name in tree_counts:
                # if it is, increment the count of trees treated by 1
                tree_counts[park_name] += 1
            else:
                # if it is not, add it to the dictionary with a count of 1
                tree_counts[park_name] = 1

    # sort the dictionary by the park name
    sorted_tree_counts = sorted(tree_counts.items(), key=lambda x: (-x[1], x[0]))
    sorted_tree_counts.pop(0)

    # create a string with the park name and its count, separated by a comma, for each park in the dictionary
    result = ''
    for park, count in sorted_tree_counts[:10]:
        result += f'{park},{count}\n'
    return result
    #raise NotImplementedError

def intersection(filename1, filename2):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the alphabetically sorted list of
    parks that had trees treated both in 2016 and 2015. Every list element
    must be printed on a new line.
    Test file: tests/test_intersection.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    parks_2015 = set()
    with open(filename1, "r", encoding="utf-8") as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            if row[6]:
                parks_2015.add(row[6])


    parks_2016 = set()
    with open(filename2, "r", encoding="utf-8") as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            if row[6]:
                parks_2016.add(row[6])

    common_parks = sorted(parks_2015 & parks_2016)
    result = ''
    for park in common_parks:
        result += f'{park}\n'
    return result
    # raise NotImplementedError

'''
SPARK RDD IMPLEMENTATION

You will now have to re-implement all the functions above using Apache 
Spark's Resilient Distributed Datasets API (RDD, see documentation at 
https://spark.apache.org/docs/latest/rdd-programming-guide.html). 
Outputs must be identical to the ones obtained above in plain Python. 
However, all operations must be re-implemented using the RDD API, you 
are not allowed to simply convert results obtained with plain Python to 
RDDs (this will be checked). Note that the function *toCSVLine* in the 
HELPER section at the top of this file converts RDDs into CSV strings.
'''

# RDD functions

def count_rdd(filename):
    '''
    Write a Python script using RDDs that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_rdd.py
    Note: The return value should be an integer
    '''

    # initialize a SparkSession
    spark = init_spark()

    # create an RDD from the text file
    rdd = spark.sparkContext.textFile(filename)

    # count the number of trees
    num_trees = rdd.count() - 1

    # return the number of trees
    return num_trees

    #raise NotImplementedError

def parks_rdd(filename):
    '''
    Write a Python script using RDDs that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_rdd.py
    Note: The return value should be an integer
    '''

    # initialize a SparkSession
    spark = init_spark()

    # create an RDD from the text file
    rdd = spark.sparkContext.textFile(filename)

    # parse the CSV text using the csv module since dataset contains commas inside data entries
    csv_rdd = rdd.map(lambda line: next(csv.reader([line])))

    # filter for non-empty entries in 7th column
    in_park1 = csv_rdd.filter(lambda row: len(row[6]) > 0).count() - 1

    # return the output as an integer
    return in_park1

    #raise NotImplementedError

def uniq_parks_rdd(filename):
    '''
    Write a Python script using RDDs that prints the list of unique parks where
    trees were treated. The list must be ordered alphabetically. Every element
    in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_rdd.py
    Note: The return value should be a CSV string
    '''

    # initialize a SparkSession
    spark = init_spark()

    # create an RDD from the text file
    sc = spark.sparkContext.textFile(filename)

    # parse the CSV text using the csv module since dataset contains commas inside data entries
    rdd = sc.map(lambda line: next(csv.reader([line])))

    # remove the header
    rdd = rdd.filter(lambda line: line[6] != "Nom_parc")

    # filter for non-empty "Nom_parc" entries, extract the unique entries, sort by alphabetical order
    rdd = rdd.filter(lambda row: len(row[6]) > 0).map(lambda row: row[6]).distinct().sortBy(lambda x: x)

    # one entry per line, converting to csv string format
    rdd = rdd.map(lambda park: park + "\n").reduce(lambda x, y: x+y)

    # return the csv string
    return rdd

    #raise NotImplementedError

def uniq_parks_counts_rdd(filename):
    '''
        Write a Python script using RDDs that counts the number of trees treated in
        each park and prints a list of "park,count" pairs in a CSV manner ordered
        alphabetically by the park name. Every element in the list must be printed
        on a new line.
        Test file: tests/test_uniq_parks_counts_rdd.py
        Note: The return value should be a CSV string
             Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
        '''

    # initialize a SparkSession
    spark = init_spark()

    # create an RDD from the text file
    sc = spark.sparkContext.textFile(filename)

    # parse the CSV text using the csv module since dataset contains commas inside data entries
    rdd = sc.map(lambda line: next(csv.reader([line])))

    # remove the header
    rdd = rdd.filter(lambda header: header[6] != "Nom_parc")

    # grouping by the 7th column, and sorting alphabetically
    rdd = rdd.map(lambda x: (x[6], 1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x: x)

    # removing the first element in the RDD
    rdd = rdd.mapPartitionsWithIndex(lambda i, it: iter(list(it)[1:]) if i == 0 else it)

    # convert each element to a comma-seperated string -> [[1, 2, 3], [4, 5, 6], [7, 8, 9]] -> ['1,2,3', '4,5,6', '7,8,9']
    rdd = rdd.map(lambda row: ','.join([str(elem) for elem in row]))

    # appends a new line character to each element and then concatenates them -> '1,2,3\n4,5,6\n7,8,9\n'
    rdd = rdd.map(lambda park: park + "\n").reduce(lambda x, y: x + y)

    # return the csv string
    return rdd
    #raise NotImplementedError

def frequent_parks_count_rdd(filename):
    '''
    Write a Python script using RDDs that prints the list of the 10 parks with
    the highest number of treated trees. Parks must be ordered by decreasing
    number of treated trees and by alphabetical order when they have similar
    number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_rdd.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    # initialize a SparkSession
    spark = init_spark()

    # create an RDD from the text file
    sc = spark.sparkContext.textFile(filename)

    # parse the CSV text using the csv module since dataset contains commas inside data entries
    rdd = sc.map(lambda line: next(csv.reader([line])))

    # remove the header
    rdd = rdd.filter(lambda header: header[6] != "Nom_parc")

    # grouping by the 7th column, and sorting alphabetically
    rdd = rdd.map(lambda x: (x[6], 1)).reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1], False)

    # removing the first element in the RDD
    rdd = rdd.mapPartitionsWithIndex(lambda i, it: iter(list(it)[1:]) if i == 0 else it)

    # extract the first 10 elements
    rdd = rdd.take(10)

    result = ""
    for park, count in rdd:
        result += f'{park},{count}\n'
    print(result)
    return result

    #raise NotImplementedError

def intersection_rdd(filename1, filename2):
    '''
    Write a Python script using RDDs that prints the alphabetically sorted list
    of parks that had trees treated both in 2016 and 2015. Every list element
    must be printed on a new line.
    Test file: tests/test_intersection_rdd.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    # initialize a SparkSession
    spark = init_spark()

    # create an RDD from the text file
    sc1 = spark.sparkContext.textFile(filename1)
    sc2 = spark.sparkContext.textFile(filename2)

    # parse the CSV text using the csv module since dataset contains commas inside data entries
    rdd1 = sc1.map(lambda line: next(csv.reader([line])))
    rdd2 = sc2.map(lambda line: next(csv.reader([line])))

    # remove the header
    rdd1 = rdd1.filter(lambda header: header[6] != "Nom_parc")
    rdd2 = rdd2.filter(lambda header: header[6] != "Nom_parc")

    intersection_rdd = rdd1.map(lambda x: x[6]).intersection(rdd2.map(lambda x: x[6]))

    # removing the first element in the RDD
    intersection_rdd = intersection_rdd.mapPartitionsWithIndex(lambda i, it: iter(list(it)[1:]) if i == 0 else it)

    intersection_rdd = intersection_rdd.sortBy(lambda x: x).collect()

    result = ""
    for park in intersection_rdd:
        result += f'{park}\n'
    print(result)
    return result

    #raise NotImplementedError


'''
SPARK DATAFRAME IMPLEMENTATION

You will now re-implement all the tasks above using Apache Spark's 
DataFrame API (see documentation at 
https://spark.apache.org/docs/latest/sql-programming-guide.html). 
Outputs must be identical to the ones obtained above in plain Python. 
Note: all operations must be re-implemented using the DataFrame API, 
you are not allowed to simply convert results obtained with the RDD API 
to Data Frames. Note that the function *toCSVLine* in the HELPER 
section at the top of this file also converts DataFrames into CSV 
strings.
'''

# DataFrame functions

def count_df(filename):
    '''
    Write a Python script using DataFrames that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_df.py
    Note: The return value should be an integer
    '''

    # initialize
    spark = init_spark()
    sc = spark.builder.getOrCreate()

    # create the dataframe
    df = sc.read.csv(filename, header=True, mode="DROPMALFORMED")

    # get the number of rows
    count = df.count()

    # return
    return count

    #raise NotImplementedError

def parks_df(filename):
    '''
    Write a Python script using DataFrames that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_df.py
    Note: The return value should be an integer
    '''

    # initialize
    spark = init_spark()
    sc = spark.builder.getOrCreate()

    # create the dataframe
    df = sc.read.csv(filename,header=True, mode="DROPMALFORMED")

    # remove the rows with empty entries in "Nom_parc" column
    # count the rows

    count = df.na.drop(subset=["Nom_parc"]).count()

    # return
    return count

    #raise NotImplementedError

def uniq_parks_df(filename):
    '''
    Write a Python script using DataFrames that prints the list of unique parks
    where trees were treated. The list must be ordered alphabetically. Every
    element in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_df.py
    Note: The return value should be a CSV string
    '''

    # initialize
    spark = init_spark()
    sc = spark.builder.getOrCreate()

    # create the dataframe
    df = sc.read.csv(filename, header=True, mode="DROPMALFORMED")

    # remove the rows with empty entries in "Nom_parc" column
    df = df.na.drop(subset=["Nom_parc"])

    # select the "Nom_parc" column
    # get the distinct entries in that column
    # order the dataframe by alphabetical order
    df= df.select("Nom_parc").distinct().orderBy("Nom_parc")

    # return
    return toCSVLine(df)

    #raise NotImplementedError

def uniq_parks_counts_df(filename):
    '''
    Write a Python script using DataFrames that counts the number of trees
    treated in each park and prints a list of "park,count" pairs in a CSV
    manner ordered alphabetically by the park name. Every element in the list
    must be printed on a new line.
    Test file: tests/test_uniq_parks_counts_df.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    # initialize
    spark = init_spark()
    sc = spark.builder.getOrCreate()

    # create the dataframe
    df = sc.read.csv(filename, header=True, mode="DROPMALFORMED")

    # remove the rows with empty entries in "Nom_parc" column
    df = df.na.drop(subset=["Nom_parc"])

    # group the "Nom_parc" column
    # ??
    # order the dataframe by alphabetical order
    df = df.groupBy("Nom_parc").count().orderBy("Nom_parc")

    # return
    return toCSVLine(df)

    # ADD YOUR CODE HERE
    #raise NotImplementedError

def frequent_parks_count_df(filename):
    '''
    Write a Python script using DataFrames that prints the list of the 10 parks
    with the highest number of treated trees. Parks must be ordered by
    decreasing number of treated trees and by alphabetical order when they have
    similar number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_df.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    # initialize
    spark = init_spark()
    sc = spark.builder.getOrCreate()

    # create the dataframe
    df = sc.read.csv(filename, header=True, mode="DROPMALFORMED")

    # remove the rows with empty entries in "Nom_parc" column
    df = df.na.drop(subset=["Nom_parc"])

    # group the "Nom_parc" column
    # ??
    # order the dataframe by alphabetical order
    df = df.groupBy("Nom_parc").count().orderBy("Nom_parc")

    # order in descending manner the "count" column
    df = df.orderBy("count", ascending=False)

    # show only 10 rows
    df = df.limit(10)

    # return
    return toCSVLine(df)

    # raise NotImplementedError

def intersection_df(filename1, filename2):
    '''
    Write a Python script using DataFrames that prints the alphabetically
    sorted list of parks that had trees treated both in 2016 and 2015. Every
    list element must be printed on a new line.
    Test file: tests/test_intersection_df.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    # initialize
    spark = init_spark()
    sc1 = spark.builder.getOrCreate()
    sc2 = spark.builder.getOrCreate()

    # create the dataframe
    df1 = sc1.read.csv(filename1, header=True, mode="DROPMALFORMED")
    df2 = sc2.read.csv(filename2, header=True, mode="DROPMALFORMED")

    # remove the rows with empty entries in "Nom_parc" column
    df1 = df1.na.drop(subset=["Nom_parc"])
    df2 = df2.na.drop(subset=["Nom_parc"])

    # select the "Nom_parc" column
    # get the distinct entries in that column
    # order the dataframe by alphabetical order
    df1 = df1.select("Nom_parc").distinct().orderBy("Nom_parc")
    df2 = df2.select("Nom_parc").distinct().orderBy("Nom_parc")

    # intersection between two dataframes
    df_intersection = df1.intersect(df2)

    # return
    return toCSVLine(df_intersection)

    #raise NotImplementedError

'''
DASK IMPLEMENTATION

You will now re-implement all the tasks above using Dask (see 
documentation at http://docs.dask.org/en/latest). Outputs must be 
identical to the ones obtained previously. Note: all operations must be 
re-implemented using Dask, you are not allowed to simply convert 
results obtained with the other APIs.
'''

# Dask functions

def count_dask(filename):
    '''
    Write a Python script using Dask that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_dask.py
    Note: The return value should be an integer
    '''

    # load the CSV into dask dataframe
    dask_df = df.read_csv(filename, dtype=str)

    # count the number of rows in dataframe
    num_rows = len(dask_df)

    # return
    return num_rows

    #raise NotImplementedError

def parks_dask(filename):
    '''
    Write a Python script using Dask that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_dask.py
    Note: The return value should be an integer
    '''

    # load the CSV into dask dataframe
    dask_df = df.read_csv(filename, dtype=str)

    # remove rows with empty entries in the "Nom_parc" column
    dask_df = dask_df.dropna(subset=["Nom_parc"])

    # count the number of rows in dataframe
    num_rows = len(dask_df)

    # return
    return num_rows

    #raise NotImplementedError

def uniq_parks_dask(filename):
    '''
    Write a Python script using Dask that prints the list of unique parks
    where trees were treated. The list must be ordered alphabetically. Every
    element in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_dask.py
    Note: The return value should be a CSV string
    '''

    # load the CSV into dask dataframe
    dask_df = df.read_csv(filename, dtype=str)

    # remove rows with empty entries in the "Nom_parc" column
    dask_df = dask_df.dropna(subset=["Nom_parc"])

    # getting the distinct values in the "Nom_parc" column
    # sorting them in alphabetical order
    dask_df = dask_df["Nom_parc"].unique().compute().sort_values()

    # converting to CSV string
    result = ''
    for park in dask_df:
        result += f'{park}\n'
    return result


    #raise NotImplementedError

def uniq_parks_counts_dask(filename):
    '''
    Write a Python script using Dask that counts the number of trees
    treated in each park and prints a list of "park,count" pairs in a CSV
    manner ordered alphabetically by the park name. Every element in the list
    must be printed on a new line.
    Test file: tests/test_uniq_parks_counts_dask.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    # load the CSV into dask dataframe
    dask_df = df.read_csv(filename, dtype=str)

    # remove rows with empty entries in the "Nom_parc" column
    dask_df = dask_df.dropna(subset=["Nom_parc"])

    # ?
    dask_df = dask_df.groupby("Nom_parc").count().compute()

    dask_df = dask_df.sort_values("Nom_parc")

    result = ''
    for park, count in dask_df.iterrows():
        result += f'{park},{count[0]}\n'
    print(result)
    return result

    #raise NotImplementedError

def frequent_parks_count_dask(filename):
    '''
    Write a Python script using Dask that prints the list of the 10 parks
    with the highest number of treated trees. Parks must be ordered by
    decreasing number of treated trees and by alphabetical order when they have
    similar number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_dask.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    # load the CSV into dask dataframe
    dask_df = df.read_csv(filename, dtype=str)

    # remove rows with empty entries in the "Nom_parc" column
    dask_df = dask_df.dropna(subset=["Nom_parc"])

    # ?
    dask_df = dask_df.groupby("Nom_parc").count().compute()

    dask_df = dask_df.sort_values("Nom_arrond", ascending=False)
    dask_df = dask_df.head(10)

    result = ''
    for park, count in dask_df.iterrows():
        result += f'{park},{count[0]}\n'
    return result

    #raise NotImplementedError

def intersection_dask(filename1, filename2):
    '''
    Write a Python script using Dask that prints the alphabetically
    sorted list of parks that had trees treated both in 2016 and 2015. Every
    list element must be printed on a new line.
    Test file: tests/test_intersection_dask.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    # load the CSV into dask dataframe
    dask_df1 = df.read_csv(filename1, dtype=str)
    dask_df2 = df.read_csv(filename2, dtype=str)

    # remove rows with empty entries in the "Nom_parc" column
    dask_df1 = dask_df1.dropna(subset=["Nom_parc"])
    dask_df2 = dask_df2.dropna(subset=["Nom_parc"])

    dask_intersection = df.merge(dask_df1[["Nom_parc"]], dask_df2[["Nom_parc"]], on="Nom_parc", how="inner")

    dask_intersection = dask_intersection["Nom_parc"].unique().compute()

    result = ''
    for park in dask_intersection:
        result += f'{park}\n'
    return result

    #raise NotImplementedError
