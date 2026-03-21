from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.types import *
import pandas as pd


class SparkDataFrame:
    
    def __init__(self, dataframe):
        self.df = dataframe
        
    def instance_from_csv(self, spark, file_path):
        self.df = spark.read.load(file_path,
                     format="csv", 
                     sep=",", 
                     inferSchema="true", 
                     header="true")
        return SparkDataFrame
    
    def instance_from_pd(self, spark, pd_dataframe):
        self.df = spark.CreateDataFrame(pd_dataframe)
        return SparkDataFrame
    
    def numeric_limit_check(self, column, upper=None, lower=None):
        
        types = dict(df.dtypes())
        
        if types[column] != 'float' or 'int' or 'longint' or 'bigint' or 'double' or 'integer':
            print('Please enter a numeric column')
            return SparkDataFrame
        
        if upper == None and lower == None:
            print('Please provide and upper or lower bound')
        elif(lower == None):
            self.df = self.df.withColumn(f"{column}_bounds", when(df.column <=upper, True).otherwise(False))
        elif(uuper == None):
            self.df = self.df.withColumn(f"{column}_bounds", when(df.column>=lower, True).otherwise(False))
        else:
            self.df = self.df.withColumn(f"{column}_bounds", when(df.column.between(lower,upper), True).otherwise(False))
            
        return SparkDataFrame
    
    def string_limit_check(self, column, string):
        
        types = dict(df.dtypes())
        
        if types[column] != 'string':
            print('Please enter a string column')
            return SparkDataFrame
        
        self.df = self.df.withColumn(f"{column}_check", when(df.column.isin(string),True).otherwise(False))
        
    def missing_check(self, column):
        self.df = self.df.withColumn(df.column.isNULL())
        
    def min_and_max(self, column=None, grouping=None):
        types = dict(df.dtypes())

        if types[column] != 'float' or 'int' or 'longint' or 'bigint' or 'double' or 'integer':
            print('Please enter a numeric column')
            return None
        

            
    
    
        
        
        