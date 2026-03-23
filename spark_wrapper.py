from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.types import *
import pandas as pd


class SparkDataCheck:
    
    def __init__(self, dataframe=None):
        
        self.df = dataframe
    
    def instance_from_csv(self, spark, file_path):        
        self.df = spark.read.load(file_path,
                     format="csv", 
                     sep=",", 
                     inferSchema="true", 
                     header="true")
        return self 
                    
    def instance_from_pd(self, spark, pd_dataframe):
        self.df = spark.createDataFrame(pd_dataframe)
        return self

    def numeric_limit_check(self, column, upper=None, lower=None):
        #!!!deal with NULL values!!!
        types = dict(self.df.dtypes)   
        
        column_type = types[column]
    
        if column_type != 'bigint' and column_type != 'int' and column_type != 'longint' and column_type != 'double' and column_type != 'integer':
            print('Please enter a numeric column')
            return self
        if upper == None and lower == None:
            print('Please provide and upper or lower bound')
            return self        
        elif(lower == None):
            self.df = self.df.withColumn(f"{column}_bounds", F.when(self.df[column] <=upper, True).otherwise(False))
            return self
        elif(upper == None):
            self.df = self.df.withColumn(f"{column}_bounds", F.when(self.df[column] >=lower, True).otherwise(False))
            return self
        else:
            self.df = self.df.withColumn(f"{column}_bounds", F.when(self.df[column].between(lower,upper), True).otherwise(False))
            return self

    def string_limit_check(self, column, string):
        #should string be a list??
        #!!!deal with NULL values!!!
        types = dict(self.df.dtypes)
        
        if types[column] != 'string':
            print('Please enter a string column')
            return self
        
        self.df = self.df.withColumn(f"{column}_check", F.when(self.df[column].isin(string),True).otherwise(False))
        
        return self 

    def missing_check(self, column):
        self.df = self.df.withColumn("missing_check", F.isnull(self.df[column]))
        return self 
'''        
    def min_and_max(self, column=None, grouping=None):
        types = dict(df.dtypes())

        if types[column] != 'float' or 'int' or 'longint' or 'bigint' or 'double' or 'integer':
            print('Please enter a numeric column')
            return None
        
    def string_counts(self, column_1, column_2 = None):
        types = dict(df.dtypes())
        
        if column_2 != None:
            if types[column_2] != 'string':
                print("Please enter a string column")
                return None 
        if types[column_1] != 'string':
            print("Please enter a string column")
            return None 
        
        if column_2 == None:
            return df.column1.count() 
        elif:
            df.column_1.groupBy(column_2).count()
 '''           
        
        
        
        

        
        

            
    
    
        
        
        