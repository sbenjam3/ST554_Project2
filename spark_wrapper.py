from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.types import *
import pandas as pd


class SparkDataCheck:
    
    def __init__(self, dataframe=None):
        self.df = dataframe
    
    def instance_from_csv(self, spark, file_path: str):   
        """
        Creates a spark SQL dataframe from a csv file.
        
        Args:
            spark (SparkSession): the spark session used for the proceeding commands.
            file_path (str): the path to the csv file.
        
        Returns:
            self: returns the instance of the SparkDataCheck object.
        """
        #load csv file as spark DataFrame
        self.df = spark.read.load(file_path,
                     format="csv", 
                     sep=",", 
                     inferSchema="true", 
                     header="true")
        return self 
                    
    def instance_from_pd(self, spark, pd_dataframe: pd.DataFrame):
        """
        Creates a spark SQL dataframe from a pandas dataframe. 
        
        Args:
            spark (SparkSession): the spark session used for the proceeding commands.
            pd_dataframe (pd.DataFrame): pandas dataframe of data.
            
        Returns:
            self: returns the instance of the SparkDataCheck object. 
        """
        #load pandas DataFrame as spark DataFrame 
        self.df = spark.createDataFrame(pd_dataframe)
        return self

    def numeric_limit_check(self, column: str, upper=None, lower=None):
        """
        Checks if each value in a numeric column is within the user defined limits. 
        
        Args:
            column (str): name of the numeric column.
            upper (int, float): upper limit to check. 
            lower (int, float): lower limit to check.
        
        Returns:
            self: return the instance of the SparkDataCheck object. 
        """
        #get dictionary of columns and data types
        types = dict(self.df.dtypes)   
        
        #set column_type as the data type of the column that was passed in
        column_type = types[column]
        
        #check if the column is a numeric data type
        if column_type != 'bigint' and column_type != 'int' and column_type != 'longint' and column_type != 'double' and column_type != 'integer':
            print('Please enter a numeric column')
            return self
        #check that at least an upper or lower bound is given
        if upper is None and lower is None:
            print('Please provide and upper or lower bound')
            return self        
        elif(lower is None):
            #if only upper is given, find if values are less than or equal to the upper bound 
            #create new column with True or False or NULL if original value is NULL
            self.df = self.df.withColumn(f"{column}_bounds", F.when(F.isnull(self.df[column]), None).when(self.df[column] <=upper, True).otherwise(False))
            return self
        elif(upper is None):
            #if only lower is given, find if values are greater than or equal to the lower bound 
            #create new column with True or False or NULL if original value is NULL
            self.df = self.df.withColumn(f"{column}_bounds", F.when(F.isnull(self.df[column]), None).when(self.df[column] >=lower, True).otherwise(False))
            return self
        else:
            #if both upper and lower bound are give, check that the value is between the bounds
            #create new column with True or False or NULL if original value is NULL 
            self.df = self.df.withColumn(f"{column}_bounds", F.when(F.isnull(self.df[column]), None).when(self.df[column].between(lower,upper), True).otherwise(False))
            return self

    def string_limit_check(self, column: str, string: str):
        """
        Checks if each value in a string column falls within the defined set of levels. 
        
        Args:
            column (str): name of the numeric column.
            string (str): string to check column value against. 
        
        Returns:
            self: return the instance of the SparkDataCheck object. 
        """
        #get dictionary of column data types
        types = dict(self.df.dtypes)
        
        #check if the data type of the supplied column is a string 
        if types[column] != 'string':
            print('Please enter a string column')
            return self
        
        #check if the value of the column matched or is in the supplied string 
        #return True or False or NULL if the original value is NULL
        self.df = self.df.withColumn(f"{column}_check", F.when(F.isnull(self.df[column]), None).when(self.df[column].isin(string),True).otherwise(False))
        
        return self 

    def missing_check(self, column: str):
        """
        Checks if each value in a column is missing (is NULL). 
        
        Args:
            column (str): name of the column to check. 
        
        Returns:
            self: return the instance of the SparkDataCheck object. 
        """
        #check which column values are missing or NULL
        self.df = self.df.withColumn("missing_check", F.isnull(self.df[column]))
        return self 
        
    def min_and_max(self, main_column: str = None , grouping_column: str = None):
        """
        Reports the min and max of a numeric column. 
        
        Args:
            main_column (str): primary column to calculate the min and max of. 
            grouping_column (str): optional column to group main column by. 
            
        Returns:
            Spark SQL dataframe. 
        """
        if main_column is not None:
            
            #get the data types for each column
            types = dict(self.df.dtypes)   
            #get the data type for the main column
            column_type = types[main_column]
            
            #check that the main column given is a numeric data type, if not print an error message
            if column_type != 'bigint' and column_type != 'int' and column_type != 'longint' and column_type != 'double' and column_type != 'integer':
                print('Please enter a numeric column')
                return None
            #if a main column is specified WITHOUT a grouping column 
            if grouping_column is None:
                #return min and max of the given column
                return self.df.agg(F.min(main_column).alias(f"{main_column}_min"),F.max(main_column).alias(f"{main_column}_max"))
            #if a main column is specified WITH a grouping column
            else:
                #return min and max of the given main column grouped by the grouping column
                return self.df.groupby(grouping_column).agg(F.min(main_column).alias(f"{main_column}_min"),F.max(main_column).alias(f"{main_column}_max"))
        else:
            if grouping_column is None:
                #create a dictionary to hold the sum and mean statsitics for each column
                master_stats = {}
                #create list of Numeric column names 
                numeric_cols = [c for c, t in self.df.dtypes if t.startswith('string')==False]
                #for each column run aggregation functions and collect values 
                for column in numeric_cols:
                    stats = self.df.agg(F.min(column), F.max(column)).collect()
                    #add statistics to master dictionary
                    master_stats[column] = {'min' : stats[0][0], 'max' : stats[0][1]}
                #create and return pandas dataframe made from dictionary 
                return pd.DataFrame(master_stats)
                
            else:
                #create list of Numeric column names 
                numeric_cols = [c for c, t in self.df.dtypes if t.startswith('string')==False]
                #for each column run aggregation functions and collect values 
                for index,column in enumerate(numeric_cols):
                    #create a dictionary per column to hold values 
                    column_dict = {'group_value' : [], f'min_{column}' : [], f'max_{column}' : []}
                    #collect list of statistics for the column
                    stats = self.df.groupby(grouping_column).agg(F.min(column), F.max(column)).collect()
                    #add statistics to column dictionary
                    for x in range(len(stats)):
                        column_dict['group_value'].append(stats[x][0])
                        column_dict[f'min_{column}'].append(stats[x][1])
                        column_dict[f'max_{column}'].append(stats[x][2])
                    #append column_dict to master_dict
                    if index == 0:
                        master_pd_dataframe = pd.DataFrame(column_dict) 
                    else:
                        temp_df = pd.DataFrame(column_dict)
                        master_pd_dataframe = pd.merge(master_pd_dataframe, temp_df, on = 'group_value')
                return master_pd_dataframe
        
    def string_counts(self, main_column, grouping_column = None):
        """
        Reports the counts associated with one or tow string columns.
        
        Args:
            main_column (str): primary column to calculate the min and max of. 
            grouping_column (str): optional column to group main column by. 
            
        Returns:
            Spark SQL dataframe. 
        """
        #get the data types of the columns in the DataFrame 
        types = dict(self.df.dtypes)
        
        #if a grouping column is provided, check that it is a string type 
        if grouping_column is not None:
            if types[grouping_column] != 'string':
                print("Please enter a string column")
                return None 
            
        #check that the main column provided is a string 
        if types[main_column] != 'string':
            print("Please enter a string column")
            return None 
        
        #if no grouping column is given, get the counts of the specified column
        if grouping_column is None:
            return self.df.select(F.count(self.df[main_column]))
        #if a grouping column is given. get the counts of the specified column grouped by the grouping column 
        else:
            return self.df.groupBy(grouping_column).agg(F.count(main_column))

        
        
        
        

        
        

            
    
    
        
        
        