from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.types import *
import pandas as pd

class SparkDataCheck:
    # initializing 
    def __init__(self, df):
        self.df = df

    @classmethod
    # this will read in a csv file while creating an instance SparkDataCheck
    def from_csv(cls, spark_session, filepath):
        df = spark_session.read.load(filepath,
                             format="csv",
                             sep=",", 
                             inferSchema="true",
                             header="true")
        return cls(df)
    
    @classmethod
    # this will create an instance of SparkDataCheck from a pandas dataframe
    def from_pd(cls, spark_session, pandas_df):
        df = spark_session.createDataFrame(pandas_df)
        return cls(df)
    
    #----VALIDATION METHODS ----
    
    # this METHOD will check if each value in a numeric column is within user defined limits
    #  and append a column of Boolean values.
    def check_col_range(self, column, lower=None, upper=None):
        # check for at least one upper/lower value
        if (lower is None) and (upper is None):
            print(f'Need a lower and/or an upper range for {column}!!!')
            return self
              
        #including all num types for dtype check
        num_types = ['float', 'int', 'longint', 'bigint', 'double', 'integer']
                   
        #check for dtype as number
        if dict(self.df.dtypes)[column] not in num_types:
            print("Column is not numeric!")
            return self
                   
        #do the comparison and append column with the result
        if lower is None:
            self.df = self.df.withColumn('num_check', F.col(f'`{column}`') <= upper)
        elif upper is None:
            self.df = self.df.withColumn('num_check', F.col(f'`{column}`') >= lower)
        else:
            self.df = self.df.withColumn('num_check', F.col(f'`{column}`').between(lower, upper))  
        return self
    
    # this METHOD will check a string column for values and append a boolean column
    def check_string(self, column, levels):
        # check for string type
        if dict(self.df.dtypes)[column] != 'string':
            print("Column is not a string!")
            return self

        # do the comparison to levels (NULL is built in)
        self.df = self.df.withColumn('str_check', F.col(f'`{column}`').isin(levels))
        return self
    
    # this METHOD will check for NULL values and append a column
    def check_null(self, column):
        #check for NULL value
        self.df = self.df.withColumn('null_check', F.col(f'`{column}`').isNull())
        return self
    
    #----- SUMMARIZATION METHODS----- 
    
    #This METHOD will report the min and max of a numeric column supplied 
    # by the user with and optional grouping variable
    def col_minmax(self, column=None, group=None):
        #including all num types for dtype check
        num_types = ['float', 'int', 'longint', 'bigint', 'double', 'integer']
        
        if column is not None:
            #check for dtype as number
            if dict(self.df.dtypes)[column] not in num_types:
                print(f"Column {column} is not numeric!")
                return None
               
            if group is None:
                result = self.df.agg(F.min(F.col(f'`{column}`')), F.max(F.col(f'`{column}`'))).toPandas()
                result.insert(0, 'col_name', column)
                result.columns = ['col_name', 'min', 'max']
                return result
            else:
                # column and group present
                result = (self.df.groupBy(group)
                          .agg(F.min(F.col(f'`{column}`')),
                               F.max(F.col(f'`{column}`')))).toPandas()
                result.insert(0, 'col_name', column)
                result.columns = ['col_name', group, 'min', 'max']
                return result
                
        # no column supplied so min/max for all numeric
        else:
            num_cols = []
                      
            # make a list of numeric columns 
            for c, t in self.df.dtypes:
                if t in num_types:
                    num_cols.append(c)
                       
            if group is None:
                # build list of min and max
                agg_exprs = []
                for c in num_cols:
                    agg_exprs.append(F.min(F.col(f'`{c}`')))
                    agg_exprs.append(F.max(F.col(f'`{c}`')))
                result = self.df.agg(*agg_exprs).toPandas()
    
                # reshape into a row per column
                rows = []
                for c in num_cols:
                    row = {'col_name': c, 
                           'min': result[f'min({c})'].iloc[0], 
                           'max': result[f'max({c})'].iloc[0]}
                    rows.append(row)
                return pd.DataFrame(rows)
            
            # if group but no column
            else:
                dfs = []
                
                for c in num_cols:
                    # create one df per column then combine
                    dfc = (self.df.groupBy(group)
                           .agg(F.min(F.col(f'`{c}`')).alias('min'),
                                F.max(F.col(f'`{c}`')).alias('max'))
                           .toPandas())
                    dfc['col_name'] = c
                    dfs.append(dfc)
                                            
                # combine into one dataframe
                result = pd.concat(dfs, ignore_index=True)
                return result
        
    # This METHOD will report the counts associated with one or two string columns
    def str_count(self, col1, col2=None):
        #initialize strings to false
        col1_string = False
        col2_string = False

        # check col1 is type string
        if dict(self.df.dtypes)[col1] != 'string':
            print(f"Column {col1} is numeric! Need a string.")
        else:
            col1_string = True
            
        #check col 2 
        if col2 is not None:
            if dict(self.df.dtypes)[col2] != 'string':
                print(f"Column {col2} is numeric! Need a string.")
            else:
                col2_string = True
        
        #both fail
        if not col1_string and not col2_string:
            return None
        
        #only column 1 passes
        if col2 is None or not col2_string:
            if col1_string:
                result = self.df.groupBy(col1).count().toPandas()
                return result
        #only column 2 passes
        if not col1_string:
            result = self.df.groupBy(col2).count().toPandas()
            return result

        #both are strings if you made it here
        result = self.df.groupBy(col1, col2).count().toPandas()
        return result
