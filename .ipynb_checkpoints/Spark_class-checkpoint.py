from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.types import *
import pandas as pd


class SparkDataCheck:

    def __init__(self, df):
        self.df = df

    @classmethod
    def from_csv(cls, filepath, spark_session):
        df = spark_session.read.load(filepath, format="csv", header=True, inferSchema=True)
        return cls(df)

    @classmethod
    def from_pandas(cls, pandas_df, spark_session):
        df = spark_session.createDataFrame(pandas_df)
        return cls(df)

    def check_null(self, column):
        null_flag = F.col(f'`{column}`').isNull()
        result = self.df.withColumn('null_flag', null_flag)
        return result

    def check_col_range(self, column, lower=None, upper=None):
        dtype = dict(self.df.dtypes)[column]
        numeric_types = ['int', 'bigint', 'float', 'double', 'decimal', 'long', 'short']
        is_numeric = any(dtype.startswith(t) for t in numeric_types)

        if not is_numeric:
            print(f"Column '{column}' is not numeric.")
            return self

        col_ref = F.col(f'`{column}`')

        if lower is not None and upper is not None:
            range_flag = ~col_ref.between(lower, upper)
        elif lower is not None:
            range_flag = col_ref < lower
        elif upper is not None:
            range_flag = col_ref > upper
        else:
            range_flag = F.lit(False)

        result = self.df.withColumn('range_flag', range_flag)
        return result

    def check_col_string(self, column, levels):
        dtype = dict(self.df.dtypes)[column]

        if dtype != 'string':
            print(f"Column '{column}' is not a string column.")
            return self

        col_ref = F.col(f'`{column}`')
        string_flag = ~col_ref.isin(levels)
        result = self.df.withColumn('string_flag', string_flag)
        return result

    def col_minmax(self, column=None, group_by=None):
        numeric_types = ['int', 'bigint', 'float', 'double', 'decimal', 'long', 'short']
        dtypes_dict = dict(self.df.dtypes)

        def is_numeric(col_name):
            return any(dtypes_dict[col_name].startswith(t) for t in numeric_types)

        # Single column supplied
        if column is not None:
            if not is_numeric(column):
                print(f"Column '{column}' is not numeric.")
                return None

            col_ref = F.col(f'`{column}`')

            if group_by is not None:
                result = (self.df
                          .groupBy(group_by)
                          .agg(F.min(col_ref).alias('min'),
                               F.max(col_ref).alias('max')))
                result_pd = result.toPandas()
                result_pd.insert(0, 'col_name', column)
            else:
                result = self.df.agg(F.min(col_ref).alias('min'),
                                     F.max(col_ref).alias('max'))
                result_pd = result.toPandas()
                result_pd.insert(0, 'col_name', column)

            return result_pd

        # No column supplied — all numeric columns
        else:
            numeric_cols = [c for c in self.df.columns if is_numeric(c)]

            if group_by is not None:
                dfs = []
                for c in numeric_cols:
                    col_ref = F.col(f'`{c}`')
                    temp = (self.df
                            .groupBy(group_by)
                            .agg(F.min(col_ref).alias('min'),
                                 F.max(col_ref).alias('max')))
                    temp_pd = temp.toPandas()
                    temp_pd.insert(0, 'col_name', c)
                    dfs.append(temp_pd)
                result_pd = reduce(lambda left, right: pd.merge(left, right, how='outer'), dfs)
            else:
                dfs = []
                for c in numeric_cols:
                    col_ref = F.col(f'`{c}`')
                    temp = self.df.agg(F.min(col_ref).alias('min'),
                                       F.max(col_ref).alias('max'))
                    temp_pd = temp.toPandas()
                    temp_pd.insert(0, 'col_name', c)
                    dfs.append(temp_pd)
                result_pd = pd.concat(dfs, ignore_index=True)

            return result_pd

    def str_count(self, column1, column2=None):
        dtypes_dict = dict(self.df.dtypes)

        col1_valid = False
        col2_valid = False

        # Check column1
        if dtypes_dict.get(column1) == 'string':
            col1_valid = True
        else:
            print(f"Column '{column1}' is not a string column.")

        # Check column2 if supplied
        if column2 is not None:
            if dtypes_dict.get(column2) == 'string':
                col2_valid = True
            else:
                print(f"Column '{column2}' is not a string column.")

        # If both invalid, return None
        if not col1_valid and (column2 is None or not col2_valid):
            return None

        results = []

        if col1_valid:
            count1 = (self.df
                      .groupBy(F.col(f'`{column1}`'))
                      .count()
                      .toPandas())
            count1.columns = [column1, 'count']
            results.append(count1)

        if column2 is not None and col2_valid:
            count2 = (self.df
                      .groupBy(F.col(f'`{column2}`'))
                      .count()
                      .toPandas())
            count2.columns = [column2, 'count']
            results.append(count2)

        if len(results) == 1:
            return results[0]
        else:
            return results
