from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import re


class common_config:

    def null_check(self, df, columns):

        '''this function checks for the null values in the dataframe
        :param df: The input dataframe
        :param columns: list of the columns in which the null character are present
        :return df : The with an additional column "null_check" that indicates if a null value is present'''
        try:
            # Add a new column 'null_check' with an empty string as the initial value
            df = df.withColumn("null_check", lit(''))

            # Loop through each collumn and update the 'null_check' column based onn null values in the column
            for column in columns:
                df = df.withColumn("null_check", when(df[column].isNull(), 'null').otherwise(df["null_check"]))

        except Exception as e:
            raise Exception(f"ERROR : While running null_check method Failed with an  exception {e}")
        return df






    def special_character_check(self,df, column_list):

        """This method retrieves the special characters from the dataframe.
        :param df: Input dataframe.
        :param column_list: List of columns in the dataframe.
        :return df: Dataframe with additional 'special_char' column."""
        try:
            special = "[^A-za-z-0-9|\|/|\s*]"
            df = df.withColumn('special_char', lit(''))
            for i in column_list:
                # it checks the special character of the column if special character is present it will extract and insert into the new column
                df = df.withColumn('special_char',
                                   when(col('special_char') == '', regexp_extract(i, special, 0)).otherwise(
                                       col('special_char')))

            # return the df which have additional special character column
            return df
        except Exception as e:
            print(f"ERROR: error occurred while running  special_character_check method failed. Exception :  {e}")

    def modify_column_names(self,df):
        def replace_caps_with_underscore(string):
            # Find capital letters using regex
            caps = re.findall(r'[A-Z]', string)

            # Replace capital letters with lowercase preceded by underscore
            for cap in caps:
                string = string.replace(cap, '_' + cap.lower())

            return string

        # Get the column names
        column_names = df.columns

        # Apply the function to each column name
        modified_column_names = [replace_caps_with_underscore(col) for col in column_names]

        # Rename the columns in the DataFrame
        for original_col, new_col in zip(column_names, modified_column_names):
            df = df.withColumnRenamed(original_col, new_col)

        return df

