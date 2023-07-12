#from _typeshed import Self
from datetime import datetime as dt
import time
import json
import pandas as pd
import numpy as np

class DataTransformation():
    def __init__(self, df, transformation_config, synonym_transformation_config, complex_synonym_transformation_config):
        self.df = df
        self.transformation_config = transformation_config
        self.synonym_transformation_config = synonym_transformation_config
        self.complex_synonym_transformation_config = complex_synonym_transformation_config

    """"" Add Columns """""
    def add_columns(self, new_column, input_string):
        self.df[new_column]= input_string
        return self.df

    """"" TimeFormatting Transform """""
    # convert string to date format if is not already in date data type
    def str_to_date(self, input_date, date_col_name, date_format):
        # convert data type
        self.df[input_date] = pd.to_datetime(self.df[input_date], format=date_format)
        # rename column if needed
        self.df.rename(columns={input_date:date_col_name}, inplace=True)
        return self.df
    
    # extract year from date and add a dedicate year column. Data type is Interger
    def add_year(self, input_date, year_col_name, date_format):
        # convert date column from string into date format if needed
        self.df[input_date] = pd.to_datetime(self.df[input_date], format=date_format)
        # add year column
        self.df[year_col_name] = self.df[input_date].dt.year
        return self.df

    # extract month from date and add a dedicate month column. Data type is Date
    def add_month(self, input_date, month_col_name='Month', date_format='%Y-%m-%d'):
        # convert date column from string into date format if needed
        self.df[input_date] = pd.to_datetime(self.df[input_date], format=date_format)
        # add month column
        self.df[month_col_name] = self.df[input_date] + pd.offsets.DateOffset(day=1)
        return self.df

    # extract week from date and add a dedicate week column. ISO week starts with a Monday and is Date data type
    def add_week(self, input_date, week_col_name='Week', date_format='%Y-%m-%d'):
        # convert date column from string into date format if needed
        self.df[input_date] = pd.to_datetime(self.df[input_date], format=date_format)
        # add week column
        self.df[week_col_name] = self.df[input_date].dt.to_period('W').apply(lambda r: r.start_time)
        return self.df

    """"" Concatenation Transform """""   
    def concat(self, columns, resultColumn, separator, bool = "True"):
        try:
            if len(columns) != 2:
                raise Exception('Wrong number of columns supplied(should be 2 columns')
            self.df[resultColumn] = self.df[columns[0]].str.upper() + separator + self.df[columns[1]].str.upper()
            # Keep the original columns
            if bool == "True":
                return self.df
            # Drop the original columns
            elif bool == "False":
                self.df.drop(columns, axis=1, inplace=True)  
                return self.df 
        except (KeyError, ValueError) as error:
            print(error.args)
            raise

    """"" Sum Transform """""
    def sum_columns(self, cols_to_sum:list, new_col_name:str, keep_orginal_cols = "True"):
        try:
            # raise exception if cols_to_sum is not a list as expected
            if type(cols_to_sum) != list:
                raise KeyError('A list variable is expected for cols_to_sum.')
            # raise exception if cols_to_sum do not exist in data frame
            elif not set(cols_to_sum).issubset(self.df.columns):        
                raise KeyError('Columns do not exist in dataframe.')

            # convert columns to numeric, if fail then raise exception
            # add a new column by summing up the input ones
            # fill na with 0
            self.df[new_col_name] = self.df[cols_to_sum].apply(pd.to_numeric, errors='raise')\
                                                .sum(axis=1)\
                                                .fillna(0)
            # drop the original columns if wanted
            if keep_orginal_cols == "False":
                self.df = self.df.drop(cols_to_sum, axis=1, inplace=True)
            return self.df
            
        except (KeyError, ValueError) as error:
            print(error.args)
            raise
    
    """"" Null Transform """""
    #The purpose of this function is to corrently process data where a column is null
    #a function to select a related column with a value must be implemented
    def contains_null_func(self, column1, column2, new_col_name,keep_orginal_cols=True):
        try:
            #The case of column doesn't exist
            if column1 not in self.df.columns:
                self.df[column1] = np.NaN
                # if values in column1 has null, column 2 will fill null value in column1 
                if self.df[column1].isnull().all() == True:
                    self.df[new_col_name] = self.df[column1].fillna(self.df[column2])
                else:
                    self.df[new_col_name]= self.df[column1]
            
            #column exist
            else:
                if self.df[column1].isnull().any() == True:
                    self.df[new_col_name] = self.df[column1].fillna(self.df[column2])
                else:
                    self.df[new_col_name]= self.df[column1]

            # drop the original columns if wanted
            if keep_orginal_cols == False:
                self.df.drop(column1, axis=1, inplace=True)
                self.df.drop(column2, axis=1, inplace=True)
           
            return self.df
                
        except (KeyError, ValueError) as error:
                # print error
                print(error.args)
                raise

    """"" Duplicate Columns """""
    def duplicate_columns(self, current_col_name, new_col_name):
        self.df[new_col_name] = self.df[current_col_name]
        return self.df

    """"" Rename Columns """""
    def rename_columns(self, renaming_columns):
        self.df = self.df.rename(columns = renaming_columns)
        return self.df

    """"" Select Columns """""
    def select_columns(self, selected_columns):
        self.df = self.df[selected_columns]
        return self.df

    """"" Synonym Renaming """""
    def synonym_renaming(self):
        columns = [column for column in self.df.columns.tolist() if column.lower() in list(self.synonym_transformation_config.keys())]
        for column in columns:
            for key, value in self.synonym_transformation_config[column.lower()].items():
                self.df[column] = np.where(self.df[column].isin(value), key, self.df[column])
        # Exceptional cases
        # There are some cases in the data that we cannot use the synonym config to transform data
        # These hard codes below can solve that
        if set(["Subproduct", "Creative_Format", "Audience_Funnel_Stage"]).issubset(set(self.df.columns.tolist())):
            self.df["Subproduct"] = self.df["Subproduct"].apply(lambda i: np.nan if i == "" else i)
            self.df["Creative_Format"] = self.df["Creative_Format"].apply(lambda i: "OTHER" if str(i).lower() == "nan" else i)
            self.df["Audience_Funnel_Stage"] = self.df["Audience_Funnel_Stage"].apply(lambda i: "OTHER" if str(i).lower() == "nan" else i)
        return self.df

    """"" Complex Synonym Renaming """""
    def complex_synonym_renaming(self):
        # Add "CATEGORY" column
        self.df["CATEGORY"] = "PORTFOLIO"
        for column in list(self.complex_synonym_transformation_config.keys()):
            df_columns = self.df.columns.tolist()
            df_columns.append("CATEGORY")
            if column in df_columns:
                for item in self.complex_synonym_transformation_config[column]:
                    if len(item) == 2:
                        column1 = list(item["condition1"].keys())[0]
                        value1 = list(item["condition1"].values())[0]
                        if column1 in self.df.columns.tolist():
                            self.df[column] = np.where(self.df[column1].isin(value1), item["value"], self.df[column])
                    if len(item) == 3:
                        column1 = list(item["condition1"].keys())[0]
                        column2 = list(item["condition2"].keys())[0]
                        value1 = list(item["condition1"].values())[0]
                        value2 = list(item["condition2"].values())[0]
                        if column1 in self.df.columns.tolist() and column2 in self.df.columns.tolist():
                            self.df[column] = np.where(self.df[column1].isin(value1) & self.df[column2].isin(value2), item["value"], self.df[column])
        # Exceptional cases
        # There are some cases in the data that we cannot use the complex synonym config to transform data
        # These hard codes below can solve that
        if "Campaign" in self.df.columns.tolist():
            self.df["CATEGORY"] = self.df[["CATEGORY", "Campaign"]].apply(lambda i: "HHP" if "VODAFONE" in str(i[1]) else i[0], axis = 1)
        if "Product" in self.df.columns.tolist():
            self.df["CATEGORY"] = self.df[["CATEGORY", "Product"]].apply(lambda i: "WEARABLES" if str(i[1]) in ["WEARABLES", "WEARABLE"] else i[0], axis = 1)
            self.df["CATEGORY"] = self.df[["CATEGORY", "Product"]].apply(lambda i: "HHP" if str(i[1]) in ["CO_OP", "CO-OP"] else i[0], axis = 1)
        if set(["Product", "Subproduct"]).issubset(self.df.columns.tolist()):
            self.df["CATEGORY"] = self.df[["CATEGORY", "Product", "Subproduct"]].apply(lambda i: "AV" if str(i[1]) == "AUDIO" and "BUDS" not in str(i[2]) else i[0], axis = 1)
            self.df["CATEGORY"] = self.df[["CATEGORY", "Product", "Subproduct"]].apply(lambda i: "WEARABLES" if str(i[1]) == "AUDIO" and "BUDS" in str(i[2]) else i[0], axis = 1)
            self.df["Product"] = self.df[["Product", "Subproduct"]].apply(lambda i: "WEARABLE" if "BUDS" in str(i[1]) else i[0], axis = 1)
            self.df["Product"] = self.df[["Product", "Subproduct"]].apply(lambda i: "PORTFOLIO" if str(i[0]) == "OTHER" and "BUDS" not in str(i[1]) else i[0], axis = 1)
            self.df["Product"] = self.df[["Product", "Subproduct"]].apply(lambda i: "PORTFOLIO" if str(i[0]) == "OTHER" and "TAB-ACTIVE3" not in str(i[1]) else i[0], axis = 1)
        if set(["Creative", "Subproduct"]).issubset(self.df.columns.tolist()):
            self.df["Creative"] = self.df[["Creative", "Subproduct"]].apply(lambda i: "PREORDER" if str(i[0]) == "NULL" and str(i[1]) == "S20" else i[0], axis = 1)
            self.df["Creative"] = self.df[["Creative", "Subproduct"]].apply(lambda i: "PREORDER" if str(i[0]) == "nan" and str(i[1]) == "S20" else i[0], axis = 1)
        return self.df 
   
    """"" Upper Values """""
    def upper_values(self):
        upper_columns = [column for column in self.df.columns.tolist() if column != "Downloaded_From"]
        self.df[upper_columns] = self.df[upper_columns].apply(lambda i: i.upper() if type(i) == str else i)
        return self.df

    """"" Change Data Type """""
    def data_type_formatting(self, column, dtype):
        self.df[column] = self.df[column].replace('', 0.0, regex=True) 
        self.df = self.df.astype({column: dtype}, errors='raise')
        return self.df

    """"" Drop Redundant Columns """""
    def drop_columns(self, columns):
        self.df = self.df.drop(columns, axis = 1)
        return self.df

    """"" Aggregate Data """""
    def aggregate_data(self, columns):
        self.df = self.df.groupby(columns, dropna = False, as_index = 0).sum()
        return self.df

    # main
    def run_transformation(self):
     
        """"" Add columns for fixed string """""
        if "add_columns" in self.transformation_config.keys():
            add_column_transform = self.transformation_config["add_columns"]
            for item  in add_column_transform:
                df = self.add_columns(item["new_column"], item["input_string"])

        """"" TimeFormatting Transform """""
        if "str_to_date" in self.transformation_config.keys():
            df = self.str_to_date(*list(self.transformation_config["str_to_date"].values()))
        if "add_year" in self.transformation_config.keys():
            df = self.add_year(*list(self.transformation_config["add_year"].values()))
        if "add_month" in self.transformation_config.keys():
            df = self.add_month(*list(self.transformation_config["add_month"].values()))
        if "add_week" in self.transformation_config.keys():
            df = self.add_week(*list(self.transformation_config["add_week"].values()))

        """"" Concatenation Transform """""
        if "concat" in self.transformation_config.keys():
            concat_transform = self.transformation_config['concat']
            for item in concat_transform:
                self.concat(item["columns"], item["resultColumn"], item["separator"], item["bool"])

        """"" Sum Transform """""
        if "sum_columns" in self.transformation_config.keys():
            sum_columns_transformation = self.transformation_config['sum_columns']
            for item in sum_columns_transformation:
                self.sum_columns(item["cols_to_sum"],item["new_col_name"],item["keep_orginal_cols"])

        """"" Null Transform """""
        if "contains_null_func" in self.transformation_config.keys():
            contains_null_transform =self.transformation_config["contains_null_func"]
            for item in contains_null_transform:
                self.contains_null_func(item["column1"], item["column2"], item["new_col_name"], item["keep_orginal_cols"])

        """"" Duplicate Columns """""
        if "duplicate_columns" in self.transformation_config.keys():
            for item in self.transformation_config["duplicate_columns"]:
                df = self.duplicate_columns(item["current_column_name"], item["new_column_name"])

        """"" Rename Columns """""
        if "rename_columns" in self.transformation_config.keys():
            df = self.rename_columns(self.transformation_config["rename_columns"])

        """"" Data Type Formatting """""
        if "data_type_formatting" in self.transformation_config.keys():
            for item in self.transformation_config["data_type_formatting"]:
                df= self.data_type_formatting(item["column"],item["dtype"]) 
            
        """"" Select Columns """""
        if "select_columns" in self.transformation_config.keys():
            df = self.select_columns(self.transformation_config["select_columns"])

        """"" Synonym Renaming """""
        df = self.synonym_renaming()

        """"" Complex Synonym Renaming """""
        df = self.complex_synonym_renaming()

        """"" Upper Values """""
        df = self.upper_values()
    
        """"" Data Type Formatting """""
        if "data_type_formatting" in self.transformation_config.keys():
            for item in self.transformation_config["data_type_formatting"]:
                df= self.data_type_formatting(item["column"],item["dtype"]) 

        """"" Drop Redundant Columns """""
        if "redundant_columns" in self.transformation_config.keys():
            df = self.drop_columns(self.transformation_config["redundant_columns"])

        """"" Aggregate Data """""
        if "aggregated_dimensions" in self.transformation_config.keys():
            df = self.aggregate_data(self.transformation_config["aggregated_dimensions"])

        return df
