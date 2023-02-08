# Databricks notebook source
def sum_two_numbers(a, b):
    print(a+b)
    
# Pivot by indicator, and fill missing values.
def pivot_and_clean(pdf, fillna):
    pdf["value"] = pd.to_numeric(pdf["value"])
    pdf = pdf.fillna(fillna).pivot_table(
        values="value", columns="indicator", index="date"
    )
    return pdf


# Create column names that are compatible with Delta tables.
def clean_spark_cols(pdf):
    pdf.columns = pdf.columns.str.replace(" ", "_")
    return pdf


# Convert index to column (works with pandas API on Spark, too).
def index_to_col(df, colname):
    df[colname] = df.index
    return df
