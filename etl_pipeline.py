from pyspark.sql import SparkSession, Window
from pyspark.sql.types import DateType, IntegerType
import pyspark.sql.functions as F
from pyspark.sql.functions import datediff, col

#start spark Session
spark = SparkSession.builder.getOrCreate()

accounts_df = spark.read.options(header=True, inferschema=True).csv('extracts/accounts.csv')
invoice_df = spark.read.options(header=True, inferschema=True).csv('extracts/invoices.csv')
invoice_line_df = spark.read.options(header=True, inferschema=True).csv('extracts/invoice_line_items.csv')
skus_df = spark.read.options(header=True, inferschema=True).csv('extracts/skus.csv')



#inv_total: Total value (in dollars) for the invoice
#inv_items: Total number of items in the invoice

df0 = invoice_line_df.join(skus_df, on='item_id') \
    .withColumn('total_cost', col('quantity') * col('item_retail_price')).groupby('invoice_id') \
    .agg(
        F.round(F.sum('total_cost'), 2).alias('inv_total'), \
        F.sum('quantity').alias('inv_items'))

# acct_age: The age of the account (in days) at the time the invoice was issued (i.e. the number of days between when the account was set up, and the invoice date)
df1 = invoice_df.join(accounts_df, on='account_id').select('account_id', 'invoice_id', 'payment_dates', 'date_issued', 'joining_date') \
        .withColumn('acct_age', datediff('date_issued', 'joining_date'))


#number of invoices for account in 120 days prior to invoice's issuing date
#partition by account_id, order by account age and limit frame to row preceeding current row to 120 days prior

#cum_tot_inv_acct: The cumulative number of invoices for the account up to date that the invoice was issued
#partition by account_id, order by date_issued (or account_age)

#is_late: A flag to indicate if the invoice was paid late (i.e. more than 30 days after issuing). Contains 1 if invoice was paid late, 0 otherwise.
# df4.select(datediff(df4.payment_dates, df4.date_issued).alias('payment_interval'), F.when(datediff(df4.payment_dates, df4.date_issued) > 30, 1).otherwise(0).alias('is_late')).show()

windowspec_0 = Window.partitionBy('account_id').orderBy(F.asc('acct_age')).rangeBetween(-120, 0)
windowspec_1 = Window.partitionBy('account_id').orderBy(F.asc('date_issued'))


df2 = df1.withColumn('num_inv_120d', F.count('acct_age').over(windowspec_0)).\
                     withColumn('cum_tot_inv_acct', F.count('date_issued').over(windowspec_1)). \
                     withColumn('is_late', F.when(datediff('payment_dates', 'date_issued') > 30, 1).otherwise(0))

df = df0.join(df2, on='invoice_id').select(
                                            F.col('invoice_id').alias('inv_id'), 
                                            F.col('account_id').alias('acct_id'), 
                                            F.col('inv_total'),
                                            F.col('inv_items'),
                                            F.col('acct_age'),
                                            F.col('num_inv_120d'),
                                            F.col('cum_tot_inv_acct'),
                                            F.col('is_late')
                                            )
df.show()

# Write DataFrame to CSV with header and comma delimiter
df.sort([F.asc('acct_id'), F.asc('acct_age')]).toPandas().to_csv("output.csv")






