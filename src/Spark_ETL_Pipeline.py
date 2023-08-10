import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialise spark session
spark = SparkSession.builder.appName("Spark_ETL_Pipeline").getOrCreate()

def main():

    accounts = spark.read.csv("Spark_ETL_Pipeline/data_extracts/accounts.csv", header=True, sep=",")
    invoice_line_items = spark.read.csv("Spark_ETL_Pipeline/data_extracts/invoice_line_items.csv", header=True, sep=",")
    invoices = spark.read.csv("Spark_ETL_Pipeline/data_extracts/invoices.csv", header=True, sep=",")
    skus = spark.read.csv("Spark_ETL_Pipeline/data_extracts/skus.csv", header=True, sep=",")

    # Create sql table alias
    accounts.createOrReplaceTempView("ACC")
    invoices.createOrReplaceTempView("INV")
    invoice_line_items.createOrReplaceTempView("ILI")
    skus.createOrReplaceTempView("SKU")
    
    # Feature engineering
    df_basic = spark.sql("select    INV.*,\
                                    ILI.item_id, ILI.quantity,\
                                    SKU.item_retail_price,\
                                    ACC.joining_date,\
                                    item_retail_price * quantity as inv_total_item_price,\
                                    datediff(day, ACC.joining_date, INV.date_issued) as acct_age,\
                                    datediff(day, INV.payment_dates, INV.date_issued) as pment_diff,\
                                    case when datediff(day, INV.date_issued, INV.payment_dates) > 30 then 1 else 0 end as is_late\
                            from    INV\
                            full outer join ILI on INV.invoice_id == ILI.invoice_id\
                            full outer join SKU on ILI.item_id == SKU.item_ID\
                            full outer join ACC on INV.account_id == ACC.account_id")

    # Create sql table alias
    df_basic.createOrReplaceTempView("df")
    
    # Final features
    df_final = spark.sql("select   invoice_id as inv_id,\
                                    account_id as acct_id,\
                                    round(sum(item_retail_price * quantity),2) as inv_total,\
                                    cast(sum(quantity) as int) as inv_items,\
                                    acct_age,\
                                    count(invoice_id) over (partition by account_id order by cast(date_issued as timestamp) range between interval 120 days preceding and current row) as num_inv_120d,\
                                    count(invoice_id) over (partition by account_id order by date_issued) as cum_tot_inv_acct,\
                                    is_late\
                            from df\
                            group by invoice_id, account_id, date_issued, acct_age, is_late\
                            order by account_id, date_issued asc")

    # Write to csv
    df_final.toPandas().to_csv("Spark_ETL_Pipeline/data_outputs/pipeline_output.csv", index=False, header=True)
    
if __name__ == "__main__":
    main()