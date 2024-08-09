from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from common_utils import common_config

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    cmn_ut = common_config()

    stock_ledger_df = spark.read.format("csv") \
        .option("header", "True") \
        .option("inferschema", "True") \
        .option("mode", "PERMISSIVE") \
        .load(r"C:\Users\Venkatesh\OneDrive\Desktop\oil & gas\DATASETS\MIDSTREAM\Data\StockLeadger.csv")
    # stock_ledger_df.printSchema()
    # stock_ledger_df.show()



    ESandH_df = spark.read.format("csv") \
        .option("header", "True") \
        .option("inferschema", "True") \
        .option("mode", "PERMISSIVE") \
        .load(r"C:\Users\Venkatesh\OneDrive\Desktop\oil & gas\DATASETS\MIDSTREAM\Data\ES&H.csv")
    # stock_ledger_df.printSchema()
    # ESandH_df.show()

    facility_unit_df = spark.read.format("csv") \
        .option("header", "True") \
        .option("inferschema", "True") \
        .option("mode", "PERMISSIVE") \
        .load(r"C:\Users\Venkatesh\OneDrive\Desktop\oil & gas\DATASETS\MIDSTREAM\Data\facility_unit.csv")
    # stock_ledger_df.printSchema()
    # facility_unit_df.show()

    orders_df = spark.read.format("csv") \
        .option("header", "True") \
        .option("inferschema", "True") \
        .option("mode", "PERMISSIVE") \
        .load(r"C:\Users\Venkatesh\OneDrive\Desktop\oil & gas\DATASETS\MIDSTREAM\Data\orders.csv")
    # stock_ledger_df.printSchema()
    # orders_df.show()

    outbound_shipment_df = spark.read.format("csv") \
        .option("header", "True") \
        .option("inferschema", "True") \
        .option("mode", "PERMISSIVE") \
        .load(r"C:\Users\Venkatesh\OneDrive\Desktop\oil & gas\DATASETS\MIDSTREAM\Data\outbound_shipment.csv")
    # stock_ledger_df.printSchema()
    # outbound_shipment_df.show()

    pack_audit_df = spark.read.format("csv") \
        .option("header", "True") \
        .option("inferschema", "True") \
        .option("mode", "PERMISSIVE") \
        .load(r"C:\Users\Venkatesh\OneDrive\Desktop\oil & gas\DATASETS\MIDSTREAM\Data\pack_audit.csv")
    # pack_audit_df.printSchema()
    # pack_audit_df.show()

    personal_report_df = spark.read.format("csv") \
        .option("header", "True") \
        .option("inferschema", "True") \
        .option("mode", "PERMISSIVE") \
        .load(r"C:\Users\Venkatesh\OneDrive\Desktop\oil & gas\DATASETS\MIDSTREAM\Data\personal_report.csv")
    # personal_report_df.printSchema()
    # personal_report_df.show()

    pick_tickets_df = spark.read.format("csv") \
        .option("header", "True") \
        .option("inferschema", "True") \
        .option("mode", "PERMISSIVE") \
        .load(r"C:\Users\Venkatesh\OneDrive\Desktop\oil & gas\DATASETS\MIDSTREAM\Data\pick_tickets.csv")
    # pick_tickets_df.printSchema()
    # pick_tickets_df.show()

    QC_logs_df = spark.read.format("csv") \
        .option("header", "True") \
        .option("inferschema", "True") \
        .option("mode", "PERMISSIVE") \
        .load(r"C:\Users\Venkatesh\OneDrive\Desktop\oil & gas\DATASETS\MIDSTREAM\Data\QC_logs.csv")
    # QC_logs_df.printSchema()
    # QC_logs_df.show()

    receiving_audit_df = spark.read.format("csv") \
        .option("header", "True") \
        .option("inferschema", "True") \
        .option("mode", "PERMISSIVE") \
        .load(r"C:\Users\Venkatesh\OneDrive\Desktop\oil & gas\DATASETS\MIDSTREAM\Data\receiving_audit.csv")
    # receiving_audit_df.printSchema()
    # receiving_audit_df.show()

    return_logs_df = spark.read.format("csv") \
        .option("header", "True") \
        .option("inferschema", "True") \
        .option("mode", "PERMISSIVE") \
        .load(r"C:\Users\Venkatesh\OneDrive\Desktop\oil & gas\DATASETS\MIDSTREAM\Data\return_logs.csv")
    # return_logs_df.printSchema()
    # return_logs_df.show()

    upkeep_logs_df = spark.read.format("csv") \
        .option("header", "True") \
        .option("inferschema", "True") \
        .option("mode", "PERMISSIVE") \
        .load(r"C:\Users\Venkatesh\OneDrive\Desktop\oil & gas\DATASETS\MIDSTREAM\Data\upkeep_logss.csv")
    # upkeep_logs_df.printSchema()
    # upkeep_logs_df.show()


##########################################################################################################################################################

    # 1. ** Update Item Status Based on Storage Condition:
    # If the storage condition of an item in the  Stock Ledger  is poor, change the status of the item to "Discontinued."

    # df_updated = stock_ledger_df.withColumn("Status",when(stock_ledger_df["StorageCondition"] == "Poor", "Discontinued").otherwise(stock_ledger_df["Status"]))
    #
    # df_updated.show()

##########################################################################################################################################################

    # 2.  Assess Supplier Quality from Return Logs: Using the  Stock Ledger  and  Return Logs  identify the SupplierID  from the Return Logs  based
    # on SupplyID and  InventoryID .Assess he supplier’s information and the quality of their supplies.

    # joined_df = return_logs_df.join(stock_ledger_df, "InventoryID")
    # # joined_df.show()
    #
    # result_df = joined_df.withColumn("quality_inspection",
    #                                  when(joined_df["ReturnType"] == "Defective", "Poor").otherwise(
    #                                      '')).select(col('InventoryID'),col('ProductID'),col('ProductName'),col('SupplierID'),col('ReturnID'),col('ReturnReason'),col('quality_inspection'))
    # # result_df.show()



###########################################################################################################################################################

    # 3. Identify Reporting Individuals and Safety Violations:
    # Using  ES&H  data, find the personal information of the individual who reported anincident and determine if there were any safety violations

    # joined_df1 = ESandH_df.alias('e').join(personal_report_df.alias('p'),
    #                                        col('e.ReportedBy') == col('p.StaffID')).select(col('p.Name'), col('p.Role'),
    #                                                                                        col('p.ContactInfo'),
    #                                                                                        col('TrainingCompleted'),
    #                                                                                        col('EmploymentType'),col('e.SafetyViolation'),
    #                                                                                        col('e.IncidentDescription'))
    #
    # joined_df1.show(truncate= False)
    #
    # violations_df = joined_df1.filter(ESandH_df["SafetyViolation"] == "Yes")
    #
    # violations_df.show(truncate= False)



#######################################################################################################################################

    # 4.  Assess Incident Costs Monthly: Using  ES&H data, assess the incident costs on a monthly basis.

    # Convert the IncidentDate column to date format

    # ESandH_df = ESandH_df.withColumn("IncidentDate", to_date(col("IncidentDate"), "dd-MM-yyyy"))
    #
    # # Extract Year and Month
    #
    # ESandH_df = ESandH_df.withColumn("Year", year(col("IncidentDate"))) \
    #     .withColumn("Month", month(col("IncidentDate")))
    #
    # # Group by Year and Month and sum the Incident Costs And Display the results
    #
    # monthly_costs_df = ESandH_df.groupBy("Year", "Month") \
        # .agg(sum("IncidentCost").alias("TotalIncidentCost")).orderBy("Year", "Month").show()


##########################################################################################################################################################

    # 5. Assess Shipping Method Performance:
    # In the Outbound Shipment  table, assess each shipping method's performance by analyzing the delivery status.
    # Identify which shipping methods perform the best and which ones need improvement.


    # Group by ShippingMethod and DeliveryPerformance, then count the occurrences
    # performance_df = outbound_shipment_df.groupBy("ShippingMethod", "DeliveryPerformance").agg(count("*").alias("Count"))
    #
    # # Show the grouped and counted DataFrame
    # performance_df.show()
    #
    # # Optionally, you can pivot the data to have DeliveryPerformance as columns
    # pivot_df = performance_df.groupBy("ShippingMethod").pivot("DeliveryPerformance").sum("Count")
    #
    # # Show the pivoted DataFrame
    # pivot_df.show()


########################################################################################################################################################

    # 6. 3. Tracking Defective Products and Their Impact on Stock Levels

    # 	Tables Involved: Return Logs, Stock Ledger

    # 	Objective:
    # Implement tracking mechanisms to identify defective products from the Return Logs table,
    # adjust stock levels in the Stock Ledger accordingly, and analyze the subsequent impact on total stock value.
    # By ensuring that defective items are correctly accounted for and removed from stock,
    # you can maintain accurate inventory levels and minimize financial discrepancies due to faulty goods.



    # # Filter Return Logs for Defective Products
    # defective_returns_df = return_logs_df.filter(col("ReturnType") == "Defective")
    #
    # # Join with Stock Ledger to adjust stock levels
    # adjusted_stock_df = stock_ledger_df.join(defective_returns_df, "InventoryID").withColumn(
    #     "AdjustedQuantity", col("QuantityOnHand") - col("QuantityReturned")).withColumn("AdjustedTotalValue",
    #                                                                                     col("AdjustedQuantity") * col("UnitPrice"))
    # adjusted_stock_df.show()
    #
    # # Summarize the impact on stock value
    # stock_value_impact_df = adjusted_stock_df.groupBy("ProductID").agg(
    #     sum("TotalValue").alias("OriginalStockValue"),
    #     sum("AdjustedTotalValue").alias("AdjustedStockValue")
    # ).withColumn(
    #     "StockValueImpact", col("OriginalStockValue") - col("AdjustedStockValue"))
    #
    # stock_value_impact_df.show()






###################################################################################################################################################

    # 7.Calculating Warehouse Staff Efficiency
    # TablesInvolved: Pick Tickets, Pack Audit, PersonalReports
    # Objective: Measure the efficiency of warehouse staff by analyzing the time taken
    # for picking and packing orders, and correlate it with their performance ratings.

    # # Calculate Picking and Packing Time
    # efficiency_df = pick_tickets_df.join(pack_audit_df, "OrderID").join(personal_report_df,
    #                                                                     pick_tickets_df.PickedBy == personal_report_df.StaffID)

    # # Calculate time difference between picking and packing
    # efficiency_df = efficiency_df.withColumn("PickDate", to_date(col("PickDate"), "dd-MM-yyyy"))
    # efficiency_df = efficiency_df.withColumn("PackingDate", to_date(col("PackingDate"), "dd-MM-yyyy"))
    # #
    # efficiency_df = efficiency_df.withColumn(
    #     "PickingPackingTime", datediff("PackingDate","PickDate" )
    # )
    #
    # # Map string ratings to numeric values
    # efficiency_df = efficiency_df.withColumn(
    #     "NumericPerformanceRating",
    #     when(col("PerformanceRating") == "Excellent", 5)
    #     .when(col("PerformanceRating") == "Good", 4)
    #     .when(col("PerformanceRating") == "Average", 3)
    #     .otherwise(0)  # Default value if there are any unexpected strings
    # )
    #
    # efficiency_df.show()
    #
    # # Calculate average time and correlate with performance rating
    # staff_efficiency_df = efficiency_df.groupBy("StaffID").agg(
    #     avg("PickingPackingTime").alias("AvgPickingPackingTime"),
    #     avg("NumericPerformanceRating").alias("AvgPerformanceRating")
    # ).withColumn(
    #     "EfficiencyScore", col("AvgPerformanceRating") / col("AvgPickingPackingTime")
    # )
    #
    # staff_efficiency_df.show()


##########################################################################################################################################################

    # 8.Evaluate the effectiveness of different shipping methods by comparing ShippingCost and
    # DeliveryPerformance in the Outbound Shipment table. Generate a report highlighting the
    # most cost-effective and timely shipping methods.

    # # Map DeliveryPerformance to numeric scores
    # outbound_shipment_df = outbound_shipment_df.withColumn(
    # "DeliveryPerformanceScore",
    # when(col("DeliveryPerformance") == "On Time", 1)
    # .when(col("DeliveryPerformance") == "Early", 2)
    # .when(col("DeliveryPerformance") == "Delayed", 0)
    # .otherwise(None))
    # # Handle unexpected values, though your data seems clean
    #
    #
    # # Perform the aggregation
    # shipping_performance_df = outbound_shipment_df.groupBy("ShippingMethod").agg(
    # avg("ShippingCost").alias("AvgShippingCost"),
    # avg("DeliveryPerformanceScore").alias("AvgDeliveryPerformance"))
    #
    # # Generate the report sorted by cost and performance
    # report_df = shipping_performance_df.orderBy(col("AvgShippingCost").asc(), col("AvgDeliveryPerformance").desc())
    #
    # report_df.show()

##########################################################################################################################################################


# 8.5.Assessing Maintenance Impact on Facility Efficiency

# Tables Involved: Upkeep Logs, Facility Unit

# Objective:
# Evaluate the effect of maintenance activities on the operational efficiency of facilities by analyzing data from the Upkeep Logs
# and Facility Unit tables.Ensure that maintenance records are used to assess their impact on the performance and functionality of facility units.
# By correlating maintenance activities with facility performance metrics,
# you can identify how maintenance actions influence operational efficiency and make informed decisions to optimize facility management.

# # Convert string dates to actual date format
# upkeep_logs_df = upkeep_logs_df.withColumn("MaintenanceDate", to_date(col("MaintenanceDate"), "dd-MM-yyyy"))
# upkeep_logs_df = upkeep_logs_df.withColumn("NextMaintenanceDate", to_date(col("NextMaintenanceDate"), "dd-MM-yyyy"))
#
# # Calculate total downtime per facility due to maintenance
#
# facility_downtime_df = upkeep_logs_df.groupBy("FacilityID").agg(
#     sum("Cost").alias("TotalMaintenanceCost"),
#     avg(datediff(col("NextMaintenanceDate"), col("MaintenanceDate"))).alias("AvgMaintenanceDowntime"))
#
# # Join with Facility Unit to correlate with operational efficiency
# facility_efficiency_df = facility_unit_df.join(facility_downtime_df, "FacilityID").withColumn(
#     "EfficiencyImpact", col("TotalMaintenanceCost") / col("Capacity"))
#
# facility_efficiency_df.show()