from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from common_utils import common_config

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    cmn_ut = common_config()



    # to check the null value


    # personal_report_df = spark.read.format("csv") \
    #     .option("header", "True") \
    #     .option("inferschema", "True") \
    #     .option("mode", "FAILFAST") \
    #     .load(r"C:\Users\Venkatesh\OneDrive\Desktop\oil & gas\DATASETS\MIDSTREAM\Data\personal_report.csv")
    # # personal_report_df.printSchema()
    # # personal_report_df.show(truncate= False)

    # x = ['StaffID','Name','Role','ShiftID','ContactInfo','Status','TrainingCompleted','Salary','EmploymentType','PerformanceRating','Skills','Certifications']


    # personal_report_df = cmn_ut.null_check(personal_report_df,x)
    # personal_report_df.show()

    # bad_df = personal_report_df.filter(col('null_check')== 'null')
    # # bad_df.show()
    #
    # good_df = personal_report_df.filter(col('null_check')== '')
    # # good_df.show()




    # to check the special_character_check

    # facility_unit_df = spark.read.format("csv") \
    #     .option("header", "True") \
    #     .option("inferschema", "True") \
    #     .option("mode", "FAILFAST") \
    #     .load(r"C:\Users\Venkatesh\OneDrive\Desktop\oil & gas\DATASETS\MIDSTREAM\Data\facility_unit.csv")
    # facility_unit_df.printSchema()
    # facility_unit_df.show()

    # y = ['FacilityID','FacilityName','FacilityType','Location','Capacity','CurrentOccupancy','TemperatureControl','SecurityLevel','SafetyFeatures','MaintenanceSchedule','FacilityManager','LastInspectionDate','FacilityStatus','Status']

    # facility_unit_df = cmn_ut.special_character_check(facility_unit_df,y)
    # facility_unit_df.cache()

    # good_sep_char_df = facility_unit_df.filter(col('special_char')== '').show()




    # to drop the duplicates_records

    # QC_logs_df = spark.read.format("csv") \
    #     .option("header", "True") \
    #     .option("inferschema", "True") \
    #     .option("mode", "FAILFAST") \
    #     .load(r"C:\Users\Venkatesh\OneDrive\Desktop\oil & gas\DATASETS\MIDSTREAM\Data\QC_logs.csv")
    # # QC_logs_df.printSchema()
    # QC_logs_df.show()
    # print(QC_logs_df.count())

    # QC_logs_df = QC_logs_df.drop_duplicates()
    #
    # QC_logs_df.show()
    # print(QC_logs_df.count())

















