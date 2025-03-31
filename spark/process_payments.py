# process_payments.py (ВЕРСИЯ 2 - Исправленная схема)
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType, TimestampType
import argparse

def process_data(spark, input_path, output_path):
    """
    Reads raw payment data from GCS, processes it, and writes to GCS as Parquet.
    """
    print(f"Starting processing from {input_path} to {output_path}")

    # --- ИСПРАВЛЕННАЯ СХЕМА в соответствии с заголовком CSV ---
    schema = StructType([
        StructField("Change_Type", StringType(), True),
        StructField("Covered_Recipient_Type", StringType(), True),
        StructField("Teaching_Hospital_CCN", StringType(), True),
        StructField("Teaching_Hospital_ID", StringType(), True),
        StructField("Teaching_Hospital_Name", StringType(), True),
        # Исправлено имя поля:
        StructField("Covered_Recipient_Profile_ID", StringType(), True),
        StructField("Covered_Recipient_NPI", StringType(), True),
        StructField("Covered_Recipient_First_Name", StringType(), True),
        StructField("Covered_Recipient_Middle_Name", StringType(), True),
        StructField("Covered_Recipient_Last_Name", StringType(), True),
        StructField("Covered_Recipient_Name_Suffix", StringType(), True),
        StructField("Recipient_Primary_Business_Street_Address_Line1", StringType(), True),
        StructField("Recipient_Primary_Business_Street_Address_Line2", StringType(), True),
        StructField("Recipient_City", StringType(), True),
        StructField("Recipient_State", StringType(), True),
        StructField("Recipient_Zip_Code", StringType(), True),
        StructField("Recipient_Country", StringType(), True),
        StructField("Recipient_Province", StringType(), True),
        StructField("Recipient_Postal_Code", StringType(), True),
        # Добавлены поля Type 1-6
        StructField("Covered_Recipient_Primary_Type_1", StringType(), True),
        StructField("Covered_Recipient_Primary_Type_2", StringType(), True),
        StructField("Covered_Recipient_Primary_Type_3", StringType(), True),
        StructField("Covered_Recipient_Primary_Type_4", StringType(), True),
        StructField("Covered_Recipient_Primary_Type_5", StringType(), True),
        StructField("Covered_Recipient_Primary_Type_6", StringType(), True),
        # Добавлены поля Specialty 1-6
        StructField("Covered_Recipient_Specialty_1", StringType(), True),
        StructField("Covered_Recipient_Specialty_2", StringType(), True),
        StructField("Covered_Recipient_Specialty_3", StringType(), True),
        StructField("Covered_Recipient_Specialty_4", StringType(), True),
        StructField("Covered_Recipient_Specialty_5", StringType(), True),
        StructField("Covered_Recipient_Specialty_6", StringType(), True),
        # Добавлены поля License State 1-5
        StructField("Covered_Recipient_License_State_code1", StringType(), True),
        StructField("Covered_Recipient_License_State_code2", StringType(), True),
        StructField("Covered_Recipient_License_State_code3", StringType(), True),
        StructField("Covered_Recipient_License_State_code4", StringType(), True),
        StructField("Covered_Recipient_License_State_code5", StringType(), True),
        StructField("Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name", StringType(), True),
        StructField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID", StringType(), True),
        StructField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name", StringType(), True),
        StructField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State", StringType(), True),
        StructField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country", StringType(), True),
        StructField("Total_Amount_of_Payment_USDollars", DoubleType(), True),
        StructField("Date_of_Payment", StringType(), True),
        StructField("Number_of_Payments_Included_in_Total_Amount", IntegerType(), True),
        StructField("Form_of_Payment_or_Transfer_of_Value", StringType(), True),
        StructField("Nature_of_Payment_or_Transfer_of_Value", StringType(), True),
        StructField("City_of_Travel", StringType(), True),
        StructField("State_of_Travel", StringType(), True),
        StructField("Country_of_Travel", StringType(), True),
        StructField("Physician_Ownership_Indicator", StringType(), True),
        StructField("Third_Party_Payment_Recipient_Indicator", StringType(), True),
        StructField("Name_of_Third_Party_Entity_Receiving_Payment_or_Transfer_of_Value", StringType(), True),
        StructField("Charity_Indicator", StringType(), True),
        StructField("Third_Party_Equals_Covered_Recipient_Indicator", StringType(), True),
        StructField("Contextual_Information", StringType(), True),
        StructField("Delay_in_Publication_Indicator", StringType(), True),
        StructField("Record_ID", StringType(), True),
        StructField("Dispute_Status_for_Publication", StringType(), True),
        StructField("Related_Product_Indicator", StringType(), True),
        # Добавлены все поля продуктов 1-5
        StructField("Covered_or_Noncovered_Indicator_1", StringType(), True),
        StructField("Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1", StringType(), True),
        StructField("Product_Category_or_Therapeutic_Area_1", StringType(), True),
        StructField("Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1", StringType(), True),
        StructField("Associated_Drug_or_Biological_NDC_1", StringType(), True),
        StructField("Associated_Device_or_Medical_Supply_PDI_1", StringType(), True),
        StructField("Covered_or_Noncovered_Indicator_2", StringType(), True),
        StructField("Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_2", StringType(), True),
        StructField("Product_Category_or_Therapeutic_Area_2", StringType(), True),
        StructField("Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2", StringType(), True),
        StructField("Associated_Drug_or_Biological_NDC_2", StringType(), True),
        StructField("Associated_Device_or_Medical_Supply_PDI_2", StringType(), True),
        StructField("Covered_or_Noncovered_Indicator_3", StringType(), True),
        StructField("Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_3", StringType(), True),
        StructField("Product_Category_or_Therapeutic_Area_3", StringType(), True),
        StructField("Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3", StringType(), True),
        StructField("Associated_Drug_or_Biological_NDC_3", StringType(), True),
        StructField("Associated_Device_or_Medical_Supply_PDI_3", StringType(), True),
        StructField("Covered_or_Noncovered_Indicator_4", StringType(), True),
        StructField("Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_4", StringType(), True),
        StructField("Product_Category_or_Therapeutic_Area_4", StringType(), True),
        StructField("Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4", StringType(), True),
        StructField("Associated_Drug_or_Biological_NDC_4", StringType(), True),
        StructField("Associated_Device_or_Medical_Supply_PDI_4", StringType(), True),
        StructField("Covered_or_Noncovered_Indicator_5", StringType(), True),
        StructField("Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_5", StringType(), True),
        StructField("Product_Category_or_Therapeutic_Area_5", StringType(), True),
        StructField("Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5", StringType(), True),
        StructField("Associated_Drug_or_Biological_NDC_5", StringType(), True),
        StructField("Associated_Device_or_Medical_Supply_PDI_5", StringType(), True),
        # Конечные поля
        StructField("Program_Year", StringType(), True),
        StructField("Payment_Publication_Date", StringType(), True)
    ])
    # --- Конец секции схемы ---

    print(f"Reading CSV from: {input_path}")
    try:
        # Добавляем опцию dateFormat для корректного чтения дат, если они в стандартном формате Spark
        # Однако, т.к. мы читаем как StringType(), это не так критично здесь.
        # Важно использовать multiLine=True и escape='"' т.к. поля могут содержать кавычки и переносы строк
        raw_df = spark.read.csv(input_path, header=True, schema=schema, multiLine=True, escape='"', encoding='UTF-8')
    except Exception as e:
        print(f"Error reading CSV: {e}")
        print("Please double check the input path, CSV format/encoding, and schema definition against the actual file header.")
        raise e

    print("Schema after reading:")
    raw_df.printSchema()
    # Увеличиваем кол-во строк для показа, чтобы увидеть примеры данных
    print("Sample data (first 5 rows):")
    raw_df.show(5, truncate=False)
    print(f"Raw count: {raw_df.count()}")

    # --- Базовая очистка и выборка (с использованием правильных имен столбцов) ---
    processed_df = raw_df.select(
        F.col("Record_ID"),
        F.col("Covered_Recipient_Type"),
        F.col("Covered_Recipient_NPI").alias("recipient_npi"),
        F.col("Covered_Recipient_First_Name").alias("recipient_first_name"),
        F.col("Covered_Recipient_Last_Name").alias("recipient_last_name"),
        F.col("Recipient_State").alias("recipient_state"),
        F.col("Recipient_City").alias("recipient_city"),
        # Используем первую специальность
        F.col("Covered_Recipient_Specialty_1").alias("physician_specialty"),
        F.col("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name").alias("payer_name"),
        F.col("Total_Amount_of_Payment_USDollars").alias("payment_amount_usd"),
        F.col("Date_of_Payment").alias("payment_date_str"),
        F.col("Form_of_Payment_or_Transfer_of_Value").alias("payment_form"),
        F.col("Nature_of_Payment_or_Transfer_of_Value").alias("payment_nature"),
        F.col("Program_Year").alias("program_year")
    ).filter(F.col("Record_ID").isNotNull())

    # --- Запись в Parquet ---
    print(f"Writing processed data to {output_path}")
    # Убираем coalesce(1) для реальной работы
    processed_df.write.parquet(output_path, mode="overwrite")
    print("Processing finished.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_path',
        required=True,
        help='GCS path to the raw input CSV file (e.g., gs://bucket/path/to/file.csv)')
    parser.add_argument(
        '--output_path',
        required=True,
        help='GCS path to write the processed Parquet output folder (e.g., gs://bucket/path/to/output/)')
    args = parser.parse_args()

    spark = SparkSession.builder.appName("OpenPaymentsProcessing").getOrCreate()

    process_data(spark, args.input_path, args.output_path)

    spark.stop()
