"""
01. 샘플 데이터 생성 스크립트
항공편 및 고객 데이터를 생성하여 Raw 레이어에 저장
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.spark_config import *
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta


def generate_flight_data(num_flights=10000):
    """항공편 데이터 생성"""
    print(f"Generating {num_flights:,} flight records...")

    airlines = ['AA', 'DL', 'UA', 'WN', 'B6', 'AS', 'NK', 'F9']
    airports = [
        'ATL', 'DFW', 'DEN', 'ORD', 'LAX', 'CLT', 'LAS', 'PHX',
        'MCO', 'SEA', 'EWR', 'SFO', 'DTW', 'BOS', 'MSP', 'FLL',
        'JFK', 'LGA', 'BWI', 'DCA', 'IAH', 'SLC', 'MDW', 'SAN'
    ]

    start_date = datetime.now() - timedelta(days=90)
    date_range = [start_date + timedelta(days=x) for x in range(90)]

    flight_data = []
    for i in range(num_flights):
        flight_date = random.choice(date_range).strftime('%Y-%m-%d')
        airline = random.choice(airlines)
        origin = random.choice(airports)
        dest = random.choice([a for a in airports if a != origin])
        distance = random.randint(200, 3000)

        # 75%는 정상 운항, 25%는 지연
        if random.random() < 0.75:
            arr_delay = random.randint(-15, 15)
            dep_delay = random.randint(-10, 20)
        else:
            arr_delay = random.randint(15, 180)
            dep_delay = random.randint(15, 150)

        flight_data.append({
            'flight_date': flight_date,
            'airline': airline,
            'origin': origin,
            'dest': dest,
            'arr_delay': arr_delay,
            'dep_delay': dep_delay,
            'distance': distance,
            'ingestion_timestamp': datetime.now().isoformat()
        })

    return flight_data


def generate_customer_data(num_customers=5000):
    """고객 데이터 생성 (PII 포함)"""
    print(f"Generating {num_customers:,} customer records...")

    first_names = [
        'James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer',
        'Michael', 'Linda', 'William', 'Elizabeth', 'David', 'Barbara',
        'Richard', 'Susan', 'Joseph', 'Jessica', 'Thomas', 'Sarah',
        'Charles', 'Karen', 'Daniel', 'Nancy', 'Matthew', 'Lisa'
    ]

    last_names = [
        'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia',
        'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez',
        'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore',
        'Jackson', 'Martin', 'Lee', 'Thompson', 'White', 'Harris'
    ]

    countries = [
        'USA', 'Canada', 'UK', 'Germany', 'France', 'Japan',
        'Australia', 'South Korea', 'China', 'Brazil', 'Mexico',
        'Spain', 'Italy', 'India', 'Netherlands'
    ]

    segments = ['Economy', 'Premium Economy', 'Business', 'First Class']

    customer_data = []
    for i in range(num_customers):
        customer_id = f"CUST{str(i + 1).zfill(6)}"
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        full_name = f"{first_name} {last_name}"
        email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 999)}@example.com"
        phone = f"+1-{random.randint(200, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
        passport_no = f"P{random.randint(10000000, 99999999)}"
        country = random.choice(countries)
        segment = random.choice(segments)

        customer_data.append({
            'customer_id': customer_id,
            'full_name': full_name,
            'email': email,
            'phone': phone,
            'passport_no': passport_no,
            'country': country,
            'segment': segment,
            'ingestion_timestamp': datetime.now().isoformat()
        })

    return customer_data


def main():
    print("=" * 60)
    print("STEP 1: Generate Sample Data (Bronze Layer)")
    print("=" * 60)
    print()

    # Spark 세션 생성
    spark = get_spark_session("01_GenerateSampleData")

    # 데이터베이스 생성
    create_databases(spark)

    # ========================================
    # 항공편 데이터 생성 및 저장
    # ========================================
    flight_data = generate_flight_data(num_flights=10000)

    schema_flights = StructType([
        StructField('flight_date', StringType(), True),
        StructField('airline', StringType(), True),
        StructField('origin', StringType(), True),
        StructField('dest', StringType(), True),
        StructField('arr_delay', IntegerType(), True),
        StructField('dep_delay', IntegerType(), True),
        StructField('distance', IntegerType(), True),
        StructField('ingestion_timestamp', StringType(), True)
    ])

    df_flights = spark.createDataFrame(flight_data, schema=schema_flights)

    df_flights.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(TABLE_FLIGHTS_RAW)

    flight_count = df_flights.count()
    print(f"✓ Saved {flight_count:,} flights → {TABLE_FLIGHTS_RAW}")
    print()

    # 샘플 데이터 미리보기
    print("Sample Flight Data:")
    df_flights.show(5, truncate=False)

    # ========================================
    # 고객 데이터 생성 및 저장
    # ========================================
    customer_data = generate_customer_data(num_customers=5000)

    schema_customers = StructType([
        StructField('customer_id', StringType(), True),
        StructField('full_name', StringType(), True),
        StructField('email', StringType(), True),
        StructField('phone', StringType(), True),
        StructField('passport_no', StringType(), True),
        StructField('country', StringType(), True),
        StructField('segment', StringType(), True),
        StructField('ingestion_timestamp', StringType(), True)
    ])

    df_customers = spark.createDataFrame(customer_data, schema=schema_customers)

    df_customers.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(TABLE_CUSTOMER_RAW)

    customer_count = df_customers.count()
    print(f"✓ Saved {customer_count:,} customers → {TABLE_CUSTOMER_RAW}")
    print()

    # 샘플 데이터 미리보기
    print("Sample Customer Data:")
    df_customers.show(5, truncate=False)

    # ========================================
    # 데이터 품질 체크
    # ========================================
    print("=" * 60)
    print("DATA QUALITY CHECKS")
    print("=" * 60)

    # 항공편 데이터 체크
    print(f"\n✈️  Flight Data:")
    print(f"   Total Records: {flight_count:,}")
    print(
        f"   Date Range: {df_flights.agg({'flight_date': 'min'}).collect()[0][0]} to {df_flights.agg({'flight_date': 'max'}).collect()[0][0]}")
    print(f"   Unique Airlines: {df_flights.select('airline').distinct().count()}")
    print(f"   Unique Airports: {df_flights.select('origin').distinct().count()}")

    # 고객 데이터 체크
    print(f"\n👤 Customer Data:")
    print(f"   Total Records: {customer_count:,}")
    print(f"   Unique Customers: {df_customers.select('customer_id').distinct().count()}")
    print(f"   Countries: {df_customers.select('country').distinct().count()}")
    print(f"   Segments: {df_customers.select('segment').distinct().count()}")

    print("\n" + "=" * 60)
    print("✅ BRONZE LAYER CREATION COMPLETE")
    print("=" * 60)
    print(f"\n📁 Tables created:")
    print(f"   • {TABLE_FLIGHTS_RAW}")
    print(f"   • {TABLE_CUSTOMER_RAW}")
    print(f"\n📂 Data location: ./spark-warehouse/")
    print(f"\n🚀 Next step: python scripts/02_create_silver_layer.py")
    print()

    # Spark 세션 종료
    stop_spark_session(spark)


if __name__ == "__main__":
    main()