"""
02. Silver 레이어 생성 스크립트
Bronze(Raw) 데이터를 정제하여 Silver 레이어로 변환
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.spark_config import *
from pyspark.sql import functions as F


def create_flights_silver(spark):
    """항공편 Silver 테이블 생성"""
    print("Creating flights_silver table...")

    spark.sql(f"""
        CREATE OR REPLACE TABLE {TABLE_FLIGHTS_SILVER}
        USING DELTA
        AS
        SELECT
          flight_date,
          airline AS airline_code,
          origin AS origin_airport,
          dest AS dest_airport,
          CAST(arr_delay AS INT) AS arr_delay,
          CAST(dep_delay AS INT) AS dep_delay,
          CAST(distance AS INT) AS distance_miles,
          ingestion_timestamp,
          CURRENT_TIMESTAMP() AS transformation_timestamp
        FROM {TABLE_FLIGHTS_RAW}
        WHERE flight_date IS NOT NULL
          AND airline IS NOT NULL
          AND origin IS NOT NULL
          AND dest IS NOT NULL
    """)

    # 데이터 품질 검증
    df = spark.table(TABLE_FLIGHTS_SILVER)
    count = df.count()

    print(f"✓ Created {TABLE_FLIGHTS_SILVER}")
    print(f"  Records: {count:,}")

    # 통계 정보
    print(f"\n  Quality Checks:")
    print(f"    • Null arr_delay: {df.filter(F.col('arr_delay').isNull()).count()}")
    print(f"    • Null dep_delay: {df.filter(F.col('dep_delay').isNull()).count()}")
    print(f"    • Invalid dates: {df.filter(F.col('flight_date').isNull()).count()}")

    return count


def create_customer_silver(spark):
    """고객 Silver 테이블 생성"""
    print("\nCreating customer_silver table...")

    spark.sql(f"""
        CREATE OR REPLACE TABLE {TABLE_CUSTOMER_SILVER}
        USING DELTA
        AS
        SELECT
          customer_id,
          full_name,
          email,
          phone,
          passport_no,
          country,
          segment,
          ingestion_timestamp,
          CURRENT_TIMESTAMP() AS transformation_timestamp
        FROM {TABLE_CUSTOMER_RAW}
        WHERE customer_id IS NOT NULL
    """)

    # 데이터 품질 검증
    df = spark.table(TABLE_CUSTOMER_SILVER)
    count = df.count()

    print(f"✓ Created {TABLE_CUSTOMER_SILVER}")
    print(f"  Records: {count:,}")

    # 통계 정보
    print(f"\n  Quality Checks:")
    print(f"    • Null emails: {df.filter(F.col('email').isNull()).count()}")
    print(f"    • Null phones: {df.filter(F.col('phone').isNull()).count()}")
    print(f"    • Duplicate customer_ids: {df.groupBy('customer_id').count().filter('count > 1').count()}")

    return count


def optimize_tables(spark):
    """Delta Lake 테이블 최적화"""
    print("\nOptimizing Delta tables...")

    tables = [TABLE_FLIGHTS_SILVER, TABLE_CUSTOMER_SILVER]

    for table in tables:
        # Z-Ordering (주요 필터 컬럼 기준 최적화)
        if "flights" in table:
            spark.sql(f"OPTIMIZE {table} ZORDER BY (flight_date, airline_code)")
        else:
            spark.sql(f"OPTIMIZE {table} ZORDER BY (country, segment)")

        print(f"  ✓ Optimized: {table}")


def show_sample_data(spark):
    """샘플 데이터 미리보기"""
    print("\n" + "=" * 60)
    print("SAMPLE DATA PREVIEW")
    print("=" * 60)

    print("\n✈️  Flights Silver (Top 5):")
    spark.table(TABLE_FLIGHTS_SILVER).show(5, truncate=False)

    print("\n👤 Customers Silver (Top 5):")
    spark.table(TABLE_CUSTOMER_SILVER) \
        .select("customer_id", "full_name", "email", "country", "segment") \
        .show(5, truncate=False)


def generate_statistics(spark):
    """통계 정보 생성"""
    print("\n" + "=" * 60)
    print("SILVER LAYER STATISTICS")
    print("=" * 60)

    # 항공편 통계
    print("\n✈️  Flight Statistics:")
    spark.sql(f"""
        SELECT 
            'Total Flights' AS metric,
            COUNT(*) AS value
        FROM {TABLE_FLIGHTS_SILVER}

        UNION ALL

        SELECT 
            'Unique Airlines' AS metric,
            COUNT(DISTINCT airline_code) AS value
        FROM {TABLE_FLIGHTS_SILVER}

        UNION ALL

        SELECT 
            'Unique Routes' AS metric,
            COUNT(DISTINCT CONCAT(origin_airport, '-', dest_airport)) AS value
        FROM {TABLE_FLIGHTS_SILVER}

        UNION ALL

        SELECT 
            'Avg Arrival Delay (min)' AS metric,
            CAST(AVG(arr_delay) AS INT) AS value
        FROM {TABLE_FLIGHTS_SILVER}
    """).show(truncate=False)

    # 고객 통계
    print("\n👤 Customer Statistics:")
    spark.sql(f"""
        SELECT 
            'Total Customers' AS metric,
            COUNT(*) AS value
        FROM {TABLE_CUSTOMER_SILVER}

        UNION ALL

        SELECT 
            'Unique Countries' AS metric,
            COUNT(DISTINCT country) AS value
        FROM {TABLE_CUSTOMER_SILVER}

        UNION ALL

        SELECT 
            'Business Class %' AS metric,
            CAST(SUM(CASE WHEN segment = 'Business' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS INT) AS value
        FROM {TABLE_CUSTOMER_SILVER}
    """).show(truncate=False)

    # 국가별 고객 분포
    print("\n🌍 Top 10 Countries by Customer Count:")
    spark.sql(f"""
        SELECT 
            country,
            COUNT(*) AS customer_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
        FROM {TABLE_CUSTOMER_SILVER}
        GROUP BY country
        ORDER BY customer_count DESC
        LIMIT 10
    """).show(truncate=False)


def main():
    print("=" * 60)
    print("STEP 2: Create Silver Layer (Curated Data)")
    print("=" * 60)
    print()

    # Spark 세션 생성
    spark = get_spark_session("02_CreateSilverLayer")

    # Silver 테이블 생성
    flights_count = create_flights_silver(spark)
    customer_count = create_customer_silver(spark)

    # 테이블 최적화
    optimize_tables(spark)

    # 샘플 데이터 미리보기
    show_sample_data(spark)

    # 통계 정보 생성
    generate_statistics(spark)

    # 요약
    print("\n" + "=" * 60)
    print("✅ SILVER LAYER CREATION COMPLETE")
    print("=" * 60)
    print(f"\n📊 Summary:")
    print(f"   • Flights processed: {flights_count:,}")
    print(f"   • Customers processed: {customer_count:,}")
    print(f"\n📁 Tables created:")
    print(f"   • {TABLE_FLIGHTS_SILVER}")
    print(f"   • {TABLE_CUSTOMER_SILVER}")
    print(f"\n🎯 Data Quality:")
    print(f"   • Validated and cleaned")
    print(f"   • Optimized with Z-Ordering")
    print(f"   • Ready for analytics")
    print(f"\n🚀 Next step: python scripts/03_create_mart_layer.py")
    print()

    # Spark 세션 종료
    stop_spark_session(spark)


if __name__ == "__main__":
    main()