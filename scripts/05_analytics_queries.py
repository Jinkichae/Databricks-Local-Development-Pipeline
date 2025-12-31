"""
05. 분석 쿼리 스크립트
Mart 레이어를 활용한 비즈니스 분석 쿼리 실행
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.spark_config import *


def airline_performance_analysis(spark):
    """항공사 성과 분석"""
    print("=" * 60)
    print("1️⃣  AIRLINE PERFORMANCE ANALYSIS")
    print("=" * 60)

    # 항공사별 전체 성과
    print("\n📊 Overall Airline Performance:")
    spark.sql(f"""
        SELECT
            airline_code,
            total_flights,
            CONCAT(ROUND(avg_arr_delay, 1), ' min') AS avg_delay,
            CONCAT(delayed_rate_pct, '%') AS delay_rate,
            CONCAT(ROUND(avg_distance_miles, 0), ' mi') AS avg_distance,
            CASE 
                WHEN delayed_rate_pct < 20 THEN '⭐⭐⭐⭐⭐'
                WHEN delayed_rate_pct < 25 THEN '⭐⭐⭐⭐'
                WHEN delayed_rate_pct < 30 THEN '⭐⭐⭐'
                ELSE '⭐⭐'
            END AS rating
        FROM {TABLE_FLIGHT_DELAY_KPI}
        ORDER BY delayed_rate_pct ASC
    """).show(truncate=False)

    # 지연 시간 분포
    print("\n📈 Delay Time Distribution:")
    spark.sql(f"""
        SELECT
            airline_code,
            median_arr_delay AS median_delay,
            max_arr_delay AS max_delay,
            min_arr_delay AS min_delay,
            max_arr_delay - min_arr_delay AS delay_range
        FROM {TABLE_FLIGHT_DELAY_KPI}
        ORDER BY median_delay DESC
        LIMIT 5
    """).show(truncate=False)


def route_analysis(spark):
    """노선 분석"""
    print("\n" + "=" * 60)
    print("2️⃣  ROUTE ANALYSIS")
    print("=" * 60)

    # 가장 바쁜 노선
    print("\n🛫 Top 10 Busiest Routes:")
    spark.sql(f"""
        SELECT
            route,
            total_flights,
            CONCAT(ROUND(distance_miles, 0), ' mi') AS distance,
            CONCAT(ROUND(avg_arr_delay, 1), ' min') AS avg_delay,
            airlines_serving AS airlines,
            CASE 
                WHEN delayed_rate_pct < 20 THEN '✅ Good'
                WHEN delayed_rate_pct < 30 THEN '⚠️  Fair'
                ELSE '❌ Poor'
            END AS performance
        FROM {TABLE_ROUTE_PERFORMANCE}
        ORDER BY total_flights DESC
        LIMIT 10
    """).show(truncate=False)

    # 가장 지연이 심한 노선
    print("\n⏰ Top 10 Most Delayed Routes:")
    spark.sql(f"""
        SELECT
            route,
            total_flights,
            CONCAT(ROUND(avg_arr_delay, 1), ' min') AS avg_delay,
            CONCAT(delayed_rate_pct, '%') AS delay_rate,
            delayed_flights
        FROM {TABLE_ROUTE_PERFORMANCE}
        WHERE total_flights >= 50
        ORDER BY avg_arr_delay DESC
        LIMIT 10
    """).show(truncate=False)

    # 가장 효율적인 노선
    print("\n⚡ Top 10 Most Efficient Routes:")
    spark.sql(f"""
        SELECT
            route,
            total_flights,
            CONCAT(ROUND(avg_arr_delay, 1), ' min') AS avg_delay,
            CONCAT(delayed_rate_pct, '%') AS delay_rate
        FROM {TABLE_ROUTE_PERFORMANCE}
        WHERE total_flights >= 50
        ORDER BY avg_arr_delay ASC
        LIMIT 10
    """).show(truncate=False)


def customer_segment_analysis(spark):
    """고객 세그먼트 분석"""
    print("\n" + "=" * 60)
    print("3️⃣  CUSTOMER SEGMENT ANALYSIS")
    print("=" * 60)

    # 세그먼트별 전체 분포
    print("\n👥 Customer Distribution by Segment:")
    spark.sql(f"""
        SELECT
            segment,
            SUM(customer_count) AS total_customers,
            COUNT(DISTINCT country) AS countries,
            ROUND(SUM(customer_count) * 100.0 / 
                  (SELECT SUM(customer_count) FROM {TABLE_CUSTOMER_SEGMENT_STATS}), 2) AS percentage
        FROM {TABLE_CUSTOMER_SEGMENT_STATS}
        GROUP BY segment
        ORDER BY total_customers DESC
    """).show(truncate=False)

    # 국가별 TOP 세그먼트
    print("\n🌍 Top Segment by Country (Top 10 Countries):")
    spark.sql(f"""
        WITH ranked AS (
            SELECT
                country,
                segment,
                customer_count,
                ROW_NUMBER() OVER (PARTITION BY country ORDER BY customer_count DESC) AS rn
            FROM {TABLE_CUSTOMER_SEGMENT_STATS}
        )
        SELECT
            country,
            segment AS top_segment,
            customer_count,
            pct_in_country AS pct
        FROM ranked JOIN {TABLE_CUSTOMER_SEGMENT_STATS} USING (country, segment, customer_count)
        WHERE rn = 1
        ORDER BY customer_count DESC
        LIMIT 10
    """).show(truncate=False)

    # 프리미엄 고객 분석
    print("\n💎 Premium Customer Analysis (Business + First Class):")
    spark.sql(f"""
        SELECT
            country,
            SUM(CASE WHEN segment IN ('Business', 'First Class') THEN customer_count ELSE 0 END) AS premium_customers,
            SUM(customer_count) AS total_customers,
            ROUND(SUM(CASE WHEN segment IN ('Business', 'First Class') THEN customer_count ELSE 0 END) * 100.0 
                  / SUM(customer_count), 2) AS premium_pct
        FROM {TABLE_CUSTOMER_SEGMENT_STATS}
        GROUP BY country
        HAVING premium_customers > 0
        ORDER BY premium_pct DESC
        LIMIT 10
    """).show(truncate=False)


def cross_analysis(spark):
    """교차 분석"""
    print("\n" + "=" * 60)
    print("4️⃣  CROSS ANALYSIS")
    print("=" * 60)

    # 항공사-노선 교차 분석
    print("\n🔍 Airline Performance on Busiest Routes:")
    spark.sql(f"""
        SELECT
            r.route,
            COUNT(DISTINCT f.airline_code) AS airlines,
            r.total_flights,
            ROUND(AVG(k.avg_arr_delay), 1) AS avg_airline_delay,
            ROUND(r.avg_arr_delay, 1) AS route_avg_delay
        FROM {TABLE_ROUTE_PERFORMANCE} r
        JOIN {TABLE_FLIGHTS_SILVER} f 
            ON r.origin_airport = f.origin_airport 
            AND r.dest_airport = f.dest_airport
        JOIN {TABLE_FLIGHT_DELAY_KPI} k 
            ON f.airline_code = k.airline_code
        WHERE r.total_flights >= 100
        GROUP BY r.route, r.total_flights, r.avg_arr_delay
        ORDER BY r.total_flights DESC
        LIMIT 10
    """).show(truncate=False)


def operational_metrics(spark):
    """운영 메트릭"""
    print("\n" + "=" * 60)
    print("5️⃣  OPERATIONAL METRICS")
    print("=" * 60)

    # 전체 운영 통계
    print("\n📊 Overall Operational Statistics:")
    result = spark.sql(f"""
        SELECT
            SUM(total_flights) AS total_flights,
            SUM(delayed_flights) AS total_delayed,
            ROUND(SUM(delayed_flights) * 100.0 / SUM(total_flights), 2) AS overall_delay_rate,
            ROUND(AVG(avg_arr_delay), 2) AS avg_delay_minutes,
            SUM(total_distance_miles) AS total_miles,
            COUNT(DISTINCT airline_code) AS airlines
        FROM {TABLE_FLIGHT_DELAY_KPI}
    """).collect()[0]

    print(f"""
    ╔═══════════════════════════════════════════════════════╗
    ║  Total Flights:          {result['total_flights']:>10,}              ║
    ║  Delayed Flights:        {result['total_delayed']:>10,}              ║
    ║  Overall Delay Rate:     {result['overall_delay_rate']:>10.2f}%            ║
    ║  Avg Delay Time:         {result['avg_delay_minutes']:>10.2f} minutes      ║
    ║  Total Miles Flown:      {result['total_miles']:>10,.0f}          ║
    ║  Airlines Operating:     {result['airlines']:>10}              ║
    ╚═══════════════════════════════════════════════════════╝
    """)

    # 효율성 벤치마크
    print("\n🎯 Efficiency Benchmark:")
    spark.sql(f"""
        SELECT
            'Best Performer' AS category,
            airline_code,
            delayed_rate_pct AS metric
        FROM {TABLE_FLIGHT_DELAY_KPI}
        ORDER BY delayed_rate_pct ASC
        LIMIT 1

        UNION ALL

        SELECT
            'Worst Performer' AS category,
            airline_code,
            delayed_rate_pct AS metric
        FROM {TABLE_FLIGHT_DELAY_KPI}
        ORDER BY delayed_rate_pct DESC
        LIMIT 1

        UNION ALL

        SELECT
            'Industry Average' AS category,
            'N/A' AS airline_code,
            ROUND(AVG(delayed_rate_pct), 2) AS metric
        FROM {TABLE_FLIGHT_DELAY_KPI}
    """).show(truncate=False)


def export_summary_report(spark):
    """요약 리포트 내보내기"""
    print("\n" + "=" * 60)
    print("6️⃣  SUMMARY REPORT")
    print("=" * 60)

    # 데이터 레이어 요약
    print("\n📁 Data Layer Summary:")
    layers = [
        ('Bronze/Raw', DB_RAW, [TABLE_FLIGHTS_RAW, TABLE_CUSTOMER_RAW]),
        ('Silver/Curated', DB_SILVER, [TABLE_FLIGHTS_SILVER, TABLE_CUSTOMER_SILVER]),
        ('Gold/Mart', DB_MART, [TABLE_FLIGHT_DELAY_KPI, TABLE_ROUTE_PERFORMANCE, TABLE_CUSTOMER_SEGMENT_STATS]),
        ('Metadata', DB_META, [TABLE_PII_DETECTION])
    ]

    for layer_name, db, tables in layers:
        print(f"\n  {layer_name}:")
        for table in tables:
            try:
                count = spark.table(table).count()
                table_name = table.split('.')[-1]
                print(f"    • {table_name:30} {count:>10,} rows")
            except:
                print(f"    • {table_name:30} {'N/A':>10}")

    # 주요 인사이트
    print("\n💡 Key Insights:")

    # 최고 항공사
    best_airline = spark.sql(f"""
        SELECT airline_code, delayed_rate_pct
        FROM {TABLE_FLIGHT_DELAY_KPI}
        ORDER BY delayed_rate_pct ASC
        LIMIT 1
    """).collect()[0]

    # 가장 바쁜 노선
    busiest_route = spark.sql(f"""
        SELECT route, total_flights
        FROM {TABLE_ROUTE_PERFORMANCE}
        ORDER BY total_flights DESC
        LIMIT 1
    """).collect()[0]

    # 최대 고객 세그먼트
    top_segment = spark.sql(f"""
        SELECT segment, SUM(customer_count) AS total
        FROM {TABLE_CUSTOMER_SEGMENT_STATS}
        GROUP BY segment
        ORDER BY total DESC
        LIMIT 1
    """).collect()[0]

    print(f"""
    1. Best Performing Airline: {best_airline['airline_code']} ({best_airline['delayed_rate_pct']}% delay rate)
    2. Busiest Route: {busiest_route['route']} ({busiest_route['total_flights']:,} flights)
    3. Largest Customer Segment: {top_segment['segment']} ({top_segment['total']:,} customers)
    """)


def main():
    print("=" * 60)
    print("STEP 5: Analytics & Business Intelligence")
    print("=" * 60)
    print()

    # Spark 세션 생성
    spark = get_spark_session("05_AnalyticsQueries")

    # 각 분석 실행
    airline_performance_analysis(spark)
    route_analysis(spark)
    customer_segment_analysis(spark)
    cross_analysis(spark)
    operational_metrics(spark)
    export_summary_report(spark)

    # 최종 요약
    print("\n" + "=" * 60)
    print("✅ ANALYTICS PIPELINE COMPLETE")
    print("=" * 60)
    print(f"""
    🎉 All Analysis Complete!

    📊 Analyses Performed:
       • Airline Performance Analysis
       • Route Performance Analysis  
       • Customer Segment Analysis
       • Cross-dimensional Analysis
       • Operational Metrics

    🎯 Business Value:
       • Executive dashboards
       • Performance monitoring
       • Strategic decision support
       • Customer insights

    📈 Data Architecture:
       Bronze (Raw) → Silver (Curated) → Gold (Mart)

    🔒 Data Governance:
       • PII detection completed
       • Masked views available
       • Access controls ready

    📂 Project Location: ./airlines-pyspark-demo/
    📁 Data Location: ./spark-warehouse/

    ✨ Portfolio Highlights:
       ✓ PySpark + Delta Lake
       ✓ Multi-layer data architecture
       ✓ Data quality & governance
       ✓ Business analytics
       ✓ Production-ready code
    """)

    # Spark 세션 종료
    stop_spark_session(spark)


if __name__ == "__main__":
    main()