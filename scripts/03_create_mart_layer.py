"""
03. Mart(Gold) 레이어 생성 스크립트
Silver 데이터를 집계하여 분석용 Mart 테이블 생성
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.spark_config import *


def create_flight_delay_kpi(spark):
    """항공사별 지연 KPI 테이블 생성"""
    print("Creating flight_delay_kpi mart...")

    spark.sql(f"""
        CREATE OR REPLACE TABLE {TABLE_FLIGHT_DELAY_KPI}
        USING DELTA
        AS
        SELECT
          airline_code,
          COUNT(*) AS total_flights,
          ROUND(AVG(arr_delay), 2) AS avg_arr_delay,
          ROUND(AVG(dep_delay), 2) AS avg_dep_delay,
          ROUND(PERCENTILE(arr_delay, 0.5), 2) AS median_arr_delay,
          MAX(arr_delay) AS max_arr_delay,
          MIN(arr_delay) AS min_arr_delay,
          SUM(CASE WHEN arr_delay > 15 THEN 1 ELSE 0 END) AS delayed_flights,
          ROUND(SUM(CASE WHEN arr_delay > 15 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS delayed_rate_pct,
          ROUND(AVG(distance_miles), 2) AS avg_distance_miles,
          SUM(distance_miles) AS total_distance_miles
        FROM {TABLE_FLIGHTS_SILVER}
        GROUP BY airline_code
        ORDER BY delayed_rate_pct DESC
    """)

    count = spark.table(TABLE_FLIGHT_DELAY_KPI).count()
    print(f"✓ Created {TABLE_FLIGHT_DELAY_KPI} ({count} airlines)")

    return count


def create_route_performance(spark):
    """노선별 성과 테이블 생성"""
    print("\nCreating route_performance mart...")

    spark.sql(f"""
        CREATE OR REPLACE TABLE {TABLE_ROUTE_PERFORMANCE}
        USING DELTA
        AS
        SELECT
          origin_airport,
          dest_airport,
          CONCAT(origin_airport, ' → ', dest_airport) AS route,
          COUNT(*) AS total_flights,
          ROUND(AVG(arr_delay), 2) AS avg_arr_delay,
          ROUND(AVG(dep_delay), 2) AS avg_dep_delay,
          ROUND(AVG(distance_miles), 2) AS distance_miles,
          SUM(CASE WHEN arr_delay > 15 THEN 1 ELSE 0 END) AS delayed_flights,
          ROUND(SUM(CASE WHEN arr_delay > 15 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS delayed_rate_pct,
          COUNT(DISTINCT airline_code) AS airlines_serving
        FROM {TABLE_FLIGHTS_SILVER}
        GROUP BY origin_airport, dest_airport
        HAVING COUNT(*) >= 10
        ORDER BY total_flights DESC
    """)

    count = spark.table(TABLE_ROUTE_PERFORMANCE).count()
    print(f"✓ Created {TABLE_ROUTE_PERFORMANCE} ({count} routes)")

    return count


def create_customer_segment_stats(spark):
    """고객 세그먼트 통계 테이블 생성"""
    print("\nCreating customer_segment_stats mart...")

    spark.sql(f"""
        CREATE OR REPLACE TABLE {TABLE_CUSTOMER_SEGMENT_STATS}
        USING DELTA
        AS
        SELECT
          segment,
          country,
          COUNT(*) AS customer_count,
          COUNT(DISTINCT email) AS unique_emails,
          ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY country), 2) AS pct_in_country,
          ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY segment), 2) AS pct_in_segment
        FROM {TABLE_CUSTOMER_SILVER}
        WHERE segment IS NOT NULL
        GROUP BY segment, country
        ORDER BY customer_count DESC
    """)

    count = spark.table(TABLE_CUSTOMER_SEGMENT_STATS).count()
    print(f"✓ Created {TABLE_CUSTOMER_SEGMENT_STATS} ({count} combinations)")

    return count


def show_mart_previews(spark):
    """Mart 테이블 미리보기"""
    print("\n" + "=" * 60)
    print("MART LAYER PREVIEW")
    print("=" * 60)

    print("\n✈️  Top 5 Airlines by Delay Rate:")
    spark.sql(f"""
        SELECT 
            airline_code,
            total_flights,
            avg_arr_delay,
            delayed_flights,
            delayed_rate_pct
        FROM {TABLE_FLIGHT_DELAY_KPI}
        ORDER BY delayed_rate_pct DESC
        LIMIT 5
    """).show(truncate=False)

    print("\n🛫 Top 10 Busiest Routes:")
    spark.sql(f"""
        SELECT 
            route,
            total_flights,
            avg_arr_delay,
            delayed_rate_pct,
            airlines_serving
        FROM {TABLE_ROUTE_PERFORMANCE}
        ORDER BY total_flights DESC
        LIMIT 10
    """).show(truncate=False)

    print("\n👥 Top 10 Customer Segments:")
    spark.sql(f"""
        SELECT 
            segment,
            country,
            customer_count,
            pct_in_country
        FROM {TABLE_CUSTOMER_SEGMENT_STATS}
        ORDER BY customer_count DESC
        LIMIT 10
    """).show(truncate=False)


def generate_insights(spark):
    """비즈니스 인사이트 생성"""
    print("\n" + "=" * 60)
    print("BUSINESS INSIGHTS")
    print("=" * 60)

    # 인사이트 1: 최고/최악 항공사
    print("\n📊 Airline Performance:")
    result = spark.sql(f"""
        SELECT 
            airline_code,
            delayed_rate_pct
        FROM {TABLE_FLIGHT_DELAY_KPI}
        ORDER BY delayed_rate_pct
    """).collect()

    best = result[0]
    worst = result[-1]
    print(f"   Best:  {best['airline_code']} ({best['delayed_rate_pct']}% delayed)")
    print(f"   Worst: {worst['airline_code']} ({worst['delayed_rate_pct']}% delayed)")

    # 인사이트 2: 가장 바쁜 노선
    print("\n🛫 Busiest Route:")
    busiest = spark.sql(f"""
        SELECT route, total_flights
        FROM {TABLE_ROUTE_PERFORMANCE}
        ORDER BY total_flights DESC
        LIMIT 1
    """).collect()[0]
    print(f"   {busiest['route']}: {busiest['total_flights']:,} flights")

    # 인사이트 3: 고객 분포
    print("\n👥 Customer Distribution:")
    segment_dist = spark.sql(f"""
        SELECT 
            segment,
            SUM(customer_count) AS total
        FROM {TABLE_CUSTOMER_SEGMENT_STATS}
        GROUP BY segment
        ORDER BY total DESC
    """).collect()

    for row in segment_dist:
        print(f"   {row['segment']:20} {row['total']:5,} customers")

    # 인사이트 4: 전체 통계
    print("\n📈 Overall Statistics:")
    overall = spark.sql(f"""
        SELECT 
            SUM(total_flights) AS total_flights,
            ROUND(AVG(delayed_rate_pct), 2) AS avg_delay_rate,
            SUM(total_distance_miles) AS total_miles
        FROM {TABLE_FLIGHT_DELAY_KPI}
    """).collect()[0]

    print(f"   Total Flights:    {overall['total_flights']:,}")
    print(f"   Avg Delay Rate:   {overall['avg_delay_rate']}%")
    print(f"   Total Miles Flown: {overall['total_miles']:,.0f}")


def create_analytical_views(spark):
    """분석용 뷰 생성"""
    print("\n" + "=" * 60)
    print("Creating Analytical Views...")
    print("=" * 60)

    # 뷰 1: 항공사-노선 결합 뷰
    spark.sql(f"""
        CREATE OR REPLACE VIEW {DB_MART}.airline_route_analysis AS
        SELECT 
            f.airline_code,
            r.route,
            r.total_flights,
            r.avg_arr_delay,
            r.delayed_rate_pct,
            k.avg_arr_delay AS airline_avg_delay
        FROM {TABLE_ROUTE_PERFORMANCE} r
        JOIN {TABLE_FLIGHTS_SILVER} f 
            ON r.origin_airport = f.origin_airport 
            AND r.dest_airport = f.dest_airport
        JOIN {TABLE_FLIGHT_DELAY_KPI} k 
            ON f.airline_code = k.airline_code
        GROUP BY f.airline_code, r.route, r.total_flights, r.avg_arr_delay, 
                 r.delayed_rate_pct, k.avg_arr_delay
    """)
    print("✓ Created view: airline_route_analysis")

    # 뷰 2: 고객 세그먼트 상세
    spark.sql(f"""
        CREATE OR REPLACE VIEW {DB_MART}.customer_segment_detail AS
        SELECT 
            s.*,
            RANK() OVER (PARTITION BY s.country ORDER BY s.customer_count DESC) AS rank_in_country,
            RANK() OVER (PARTITION BY s.segment ORDER BY s.customer_count DESC) AS rank_in_segment
        FROM {TABLE_CUSTOMER_SEGMENT_STATS} s
    """)
    print("✓ Created view: customer_segment_detail")


def main():
    print("=" * 60)
    print("STEP 3: Create Mart Layer (Gold/Analytics)")
    print("=" * 60)
    print()

    # Spark 세션 생성
    spark = get_spark_session("03_CreateMartLayer")

    # Mart 테이블 생성
    kpi_count = create_flight_delay_kpi(spark)
    route_count = create_route_performance(spark)
    segment_count = create_customer_segment_stats(spark)

    # 분석용 뷰 생성
    create_analytical_views(spark)

    # Mart 미리보기
    show_mart_previews(spark)

    # 비즈니스 인사이트
    generate_insights(spark)

    # 요약
    print("\n" + "=" * 60)
    print("✅ MART LAYER CREATION COMPLETE")
    print("=" * 60)
    print(f"\n📊 Summary:")
    print(f"   • Airline KPIs: {kpi_count} airlines")
    print(f"   • Route Performance: {route_count} routes")
    print(f"   • Customer Segments: {segment_count} combinations")
    print(f"\n📁 Tables created:")
    print(f"   • {TABLE_FLIGHT_DELAY_KPI}")
    print(f"   • {TABLE_ROUTE_PERFORMANCE}")
    print(f"   • {TABLE_CUSTOMER_SEGMENT_STATS}")
    print(f"\n📈 Views created:")
    print(f"   • {DB_MART}.airline_route_analysis")
    print(f"   • {DB_MART}.customer_segment_detail")
    print(f"\n🎯 Use Case:")
    print(f"   • BI Dashboard feeds")
    print(f"   • Executive reports")
    print(f"   • Performance monitoring")
    print(f"\n🚀 Next step: python scripts/04_pii_detection.py")
    print()

    # Spark 세션 종료
    stop_spark_session(spark)


if __name__ == "__main__":
    main()