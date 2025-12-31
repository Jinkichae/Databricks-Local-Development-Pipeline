"""
Spark 세션 설정 모듈
Delta Lake + Hive Metastore 지원
"""

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os
import sys

# Windows 환경 설정
if os.name == 'nt':
    # Python 경로 설정
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    # Hadoop 설정
    hadoop_home = r"C:\hadoop-3.3.6"
    os.environ['HADOOP_HOME'] = hadoop_home
    os.environ['PATH'] = f"{hadoop_home}\\bin;{os.environ['PATH']}"


def get_spark_session(app_name="AirlinesDemo"):
    """
    Delta Lake가 활성화된 Spark 세션 생성

    Args:
        app_name: Spark 애플리케이션 이름

    Returns:
        SparkSession: 설정된 Spark 세션
    """

    # 프로젝트 루트 디렉토리
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    warehouse_dir = os.path.join(project_root, "spark-warehouse")

    # Python 실행 파일 경로 (venv)
    python_exe = sys.executable

    # Spark 세션 빌더 설정
    builder = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir", warehouse_dir) \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.default.parallelism", "8") \
        .config("spark.pyspark.python", python_exe) \
        .config("spark.pyspark.driver.python", python_exe) \
        .config("spark.python.worker.reuse", "false") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.ui.showConsoleProgress", "false") \
        .enableHiveSupport()

    # Delta Lake 설정 적용
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # 로그 레벨 설정 (경고만 표시)
    spark.sparkContext.setLogLevel("WARN")

    print(f"✓ Spark Session Created: {app_name}")
    print(f"✓ Warehouse Directory: {warehouse_dir}")
    print(f"✓ Python Executable: {python_exe}")
    print(f"✓ Spark Version: {spark.version}")
    print(f"✓ Delta Lake: Enabled")
    print()

    return spark


def stop_spark_session(spark):
    """Spark 세션 종료"""
    spark.stop()
    print("✓ Spark Session Stopped")


# 데이터베이스 이름 상수
ENV = "dev"
DB_RAW = f"{ENV}_air_raw"
DB_SILVER = f"{ENV}_air_silver"
DB_MART = f"{ENV}_air_mart"
DB_META = f"{ENV}_air_meta"

# 테이블 이름 상수
TABLE_FLIGHTS_RAW = f"{DB_RAW}.flights_raw"
TABLE_CUSTOMER_RAW = f"{DB_RAW}.customer_raw"
TABLE_FLIGHTS_SILVER = f"{DB_SILVER}.flights_silver"
TABLE_CUSTOMER_SILVER = f"{DB_SILVER}.customer_silver"
TABLE_FLIGHT_DELAY_KPI = f"{DB_MART}.flight_delay_kpi"
TABLE_ROUTE_PERFORMANCE = f"{DB_MART}.route_performance"
TABLE_CUSTOMER_SEGMENT_STATS = f"{DB_MART}.customer_segment_stats"
TABLE_PII_DETECTION = f"{DB_META}.pii_detection_result"


def create_databases(spark):
    """모든 데이터베이스 생성"""
    databases = [DB_RAW, DB_SILVER, DB_MART, DB_META]

    for db in databases:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
        print(f"✓ Database: {db}")

    print()