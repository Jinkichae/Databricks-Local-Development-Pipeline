"""
04. PII 검출 스크립트
Rule 기반 + AI 기반 PII 검출 파이프라인
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.spark_config import *
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import re

# PII 검출 규칙 정의
EMAIL_PATTERN = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$'
PHONE_PATTERN = r'^\+?\d{1,3}[-\s]?\(?\d{3}\)?[-\s]?\d{3}[-\s]?\d{4}$'
PASSPORT_PATTERN = r'^P\d{8}$'


def rule_based_pii_detection(spark):
    """Rule 기반 PII 검출"""
    print("Running Rule-based PII Detection...")

    df_customer = spark.table(TABLE_CUSTOMER_SILVER)

    # 각 컬럼별 PII 타입 판별
    df_pii = df_customer.select(
        'customer_id',
        'full_name',
        'email',
        'phone',
        'passport_no',

        # Email 검증
        F.when(F.col('email').rlike(EMAIL_PATTERN), 'EMAIL')
        .otherwise('INVALID').alias('email_pii_type'),

        # Phone 검증
        F.when(F.col('phone').rlike(PHONE_PATTERN), 'PHONE')
        .otherwise('INVALID').alias('phone_pii_type'),

        # Passport 검증
        F.when(F.col('passport_no').rlike(PASSPORT_PATTERN), 'PASSPORT')
        .otherwise('INVALID').alias('passport_pii_type'),

        # Full Name은 항상 PII
        F.lit('NAME').alias('name_pii_type')
    )

    # PII 검출 통계
    print("\n📊 Rule-based Detection Results:")
    print(f"   Valid Emails:    {df_pii.filter(F.col('email_pii_type') == 'EMAIL').count():,}")
    print(f"   Valid Phones:    {df_pii.filter(F.col('phone_pii_type') == 'PHONE').count():,}")
    print(f"   Valid Passports: {df_pii.filter(F.col('passport_pii_type') == 'PASSPORT').count():,}")

    return df_pii


def ai_based_pii_classifier(text):
    """
    AI 기반 PII 분류기 (시뮬레이션)
    실제 환경에서는 NER 모델이나 LLM을 사용
    """
    if text is None or text == '':
        return 'NONE'

    text_lower = text.lower()

    # Email 패턴
    if '@' in text and '.' in text:
        if re.match(EMAIL_PATTERN, text):
            return 'EMAIL'
        else:
            return 'EMAIL_LIKE'

    # Phone 패턴
    if re.search(r'\d{3}[-\s]?\d{3}[-\s]?\d{4}', text):
        return 'PHONE'

    # Passport 패턴
    if text.startswith('P') and len(text) == 9:
        return 'PASSPORT'

    # 이름 패턴 (공백으로 구분된 2개 이상의 단어)
    words = text.split()
    if len(words) >= 2 and all(w.isalpha() for w in words):
        return 'NAME'

    # ID 패턴
    if text.startswith('CUST') and len(text) == 10:
        return 'CUSTOMER_ID'

    return 'UNKNOWN'


def ai_based_pii_detection(spark, df_rule_based):
    """AI 기반 PII 검출 (UDF 사용)"""
    print("\nRunning AI-based PII Detection...")

    # UDF 등록
    pii_classifier_udf = F.udf(ai_based_pii_classifier, StringType())

    # AI 분류 적용
    df_ai = df_rule_based.select(
        'customer_id',
        'full_name',
        'email',
        'phone',
        'passport_no',
        'email_pii_type',
        'phone_pii_type',
        'passport_pii_type',
        'name_pii_type',

        # AI 기반 분류 추가
        pii_classifier_udf('full_name').alias('ai_name_type'),
        pii_classifier_udf('email').alias('ai_email_type'),
        pii_classifier_udf('phone').alias('ai_phone_type'),
        pii_classifier_udf('passport_no').alias('ai_passport_type'),

        # 검출 신뢰도 계산
        F.when(
            (F.col('email_pii_type') == pii_classifier_udf('email')) &
            (F.col('phone_pii_type') == pii_classifier_udf('phone')) &
            (F.col('passport_pii_type') == pii_classifier_udf('passport_no')),
            'HIGH'
        ).when(
            (F.col('email_pii_type') == pii_classifier_udf('email')) |
            (F.col('phone_pii_type') == pii_classifier_udf('phone')),
            'MEDIUM'
        ).otherwise('LOW').alias('detection_confidence'),

        F.current_timestamp().alias('detection_timestamp')
    )

    # 통계
    print("\n📊 AI-based Detection Results:")
    confidence_dist = df_ai.groupBy('detection_confidence').count().collect()
    for row in confidence_dist:
        print(f"   {row['detection_confidence']} confidence: {row['count']:,}")

    return df_ai


def save_pii_detection_results(spark, df_pii):
    """PII 검출 결과 저장"""
    print("\nSaving PII detection results...")

    df_pii.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(TABLE_PII_DETECTION)

    count = df_pii.count()
    print(f"✓ Saved {count:,} records → {TABLE_PII_DETECTION}")

    return count


def create_masked_view(spark):
    """마스킹된 고객 뷰 생성"""
    print("\nCreating masked customer view...")

    spark.sql(f"""
        CREATE OR REPLACE VIEW {DB_SILVER}.customer_masked_view AS
        SELECT
            customer_id,
            CONCAT(SUBSTRING(full_name, 1, 1), '***') AS full_name_masked,
            CONCAT(SUBSTRING(email, 1, 3), '***@***', 
                   SUBSTRING_INDEX(email, '@', -1)) AS email_masked,
            CONCAT(SUBSTRING(phone, 1, 6), '***-****') AS phone_masked,
            '***MASKED***' AS passport_masked,
            country,
            segment,
            transformation_timestamp
        FROM {TABLE_CUSTOMER_SILVER}
    """)

    print(f"✓ Created view: {DB_SILVER}.customer_masked_view")

    # 마스킹 예시
    print("\n🔒 Masked Data Sample:")
    spark.sql(f"SELECT * FROM {DB_SILVER}.customer_masked_view LIMIT 3").show(truncate=False)


def generate_pii_report(spark):
    """PII 검출 리포트 생성"""
    print("\n" + "=" * 60)
    print("PII DETECTION REPORT")
    print("=" * 60)

    # 전체 통계
    print("\n📊 Overall Statistics:")
    overall = spark.sql(f"""
        SELECT 
            COUNT(*) AS total_records,
            SUM(CASE WHEN detection_confidence = 'HIGH' THEN 1 ELSE 0 END) AS high_confidence,
            SUM(CASE WHEN detection_confidence = 'MEDIUM' THEN 1 ELSE 0 END) AS medium_confidence,
            SUM(CASE WHEN detection_confidence = 'LOW' THEN 1 ELSE 0 END) AS low_confidence
        FROM {TABLE_PII_DETECTION}
    """).collect()[0]

    print(f"   Total Records:      {overall['total_records']:,}")
    print(
        f"   High Confidence:    {overall['high_confidence']:,} ({overall['high_confidence'] * 100 / overall['total_records']:.1f}%)")
    print(
        f"   Medium Confidence:  {overall['medium_confidence']:,} ({overall['medium_confidence'] * 100 / overall['total_records']:.1f}%)")
    print(
        f"   Low Confidence:     {overall['low_confidence']:,} ({overall['low_confidence'] * 100 / overall['total_records']:.1f}%)")

    # PII 타입별 분포
    print("\n📋 PII Type Distribution:")
    spark.sql(f"""
        SELECT 
            'Email' AS pii_type,
            COUNT(*) AS count
        FROM {TABLE_PII_DETECTION}
        WHERE email_pii_type = 'EMAIL'

        UNION ALL

        SELECT 
            'Phone' AS pii_type,
            COUNT(*) AS count
        FROM {TABLE_PII_DETECTION}
        WHERE phone_pii_type = 'PHONE'

        UNION ALL

        SELECT 
            'Passport' AS pii_type,
            COUNT(*) AS count
        FROM {TABLE_PII_DETECTION}
        WHERE passport_pii_type = 'PASSPORT'

        UNION ALL

        SELECT 
            'Name' AS pii_type,
            COUNT(*) AS count
        FROM {TABLE_PII_DETECTION}
        WHERE name_pii_type = 'NAME'
    """).show(truncate=False)

    # 검출 불일치 케이스
    print("\n⚠️  Rule vs AI Discrepancies:")
    discrepancies = spark.sql(f"""
        SELECT COUNT(*) AS count
        FROM {TABLE_PII_DETECTION}
        WHERE email_pii_type != ai_email_type
           OR phone_pii_type != ai_phone_type
           OR passport_pii_type != ai_passport_type
    """).collect()[0]['count']

    print(f"   Discrepant Records: {discrepancies:,}")

    if discrepancies > 0:
        print("\n   Sample Discrepancies:")
        spark.sql(f"""
            SELECT 
                customer_id,
                email_pii_type,
                ai_email_type,
                phone_pii_type,
                ai_phone_type
            FROM {TABLE_PII_DETECTION}
            WHERE email_pii_type != ai_email_type
               OR phone_pii_type != ai_phone_type
            LIMIT 5
        """).show(truncate=False)


def main():
    print("=" * 60)
    print("STEP 4: PII Detection Pipeline")
    print("=" * 60)
    print()

    # Spark 세션 생성
    spark = get_spark_session("04_PIIDetection")

    # Rule 기반 검출
    df_rule = rule_based_pii_detection(spark)

    # AI 기반 검출
    df_ai = ai_based_pii_detection(spark, df_rule)

    # 결과 저장
    result_count = save_pii_detection_results(spark, df_ai)

    # 마스킹 뷰 생성
    create_masked_view(spark)

    # 리포트 생성
    generate_pii_report(spark)

    # 요약
    print("\n" + "=" * 60)
    print("✅ PII DETECTION COMPLETE")
    print("=" * 60)
    print(f"\n🔍 Detection Summary:")
    print(f"   • Records scanned: {result_count:,}")
    print(f"   • Methods used: Rule-based + AI-based")
    print(f"   • Confidence levels: HIGH, MEDIUM, LOW")
    print(f"\n📁 Outputs:")
    print(f"   • {TABLE_PII_DETECTION}")
    print(f"   • {DB_SILVER}.customer_masked_view")
    print(f"\n🔒 Data Governance:")
    print(f"   • PII identified and catalogued")
    print(f"   • Masked views created")
    print(f"   • Ready for access control")
    print(f"\n🚀 Next step: python scripts/05_analytics_queries.py")
    print()

    # Spark 세션 종료
    stop_spark_session(spark)


if __name__ == "__main__":
    main()