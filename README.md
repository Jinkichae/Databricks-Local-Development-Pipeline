# Databricks Local Development Pipeline

Delta Lake 기반의 데이터 엔지니어링 파이프라인 로컬 개발 환경

## 📋 프로젝트 개요

이 프로젝트는 Databricks의 Medallion Architecture(Bronze-Silver-Gold)를 로컬 환경에서 구현한 데이터 파이프라인입니다. PySpark와 Delta Lake를 활용하여 항공 데이터를 처리하고, PII(개인식별정보) 검출 및 데이터 거버넌스를 포함합니다.

## 🏗️ 아키텍처
```
Bronze Layer (Raw)
    ↓
Silver Layer (Cleaned & Validated)
    ↓
Gold Layer (Analytics & Mart)
    ↓
Governance & PII Detection
```

### 데이터 레이어

- **Bronze Layer**: 원본 데이터 그대로 저장 (Raw Data)
- **Silver Layer**: 데이터 정제, 검증, PII 마스킹
- **Gold Layer**: 비즈니스 분석용 Mart 테이블
- **Meta Layer**: PII 검출 결과 및 거버넌스 메타데이터

## 🚀 주요 기능

- ✅ Delta Lake 기반 ACID 트랜잭션
- ✅ 타임 트래블 (버전 관리)
- ✅ 스키마 진화 (Schema Evolution)
- ✅ PII 자동 검출 및 마스킹
- ✅ 데이터 품질 검증
- ✅ Z-Ordering 최적화
- ✅ 데이터 거버넌스 정책

## 📊 데이터셋

### 생성되는 테이블

| Layer | 테이블명 | 설명 | 레코드 수 |
|-------|---------|------|-----------|
| Bronze | `flights_raw` | 항공편 원본 데이터 | 10,000 |
| Bronze | `customer_raw` | 고객 원본 데이터 (PII 포함) | 5,000 |
| Silver | `flights_silver` | 정제된 항공편 데이터 | 10,000 |
| Silver | `customer_silver` | PII 마스킹된 고객 데이터 | 5,000 |
| Gold | `flight_delay_kpi` | 항공사별 지연 KPI | - |
| Gold | `route_performance` | 노선별 성과 분석 | - |
| Gold | `customer_segment_stats` | 고객 세그먼트 통계 | - |
| Meta | `pii_detection_result` | PII 검출 결과 | - |

## 🛠️ 기술 스택

- **Python**: 3.8+
- **PySpark**: 3.5.0
- **Delta Lake**: 2.4.0
- **Apache Hadoop**: 3.3.6 (Windows용 winutils 포함)
- **Hive Metastore**: 내장 Derby DB

## 📁 프로젝트 구조
```
databricks-local-pipeline/
├── config/
│   └── spark_config.py          # Spark 세션 및 설정
├── scripts/
│   ├── 01_generate_sample_data.py    # Bronze Layer 생성
│   ├── 02_create_silver_layer.py     # Silver Layer 생성
│   ├── 03_create_mart_layer.py       # Gold Layer 생성
│   ├── 04_detect_pii.py              # PII 검출
│   └── 05_apply_governance.py        # 거버넌스 정책
├── spark-warehouse/             # Delta Lake 테이블 저장소
├── requirements.txt             # Python 의존성
├── .gitignore                   # Git 제외 파일
└── README.md                    # 프로젝트 문서

생성되는 디렉토리:
├── metastore_db/               # Hive Metastore
├── derby.log                   # Derby 로그
└── venv/                       # Python 가상환경
```

## 💻 설치 및 실행

### 1. 사전 요구사항

#### Windows 환경
- Python 3.8 이상
- Java 11 (PySpark 요구사항)
- Hadoop winutils (Windows 전용)

#### Hadoop winutils 설치 (Windows)
```bash
# 1. winutils 다운로드
# https://github.com/cdarlint/winutils 에서 hadoop-3.3.6 버전 다운로드

# 2. 경로 설정
C:\hadoop-3.3.6\bin\winutils.exe

# 3. 환경 변수 설정 (선택사항, 코드에서 자동 설정됨)
HADOOP_HOME=C:\hadoop-3.3.6
```

### 2. 프로젝트 설치
```bash
# 저장소 클론
git clone https://github.com/yourusername/databricks-local-pipeline.git
cd databricks-local-pipeline

# 가상환경 생성
python -m venv venv

# 가상환경 활성화
# Windows
venv\Scripts\activate
# Linux/Mac
source venv/bin/activate

# 의존성 설치
pip install -r requirements.txt
```

### 3. 파이프라인 실행
```bash
# Step 1: Bronze Layer - 샘플 데이터 생성
python scripts/01_generate_sample_data.py

# Step 2: Silver Layer - 데이터 정제 및 검증
python scripts/02_create_silver_layer.py

# Step 3: Gold Layer - 분석용 Mart 생성
python scripts/03_create_mart_layer.py

# Step 4: PII 검출 (선택사항)
python scripts/04_detect_pii.py

# Step 5: 거버넌스 정책 적용 (선택사항)
python scripts/05_apply_governance.py
```

## 📖 사용 예시

### 데이터 조회
```python
from config.spark_config import *

# Spark 세션 생성
spark = get_spark_session("DataAnalysis")

# Bronze Layer 조회
df_raw = spark.table("dev_air_raw.flights_raw")
df_raw.show(10)

# Silver Layer 조회
df_silver = spark.table("dev_air_silver.flights_silver")
df_silver.show(10)

# SQL 쿼리
spark.sql("""
    SELECT airline_code, 
           AVG(arr_delay) as avg_delay,
           COUNT(*) as flight_count
    FROM dev_air_silver.flights_silver
    GROUP BY airline_code
    ORDER BY avg_delay DESC
""").show()

# 세션 종료
stop_spark_session(spark)
```

### 타임 트래블
```python
# 특정 버전의 데이터 조회
df_v0 = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .table("dev_air_silver.flights_silver")

# 특정 시간의 데이터 조회
df_timestamp = spark.read.format("delta") \
    .option("timestampAsOf", "2025-12-31 13:00:00") \
    .table("dev_air_silver.flights_silver")

# 히스토리 조회
spark.sql("DESCRIBE HISTORY dev_air_silver.flights_silver").show()
```

## 🔒 PII 보호

### 검출되는 PII 유형

- ✅ 이름 (Full Name)
- ✅ 이메일 (Email)
- ✅ 전화번호 (Phone)
- ✅ 여권번호 (Passport Number)
- ✅ 신용카드 번호
- ✅ 주민등록번호/SSN

### 마스킹 예시
```python
# 원본 (Bronze)
{
    "full_name": "John Doe",
    "email": "john.doe@example.com",
    "phone": "+1-555-123-4567"
}

# 마스킹 후 (Silver)
{
    "full_name": "J*** D***",
    "email": "j***@example.com",
    "phone": "+1-555-***-****"
}
```

## 🧪 테스트
```bash
# 전체 파이프라인 테스트
python -m pytest tests/

# 개별 레이어 테스트
python -m pytest tests/test_bronze_layer.py
python -m pytest tests/test_silver_layer.py
python -m pytest tests/test_pii_detection.py
```

## 📈 성능 최적화

- **Z-Ordering**: 자주 필터링하는 컬럼 최적화
- **Partition Pruning**: 날짜별 파티션
- **Data Skipping**: Min/Max 통계 활용
- **Caching**: 반복 쿼리 성능 향상

## 🐛 문제 해결

### Windows에서 "HADOOP_HOME is not set" 오류
```python
# spark_config.py에서 자동 처리됨
# 또는 수동으로 환경 변수 설정
import os
os.environ['HADOOP_HOME'] = r'C:\hadoop-3.3.6'
```

### "Python worker failed to connect back" 오류
```python
# spark_config.py에 Python 경로 명시됨
.config("spark.pyspark.python", sys.executable)
.config("spark.pyspark.driver.python", sys.executable)
```

### 임시 파일 삭제 실패 경고
```
# 무시해도 됨 - Windows 파일 잠금 문제
# 실제 데이터나 실행에는 영향 없음
```

## 📝 라이선스

MIT License

## 👥 기여

Pull Request는 언제나 환영합니다!

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📧 연락처

프로젝트 관리자: 채진기(fbg6455@naver.com)

프로젝트 링크: https://github.com/Jinkichae/Databricks-Local-Development-Pipeline

## 🙏 감사의 글

- [Apache Spark](https://spark.apache.org/)
- [Delta Lake](https://delta.io/)
- [Databricks](https://www.databricks.com/)

## 📚 추가 자료

- [Delta Lake Documentation](https://docs.delta.io/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Data Governance Best Practices](https://www.databricks.com/glossary/data-governance)
- [GDPR Compliance Guide](https://gdpr.eu/)
