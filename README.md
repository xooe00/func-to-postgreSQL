# 1️⃣ 목표

KHNP 4개 API 데이터를

```
Function App → Event Hub(4 파티션) → Stream Analytics → ADLS
```

구조로 자동 수집·분기·저장하는 실시간 파이프라인 구축

---

# 2️⃣ 데이터 수집 계층 (Function App)

### ✔ 수행 내용

- Azure Function App (`func-khnp-collector`) 생성
- Timer Trigger (10분 주기)
- 4개 API 호출:
    - weather
    - air
    - pwr
    - radiorate
- 발전소 코드:
    - WS, KR, YK, UJ, SU

### ✔ 처리 방식

각 API × 발전소 조합으로 이벤트 생성:

```json
{
  "api": "...",
  "genName": "...",
  "collected_at": "...",
  "data": {...}
}
```

### ✔ 파티션 고정 매핑

```
weather   → 0
air       → 1
pwr       → 2
radiorate → 3
```

Event Hub 전송 시:

```python
create_batch(partition_id="0")
```

으로 명시적 라우팅

---

# 3️⃣ 메시지 브로커 계층 (Event Hub)

### ✔ 구성

- 이벤트 허브 생성
- 파티션 4개
- Consumer Group 별도 생성

### ✔ 동작

- Function App이 API별로 지정 파티션에 이벤트 전송
- 메시지는 Retention 기간 동안 저장

---

# 4️⃣ 스트리밍 처리 계층 (Stream Analytics)

### ✔ 작업 생성

- `asa-khnp-collector` 배포
- 관리 ID(System Assigned) 활성화
- Storage Blob Data Contributor 권한 부여

### ✔ 입력

- Event Hub 연결
- JSON 직렬화

### ✔ 출력

ADLS Gen2 컨테이너 4개:

```
khnp-weather
khnp-air
khnp-pwr
khnp-radiorate
```

파일 형식: Parquet

파일 분할 기준: 최소행수 / 최대시간 설정

---

# 5️⃣ 쿼리 설계

초기:

```sql
SELECT *
```

이후 수정:

- `partition_id`
- `PartitionId`

제외하고 저장하도록 명시적 컬럼 선택

```sql
SELECT
    api,
    genName,
    collected_at,
    data,
    EventProcessedUtcTime,
    EventEnqueuedUtcTime
```

또한:

- 하이픈(-) 포함 alias는 `[khnp-weather]` 형태로 작성

---

# 6️⃣ 실행 및 동작 확인

- 작업 시작(Start → Now)
- 상태: Running
- ADLS에 시간 기반 폴더 생성
- Parquet 파일 생성 확인

---

# 7️⃣ 오늘 배운 핵심 개념

### 🔹 파티션은 수정 불가 (이미 저장된 데이터는 이동 불가)

### 🔹 Stream Analytics는

- 시작 시점 이후 데이터만 처리
- Retention 내라면 과거 재처리 가능

### 🔹 SELECT * EXCEPT는 지원되지 않음

→ 명시적 컬럼 나열 필요

### 🔹 실무 아키텍처 패턴

```
Raw 적재 (SELECT *)
→ 분석 레이어에서 정제
```

---

# 8️⃣ 현재 완성된 아키텍처

```
[KHNP API]
        ↓
[Azure Function]
        ↓ (partition 고정)
[Event Hub - 4 partitions]
        ↓
[Stream Analytics]
        ↓
[ADLS Gen2 - Parquet]
```

---

# 🎯 실시간 데이터 레이크 적재 파이프라인

이 구조는 그대로:

- Power BI 연결 가능
- Fabric Lakehouse 확장 가능
- RAG 인덱싱 가능
- AI 분석 파이프라인 확장 가능
