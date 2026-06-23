# [가이드라인] v2 아카이버 이중 쓰기 차단 및 아카이브 테이블 상태 관리 일원화

> 작성일: 2026-06-08  
> Google Drive 무정지 이관 파이프라인(Phase 4) 구동 후, 차세대 비동기 아카이버 `pdf_archiver_v2.py`가 기동될 때 PostgreSQL 메인 테이블(`tbl_sec_reports`)에 가해지는 DB 쓰기 트랜잭션 부하를 완전히 차단하기 위한 아키텍처 가이드라인입니다.

---

## 🗺️ 개요

현재 이관 중인 구글 드라이브 백엔드(`googledrive`)를 기반으로 정상 기동할 `pdf_archiver_v2.py`는 기존 레거시(`v1`)의 단점을 극복하고 성능이 대폭 향상되었으나, 여전히 메인 테이블(`tbl_sec_reports`)의 `pdf_sync_status`를 직접 업데이트하고 조회하는 **이중 쓰기(Double Update) 구조**를 계승하고 있습니다.

이 문서는 메인 테이블을 **순수 읽기 전용(Read-Only SELECT)**으로 완벽히 격리하고, 모든 다운로드/업로드 상태 관리 및 메타데이터를 아카이브 테이블(`tbl_sec_reports_pdf_archive`)에 일원화하여 DB 성능 병목을 원천적으로 소멸시키기 위한 구체적 설계 및 수정 로직을 담고 있습니다.

---

## 🛠️ 1. 대상 조회 쿼리 개편 (`fetch_targets`)

메인 테이블(`tbl_sec_reports`)에 직접 업데이트를 치지 않기 때문에, 대상을 스캔할 때는 아카이브 테이블(`tbl_sec_reports_pdf_archive`)을 `LEFT JOIN`하여 상태값(`pdf_sync_status`)과 재시도 횟수(`retry_count`)를 판단해야 합니다.

### 수정할 코드 위치
* 파일 경로: `scripts/pdf_archiver_v2.py`
* 함수명: `fetch_targets(conn, limit)`

### 💻 변경 내용 (Diff)
```diff
 async def fetch_targets(conn: asyncpg.Connection, limit: int) -> list[asyncpg.Record]:
-    """pdf_sync_status=0(대기) 또는 3(실패)인 레코드 fetch"""
+    """[Antigravity 튜닝] 메인 테이블 쓰기(UPDATE) 부하를 제거하기 위해, 
+    아카이브 테이블을 LEFT JOIN하여 동기화 진행 상태를 실시간으로 판단해 가져옵니다.
+    """
     return await conn.fetch(
         f"""
-        SELECT report_id, sec_firm_order, key, pdf_url, telegram_url, download_url,
-               firm_nm, article_title, reg_dt, retry_count
-        FROM {SOURCE_TABLE}
-        WHERE pdf_sync_status IN (0, 3)
-          AND COALESCE(retry_count, 0) < {DB_RETRY_LIMIT}
-          AND (NULLIF(BTRIM(pdf_url), '') IS NOT NULL
-               OR NULLIF(BTRIM(telegram_url), '') IS NOT NULL
-               OR NULLIF(BTRIM(download_url), '') IS NOT NULL
-               OR NULLIF(BTRIM(key), '') IS NOT NULL)
-        ORDER BY retry_count ASC, reg_dt DESC, report_id ASC
+        SELECT s.report_id, s.sec_firm_order, s.key, s.pdf_url, s.telegram_url, s.download_url,
+               s.firm_nm, s.article_title, s.reg_dt, COALESCE(a.retry_count, 0) as retry_count
+        FROM {SOURCE_TABLE} s
+        LEFT JOIN {ARCHIVE_TABLE} a ON s.report_id = a.report_id
+        WHERE COALESCE(a.pdf_sync_status, 0) IN (0, 3)
+          AND COALESCE(a.retry_count, 0) < {DB_RETRY_LIMIT}
+          AND (NULLIF(BTRIM(s.pdf_url), '') IS NOT NULL
+               OR NULLIF(BTRIM(s.telegram_url), '') IS NOT NULL
+               OR NULLIF(BTRIM(s.download_url), '') IS NOT NULL
+               OR NULLIF(BTRIM(s.key), '') IS NOT NULL)
+        ORDER BY COALESCE(a.retry_count, 0) ASC, s.reg_dt DESC, s.report_id ASC
         LIMIT $1
         """,
         limit,
     )
```

---

## ⚡ 2. 메인 테이블 UPDATE 전면 차단 (`process_one`)

`process_one`은 다운로드 시도, 업로드 시도 성공/실패 시 매번 `update_source_status`를 호출하여 메인 테이블에 쓰기 트랜잭션을 실행하고 있습니다. 이를 안전하게 제거하고 아카이브 전용 상태로 전환합니다.

### 💻 변경 내용 (Diff)

#### ① 다운로드 실패 또는 URL 부재 시 예외 처리
기존에 메인 테이블을 바로 업데이트하던 구문을 제거하고, 오직 아카이브 테이블(`tbl_sec_reports_pdf_archive`)에 실패 상태(`pdf_sync_status = 3`) 및 누적 시도 횟수(`retry_delta = 1`)를 UPSERT하도록 변경합니다.

```diff
         if not pdf_url:
-            await update_source_status(conn, report_id, 3, 1)
+            # 메인 쓰기 배제: 아카이브 테이블에만 실패 상태와 카운트 누적
+            await upsert_archive(conn, report_id, firm, title, reg_dt, pdf_url,
+                                 storage_key="", file_size=0, page_count=0,
+                                 pdf_hash="", pdf_hash_bytes=None, success=False, retry_delta=1)
             return False
```
```diff
         if not ok or not _is_pdf(tmp_path):
             if tmp_path.exists():
                 tmp_path.unlink(missing_ok=True)
-            await update_source_status(conn, report_id, 3, 1)
+            # 메인 쓰기 배제: 아카이브 테이블에만 실패 상태와 카운트 누적
+            await upsert_archive(conn, report_id, firm, title, reg_dt, pdf_url,
+                                 storage_key="", file_size=0, page_count=0,
+                                 pdf_hash="", pdf_hash_bytes=None, success=False, retry_delta=1)
             return False
```

#### ② 중복 감지 시 canonical 복사 및 업로드 스킵
```diff
         if existing:
             # 중복: canonical 참조만 복사, 업로드 스킵
             log.info(f"[{report_id}] DUPLICATE → canonical={existing['report_id']} hash={pdf_hash_hex[:16]}...")
             await upsert_archive(conn, report_id, firm, title, reg_dt, pdf_url,
                                  existing["storage_key"], existing["file_size"] or file_size,
                                  existing["page_count"] or 0, pdf_hash_hex, pdf_hash_bytes, True)
-            await update_source_status(conn, report_id, 2, 0, pdf_hash_bytes)
+            # await update_source_status(conn, report_id, 2, 0, pdf_hash_bytes) # 메인 쓰기 주석 처리
             tmp_path.unlink(missing_ok=True)
             return True
```

#### ③ 신규 업로드 성공 시
```diff
         if uploaded:
             await upsert_archive(conn, report_id, firm, title, reg_dt, pdf_url,
                                  storage_key, file_size, 0, pdf_hash_hex, pdf_hash_bytes, True)
-            await update_source_status(conn, report_id, 2, 0, pdf_hash_bytes)
+            # await update_source_status(conn, report_id, 2, 0, pdf_hash_bytes) # 메인 쓰기 주석 처리
             local_target.unlink(missing_ok=True)
             log.info(f"[{report_id}] UPLOADED {firm} | {title[:30]}...")
             return True
```

#### ④ 신규 업로드 실패 시 (일시적 스토리지 장애 등)
```diff
         else:
             # 업로드 실패 → 파일 보존, 다음 run에서 재시도
             await upsert_archive(conn, report_id, firm, title, reg_dt, pdf_url,
                                  storage_key, file_size, 0, pdf_hash_hex, pdf_hash_bytes, False, retry_delta=1)
-            await update_source_status(conn, report_id, 3, 1, pdf_hash_bytes)
+            # await update_source_status(conn, report_id, 3, 1, pdf_hash_bytes) # 메인 쓰기 주석 처리
             log.warning(f"[{report_id}] UPLOAD FAILED {firm} | {title[:30]}...")
             return False
```

---

## 🎯 3. 리팩토링 기대효과

1. **메인 데이터베이스 잠금 병목 원천 차단**:
   - `tbl_sec_reports` 테이블은 증권사 피드로부터 상시 대량의 신규 게시글이 수집되는 메인 허브입니다.
   - 배치 아카이버에 의한 행 잠금(Row Lock)이 완전히 사라지므로, 수집 속도 저하 및 교착 상태(Deadlock) 위험이 영구 소멸됩니다.
2. **이관 인프라 정렬**:
   - 구글 드라이브 백엔드 기반으로 구동되는 차세대 아카이버에 완전히 정렬된 성능 튜닝 패치로 활용됩니다.
   - `storage_backend = 'googledrive'` 저장 상태를 아카이브 테이블에서 무결하게 유지 관리합니다.
3. **영속성 및 백업 안정성 보장**:
   - 아카이브 전용 테이블(`tbl_sec_reports_pdf_archive`)에 PK(`report_id`) 기반 고속 인덱스 및 UPSERT를 치기 때문에, 실패 재시도 카운트 및 동기화 상태 정합성이 하나의 전용 공간에서 완벽히 수렴합니다.
