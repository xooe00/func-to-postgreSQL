import azure.functions as func
import logging
import json
import psycopg2
import os
from azure.storage.blob import BlobServiceClient
from datetime import datetime

app = func.FunctionApp()

@app.timer_trigger(schedule="0 */10 * * * *", arg_name="myTimer", run_on_startup=False)
def timer_blob_to_postgres(myTimer: func.TimerRequest):
    logging.info(f"=== 타이머 트리거 시작: {datetime.now()} ===")

    blob_conn_str = os.environ.get("AzureWebJobsStorage")
    pg_conn_str = os.environ.get("POSTGRES_CONNECTION_STRING")
    container_name = "events-json"

    try:
        # 1. PostgreSQL 연결 테스트
        with psycopg2.connect(pg_conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT version();")
                logging.info(f"✅ DB 연결 성공: {cursor.fetchone()}")
                
                # 2. 블롭 처리 함수 호출
                process_blobs(blob_conn_str, container_name, cursor, conn)
                
    except Exception as e:
        logging.error(f"❌ DB 연결 또는 처리 중 오류: {str(e)}")

def process_blobs(blob_conn_str, container_name, cursor, conn):
    blob_service_client = BlobServiceClient.from_connection_string(blob_conn_str)
    container_client = blob_service_client.get_container_client(container_name)
    
    # raw-json/ 폴더 내 모든 파일 리스트업
    blobs = list(container_client.list_blobs(name_starts_with="raw-json/"))
    logging.info(f"조회된 총 항목 수: {len(blobs)}개")

    for blob in blobs:
        # [수정 포인트] 폴더(크기가 0이거나 이름이 경로인 경우) 제외 로직
        if blob.size == 0 or blob.name.endswith('/'):
            continue
            
        if not blob.name.lower().endswith('.json'):
            continue

        logging.info(f"[{blob.name}] 데이터 삽입 시작...")
        
        try:
            blob_client = container_client.get_blob_client(blob)
            blob_data = blob_client.download_blob().readall().decode('utf-8')
            lines = [line for line in blob_data.strip().split('\n') if line.strip()]
            
            for line in lines:
                item = json.loads(line)
                sql = """
                    INSERT INTO api_collection (
                        api, gen_name, collected_at, data, 
                        event_processed_at, event_enqueued_at
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """
                cursor.execute(sql, (
                    item.get('api'), item.get('genName'), item.get('collected_at'),
                    json.dumps(item.get('data')), 
                    item.get('EventProcessedUtcTime'), item.get('EventEnqueuedUtcTime')
                ))

            # 처리 완료 후 이동 (복사 후 삭제)
            new_blob_name = blob.name.replace("raw-json/", "processed/")
            new_blob_client = container_client.get_blob_client(new_blob_name)
            new_blob_client.start_copy_from_url(blob_client.url)
            container_client.delete_blob(blob.name)
            logging.info(f"✅ 완료 및 이동: {new_blob_name}")

        except Exception as file_err:
            logging.error(f"파일 처리 실패 ({blob.name}): {str(file_err)}")

    conn.commit()