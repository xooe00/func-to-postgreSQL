import os
import json
import logging
import requests
import xmltodict
import azure.functions as func
from azure.eventhub import EventHubProducerClient, EventData
from datetime import datetime, timezone

app = func.FunctionApp()


def send_teams_notification(webhook_url: str | None, message: str) -> None:
    """Teams 채널로 웹훅 알림 전송"""
    if not webhook_url:
        return

    payload = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": "🚨 KHNP 수집기 장애 발생",
                            "weight": "Bolder",
                            "size": "Medium",
                            "color": "Attention",
                        },
                        {"type": "TextBlock", "text": message, "wrap": True},
                    ],
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "version": "1.0",
                },
            }
        ],
    }

    try:
        requests.post(webhook_url, json=payload, timeout=5)
    except Exception as e:
        logging.error(f"Teams 알림 전송 실패: {e}")


def fetch_khnp(url: str, service_key: str, gen_name: str) -> tuple[int, dict]:
    params = {"serviceKey": service_key, "genName": gen_name}
    try:
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code == 200:
            # KHNP 응답은 XML인 경우가 많아 XML 파싱
            return resp.status_code, xmltodict.parse(resp.text)
        return resp.status_code, {"error": f"HTTP {resp.status_code}"}
    except Exception as e:
        return 500, {"error": str(e)}


@app.function_name(name="khnp_to_eventhub")
@app.schedule(schedule="0 */10 * * * *", arg_name="timer", run_on_startup=False)
def khnp_to_eventhub(timer: func.TimerRequest) -> None:
    SERVICE_KEY = os.getenv("KHNP_SERVICE_KEY")
    EH_CONN_STR = os.getenv("EVENTHUB_CONNECTION_STRING")
    EH_NAME = os.getenv("EVENTHUB_NAME")
    TEAMS_WEBHOOK = os.getenv("TEAMS_WEBHOOK_URL")

    # 발전소 목록 (기본값 포함)
    GEN_NAMES = os.getenv("KHNP_GEN_NAMES", "WS,KR,YK,UJ,SU").split(",")

    # 필수 환경변수 체크
    missing = []
    if not SERVICE_KEY:
        missing.append("KHNP_SERVICE_KEY")
    if not EH_CONN_STR:
        missing.append("EVENTHUB_CONNECTION_STRING")
    if not EH_NAME:
        missing.append("EVENTHUB_NAME")

    if missing:
        msg = f"필수 환경변수 누락: {', '.join(missing)}"
        logging.error(msg)
        send_teams_notification(TEAMS_WEBHOOK, msg)
        return

    apis = [
        ("weather", "http://data.khnp.co.kr/environ/service/realtime/weather"),
        ("air", "http://data.khnp.co.kr/environ/service/realtime/air"),
        ("pwr", "http://data.khnp.co.kr/environ/service/realtime/pwr"),
        ("radiorate", "http://data.khnp.co.kr/environ/service/realtime/radiorate"),
    ]

    # API별 파티션 고정 매핑
    partition_map = {
        "weather": "0",
        "air": "1",
        "pwr": "2",
        "radiorate": "3",
    }

    producer = EventHubProducerClient.from_connection_string(
        EH_CONN_STR,
        eventhub_name=EH_NAME,
    )

    fail_details: list[str] = []

    try:
        with producer:
            for api_name, url in apis:
                pid = partition_map.get(api_name)
                if pid is None:
                    fail_details.append(f"- {api_name}: partition_map에 없음")
                    logging.error(f"partition_map 누락: {api_name}")
                    continue

                for gen_name in GEN_NAMES:
                    g_name = gen_name.strip()
                    if not g_name:
                        continue

                    status, data = fetch_khnp(url, SERVICE_KEY, g_name)

                    if status == 200 and "error" not in str(data):
                        event_body = {
                            "api": api_name,
                            "genName": g_name,
                            "partition_id": pid,
                            "collected_at": datetime.now(timezone.utc).isoformat(),
                            "data": data,
                        }

                        # 파티션 강제 라우팅: partition_id 지정
                        batch = producer.create_batch(partition_id=pid)
                        batch.add(EventData(json.dumps(event_body, ensure_ascii=False)))
                        producer.send_batch(batch)
                    else:
                        fail_details.append(f"- {api_name} ({g_name}): Status {status}")
                        logging.error(f"실패: {api_name}-{g_name} (Status {status})")

        # 수집 실패가 1건이라도 있는 경우에만 Teams 알림 전송
        if fail_details:
            error_msg = "\n".join(fail_details)
            send_teams_notification(
                TEAMS_WEBHOOK,
                f"다음 수집 항목에 문제가 발생했습니다:\n\n{error_msg}",
            )

    except Exception as e:
        # 시스템 전체 장애 시 알림
        send_teams_notification(TEAMS_WEBHOOK, f"🚨 **시스템 치명적 오류**: {str(e)}")
        logging.error(f"Fatal Error: {e}")