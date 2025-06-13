import argparse
import json
import os
from datetime import datetime, timedelta

from bitrix_delivery_manager import BitrixDeliveryManager  # импортируй свой основной класс


def get_manager(force_reload) -> BitrixDeliveryManager:
    webhook_url = os.environ.get("BITRIX_WEBHOOK_URL")
    cache_file = os.environ.get("BITRIX_CACHE_FILE", "bitrix_cache.json")
    updated_cache_file = os.environ.get("BITRIX_UPDATED_CACHE_FILE", "bitrix_updated_cache.json")

    if not webhook_url:
        raise ValueError("Не задан BITRIX_WEBHOOK_URL в переменных окружения")
    
    return BitrixDeliveryManager(webhook_url=webhook_url, cache_file=cache_file, force_reload=force_reload)


def run_refresh():
    manager = get_manager(force_reload=False)
    since = datetime.now(datetime.timezone.utc) - timedelta(minutes=1)
    manager.refresh_updates(since)


def run_load():
    get_manager(force_reload=True)


def run_get():
    manager = get_manager(force_reload=False)
    data = {
        "cache": manager.cache,
        "nested": manager.build_nested_structure()
    }
    print(json.dumps(data, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bitrix Cache Manager CLI")
    parser.add_argument("mode", choices=["refresh", "load", "get"], help="Режим запуска")

    args = parser.parse_args()

    if args.mode == "refresh":
        run_refresh()
    elif args.mode == "load":
        run_load()
    elif args.mode == "get":
        run_get()