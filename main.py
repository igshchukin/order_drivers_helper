from datetime import datetime, timezone, date
from fastapi import FastAPI
import json
import os
from pydantic import BaseModel

from webservice.src.driver_index_builder import get_drivers_deliveries
from webservice.src.bitrix_delivery_manager import BitrixDeliveryManager
from webservice.src.driver_index_builder import DriverIndexBuilder


def encrypt_response(data: dict) -> dict:
    return {'data': data}
    json_data = json.dumps(data).encode("utf-8")
    encrypted = fernet.encrypt(json_data)
    return {"data": encrypted.decode("utf-8")}

# --- Хранилище ---
manager: BitrixDeliveryManager = BitrixDeliveryManager(
    os.environ.get("BITRIX_WEBHOOK_URL"),
    os.environ.get("BITRIX_CACHE_FILE", "bitrix_cache.json"),
    force_reload=True
)
driver_index: DriverIndexBuilder = DriverIndexBuilder(manager)
last_update_time = datetime.now(timezone.utc)


# --- FastAPI ---
app = FastAPI()


# --- Роуты ---
@app.post("/load")
async def api_load():
    global manager, driver_index, last_update_time
    manager.refresh_updates(last_update_time)
    manager = BitrixDeliveryManager(
        os.environ.get("BITRIX_WEBHOOK_URL"),
        os.environ.get("BITRIX_CACHE_FILE", "webservice/src/bitrix_cache.json"),
        force_reload=True
    )
    driver_index = DriverIndexBuilder(manager)
    last_update_time = datetime.now(timezone.utc)
    

    return encrypt_response({"status": "loaded"})


@app.get("/refresh")
async def api_refresh():
    global manager, driver_index, last_update_time
    if manager is None:
        return encrypt_response({"error": "BitrixDeliveryManager is not loaded"})
    manager.refresh_updates(last_update_time)
    driver_index = DriverIndexBuilder(manager)
    last_update_time = datetime.now(timezone.utc)

    return encrypt_response({"status": "refreshed"})


@app.get("/get")
async def api_get():
    return encrypt_response({
        "cache": manager.cache,
        "structure": manager.build_nested_structure(),
    })


@app.get("/drivers_deliveries")
async def api_drivers_deliveries():
    global driver_index
    return encrypt_response(
        get_drivers_deliveries(
            bitrix_delivery_manager=manager,
            driver_index_builder=driver_index
        )
    )


@app.get("/delivery_info/{delivery_id}")
async def api_delivery_info(delivery_id: int):
    try:
        info = manager.get_delivery_full_info_by_id(delivery_id)
        return encrypt_response(info)
    except ValueError as e:
        return encrypt_response({"error": str(e)})


@app.get("/delivery_driver/{delivery_id}")
async def api_delivery_driver(delivery_id: int):
    try:
        info = manager.get_delivery_full_info_by_id(delivery_id)
        return encrypt_response({"driver": info.get("driver")})
    except ValueError as e:
        return encrypt_response({"error": str(e)})


@app.get("/driver_deliveries/{driver_id}")
async def api_driver_deliveries(driver_id: int):
    try:
        deliveries = manager.get_deliveries_grouped_by_driver(
            search_driver_id=driver_id, is_active_deliveries=False
        )
        return encrypt_response(deliveries)
    except Exception as e:
        return encrypt_response({"error": str(e)})


@app.post("/driver_id_by_phone/{phone_number}")
async def api_driver_id_by_phone(phone_number: str):
    try:
        driver_id = manager.get_driver_id_by_phone(phone_number)
        if driver_id is None:
            return encrypt_response({"error": "Водитель с таким номером не найден"})

        return encrypt_response({"driver_id": driver_id})
    except Exception as e:
        return encrypt_response({"error": str(e)})


class MoveStageRequest(BaseModel):
    entity_name: str
    entity_id: int
    new_stage_id: str

@app.post("/move_stage")
async def api_move_stage(data: MoveStageRequest):
    try:
        success = manager.move_entity_to_stage(
            entity_name=data.entity_name,
            entity_id=data.entity_id,
            new_stage_id=data.new_stage_id
        )
        if success:
            return encrypt_response({"status": "success"})
        else:
            return encrypt_response({"error": "Ошибка при перемещении в новую стадию"})
    except Exception as e:
        return encrypt_response({"error": str(e)})