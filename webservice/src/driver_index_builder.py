import os
from typing import Any, Dict
from webservice.src.bitrix_delivery_manager import BitrixDeliveryManager


class DriverIndexBuilder:
    def __init__(self, delivery_manager: BitrixDeliveryManager):
        self.delivery_manager = delivery_manager
        self.cache = delivery_manager.cache

    def build_driver_index(self) -> Dict[int, Dict[str, Any]]:
        structure = self.delivery_manager.build_nested_structure()
        driver_index: Dict[int, Dict[str, Any]] = {}

        for supply_id, supply_data in structure.items():
            for shipment_idx, shipment_entry in enumerate(supply_data['shipments']):
                delivery_block = shipment_entry.get('delivery')

                if not delivery_block:
                    continue

                driver = delivery_block.get('contact')

                if driver:
                    driver_id = int(driver['ID'])
                    driver_index.setdefault(driver_id, [])
                    driver_index[driver_id].append(
                        (supply_id, 'shipments', shipment_idx, 'delivery')
                    )

        return driver_index


def get_value_from_nested_dict(nested_dict: Dict, keys: tuple) -> Any:
    """
    Получает значение из вложенного словаря по заданным ключам.
    """
    value = nested_dict
    for key in keys:
        value = value[key]

    return value


def get_drivers_deliveries(bitrix_delivery_manager=None, driver_index_builder=None):
    if bitrix_delivery_manager is None:
        webhook_url = os.environ.get("BITRIX_WEBHOOK_URL")
        cache_file = os.environ.get("BITRIX_CACHE_FILE", "bitrix_cache.json")
        
        if not webhook_url:
            raise ValueError("Не задан BITRIX_WEBHOOK_URL в переменных окружения")
        
        bitrix_delivery_manager =  BitrixDeliveryManager(webhook_url=webhook_url, cache_file=cache_file, force_reload=False)
    nest_dict = bitrix_delivery_manager.build_nested_structure()
    if driver_index_builder is None:
        driver_index_builder = DriverIndexBuilder(bitrix_delivery_manager)
    driver_index = driver_index_builder.build_driver_index()

    driver_deliveries = {}
    for driver_id, deliveries in driver_index.items():
        driver_deliveries[driver_id] = []
        for delivery in deliveries:
            driver_deliveries[driver_id].append(
                get_value_from_nested_dict(nest_dict, delivery)
            )
    
    return driver_deliveries