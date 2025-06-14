import requests
import logging
from datetime import datetime
from typing import Dict, Any, List
import json
import os

from collections import defaultdict

logging.basicConfig(level=logging.INFO)


class BitrixDeliveryManager:
    def __init__(self, webhook_url: str, cache_file: str, force_reload: bool = True):
        self.webhook_url = webhook_url.rstrip("/")
        self.cache_file = cache_file
        self.cache: Dict[str, Dict[int, Dict[str, Any]]] = {
            'delivery': {},
            'shipment': {},
            'purchase': {},
            'unloading': {},
            'loading': {},
            'deal': {},
            'supply': {},
            'contact': {}, 
            'nacladnaya': {},
            'doverennost': {},
            'marchrutniy_list': {}
        }
        self.entity_type_ids = {
            'shipment': 1040,
            'purchase': 1044,
            'delivery': 1048,
            'unloading': 1056,
            'loading': 1060,
            'nacladnaya': 1064,
            'doverennost': 1068
        }
        self.entity_type2parent_id = {
            'shipment': 'parentId2',
            'delivery': 'parentId1040',
            'purchase': 'parentId1040',
            'loading': 'parentId1048',
            'unloading': 'parentId1048',
            'nacladnaya': 'parentId1048',
            'doverennost': 'parentId1048'
        }

        if os.path.exists(self.cache_file) and not force_reload:
            self._load_cache_from_file()
            print("Кэш загружен из файла.")
        else:
            self.load_supplies()
            self._save_cache_to_file()
            print("Кэш собран и сохранён.")

    def _paginate_list(self, method: str, params: Dict[str, Any], limit: int = 50) -> List[Dict[str, Any]]:
        all_items = []
        start = 0
        while True:
            try:
                params_with_start = params.copy()
                params_with_start['start'] = start
                params_with_start['limit'] = limit
                res = requests.post(f"{self.webhook_url}/{method}", json=params_with_start).json()
                result = res.get("result", [])
                if 'items' in result:
                    result = result['items']
                if 'documents' in result:
                    result = result['documents']
                if 'productRows' in result:
                    result = result['productRows']

                all_items.extend(result)
                if "next" not in res:
                    break
                start = res["next"]
            except Exception as e:
                logging.error(f"Ошибка при выполнении {method}: {e}")
                break
        return all_items
    
    def download_urls(self, document_name, limit: int = 50):
       ent_ids_list = [val['id'] for val in self.cache[document_name].values() if 'id' in val]
       print(document_name, len(self.cache[document_name]), len(ent_ids_list), ent_ids_list[:10])
       documents_list = self._paginate_list('/crm.documentgenerator.document.list', 
              params={
                  'entityTypeId': self.entity_type_ids[document_name], 
                  'filter': {
                      'id': ent_ids_list
                  },
                  'order': {'id': 'asc'}},
                limit=limit
        )
       print(len(documents_list))
       for item in documents_list:
           self.cache[document_name][int(item['id'])]['downloadUrl'] = item['pdfUrl']
        
    def get_delivery_full_info_by_id(self, delivery_id: int) -> Dict[str, Any]:
        """
        Возвращает полную информацию по доставке:
        - информация о самой доставке
        - водитель (контакт)
        - загрузка, разгрузка
        - накладная, доверенность, маршрутный лист
        - товары
        - родительская отгрузка, закупка и сделка
        """
        delivery = self.cache['delivery'].get(delivery_id)
        if not delivery:
            raise ValueError(f"Доставка с id={delivery_id} не найдена в кэше.")

        # Водитель
        driver_id = delivery.get('ufCrm6_1729602194')
        driver = self.cache['contact'].get(int(driver_id)) if driver_id else None

        # Загрузка и разгрузка
        loading = next((l for l in self.cache['loading'].values()
                        if int(l.get('parentId1048', 0)) == delivery_id), None)

        unloading = next((u for u in self.cache['unloading'].values()
                        if int(u.get('parentId1048', 0)) == delivery_id), None)

        # Документы
        nacladnaya = next((n for n in self.cache['nacladnaya'].values()
                        if int(n.get('parentId1048', 0)) == delivery_id), None)

        doverennost = next((d for d in self.cache['doverennost'].values()
                            if int(d.get('parentId1048', 0)) == delivery_id), None)

        marchrutniy_list = self.cache['marchrutniy_list'].get(delivery_id)

        # Родительская отгрузка
        shipment_id = int(delivery.get('parentId1040', 0))
        shipment = self.cache['shipment'].get(shipment_id)

        # Родительская поставка
        supply = None
        if shipment:
            supply_id = int(shipment.get('parentId2', 0))
            supply = self.cache['supply'].get(supply_id)
        else:
            supply_id = None

        # Связанная сделка
        deal = None
        if supply:
            deal_id = supply.get('UF_CRM_1728985624')
            if deal_id:
                deal = self.cache['deal'].get(deal_id)

        # Связанные закупки
        purchases = [
            p for p in self.cache['purchase'].values()
            if int(p.get('parentId1040', 0)) == shipment_id
        ]

        return {
            "delivery": delivery,
            "driver": driver,
            "loading": loading,
            "unloading": unloading,
            "nacladnaya": nacladnaya,
            "doverennost": doverennost,
            "marchrutniy_list": marchrutniy_list,
            "shipment": shipment,
            "supply": supply,
            "deal": deal,
            "purchases": purchases,
            "product_rows": delivery.get("product_rows", [])
        }

    def load_supplies(self, limit: int = 50):
        print("Загружаем поставки (сделки с названием, начинающимся с 'Поставка')...")
        items = self._paginate_list("crm.deal.list.json", {
            "filter": {"title": "%Поставка%"},
            'order': {'ID': 'ASC'},
            "select": ["*", "UF_*"],
            "limit": limit
        })
        supplies = [item for item in items if item.get("TITLE", "").startswith("Поставка")]
        self.cache['supply'] = {int(item['ID']): item for item in supplies}
        print(f"Загружено поставок: {len(supplies)}")
        self._load_deals_from_supplies(limit=limit)

        self._fetch_specific_entities('shipment', list(self.cache['supply'].keys()), "parentId2", limit=limit)
        self._fetch_specific_entities('delivery', list(self.cache['shipment'].keys()), "parentId1040", limit=limit)
        self.cache['marchrutniy_list'] = {
            key: {
                'downloadUrl': (
                    (val or {}).get('ufCrm6_1729602373', {}) or {}).get('url', None)
            }
            for key, val in self.cache['delivery'].items()
        }
        self._fetch_specific_entities('purchase', list(self.cache['shipment'].keys()), "parentId1040", limit=limit)

        self._load_driver_contacts_from_deliveries(limit=limit)

        self._fetch_specific_entities('loading', list(self.cache['delivery'].keys()), "parentId1048", limit=limit)
        self._fetch_specific_entities('unloading', list(self.cache['delivery'].keys()), "parentId1048", limit=limit)
        self._fetch_specific_entities('nacladnaya', list(self.cache['delivery'].keys()), 'parentId1048', limit=limit)
        self.download_urls('nacladnaya')
        self._fetch_specific_entities('doverennost', list(self.cache['delivery'].keys()), "parentId1048", limit=limit)
        self.download_urls('doverennost')

        self._get_products_for_deliveries(list(self.cache['delivery'].keys()))

    def _load_deals_from_supplies(self, limit: int = 50):
        deal_ids = [
            val['UF_CRM_1728985624'] 
            for _, val in self.cache['supply'].items()
        ]
        params = {
            'filter': {
                'ID': deal_ids
            },
            'order': {'ID': 'ASC'},
            "select": ["*", "UF_*"]
        }
        self.cache['deal'] = {
            item["ID"]: item for item in self._paginate_list("crm.deal.list.json", params, limit=limit)
        }
    
    def _get_products_for_deliveries(self, ids, limit: int = 50):
        product_rows = self._paginate_list(
            '/crm.item.productrow.list', 
            {
                "filter" : {
                    "=ownerType" : f"T{hex(self.entity_type_ids['delivery'])[2:]}", ## 1048
                    "=ownerId" : ids
                }
            }, limit=limit)

        grouped = defaultdict(list)
        for item in product_rows:
            key = item['ownerId']
            grouped[key].append(item)
        grouped = dict(grouped)
        self.cache['delivery'] = {
            del_id: {
                **delivery,
                'product_rows': [
                    {
                        'product_name': item['productName'],
                        'quantity': item['quantity'],
                        'unit': item['measureName']
                    }
                    for item in grouped[delivery['id']]
                ]
            }
            for del_id, delivery in self.cache['delivery'].items()
        }

    def _load_driver_contacts_from_deliveries(self, limit: int = 50):
        # Сбор всех уникальных ID контактов водителей из поля ufCrm6_1729602194 в deliveries
        driver_contact_ids = set()
        for delivery in self.cache['delivery'].values():
            driver_id = delivery.get('ufCrm6_1729602194')
            if driver_id:
                try:
                    driver_contact_ids.add(int(driver_id))
                except ValueError:
                    print(f"Некорректный ID водителя: {driver_id}")

        if not driver_contact_ids:
            print("Контакты водителей не найдены в deliveries.")
            return

        print(f"Загружаем данные контактов водителей: {len(driver_contact_ids)} шт.")

        # Запрашиваем контакты пачками по 50 (лимит API)
        for chunk in self._chunked(list(driver_contact_ids), 50):
            params = {
                "filter": {"ID": chunk},
                "select": ["*", "PHONE", "EMAIL"]
            }
            try:
                contacts = self._paginate_list("crm.contact.list.json", params, limit=limit)
                for c in contacts:
                    cur_cont = c.copy()
                    if "PHONE" in cur_cont:
                        cur_cont['PHONE'] = cur_cont['PHONE'][0]['VALUE'].replace('+', '')
                    else:
                        cur_cont['PHONE'] = ""
                    self.cache['contact'][int(c['ID'])] = cur_cont
                print(f"Загружено контактов водителей: {len(contacts)}")
            except Exception as e:
                print(f"Ошибка при загрузке контактов водителей: {e}")

    def _fetch_specific_entities(self, name: str, ids: set, filter_key: str, limit: int = 50):
        entity_type_id = self.entity_type_ids[name]
        items = []
        for chunk in self._chunked(list(ids), limit):
            try:
                params = {
                    "entityTypeId": entity_type_id,
                    "filter": {filter_key: chunk} if filter_key else {},
                    'order': {'ID': 'ASC'},
                    "select": ["*", "UF_*"]
                }
                #res = requests.post(f"{self.webhook_url}/crm.item.list.json", json=params).json()
                part_items = self._paginate_list("crm.item.list.json", params, limit=limit)
                items.extend(part_items['items'] if 'items' in part_items else part_items)
                print(f"Загружено {name} {len(items)} из {len(ids)}")
            except Exception as e:
                print(f"Ошибка при загрузке {name}: {e}")
        self.cache[name].update({int(item['id']): item for item in items})

    def _chunked(self, iterable, n):
        for i in range(0, len(iterable), n):
            yield iterable[i:i+n]
    
    def build_nested_structure(self) -> Dict[int, Dict[str, Any]]:
        structure = {}

        for supply_id, supply in self.cache['supply'].items():
            structure[supply_id] = {
                'supply': supply,
                'deal': self.cache['deal'].get(supply.get('UF_CRM_1728985624')),
                'shipments': []
            }

            for shipment in self.cache['shipment'].values():
                if int(shipment.get('parentId2', 0)) != supply_id:
                    continue

                shipment_id = int(shipment['id'])

                delivery = next(
                    (d for d in self.cache['delivery'].values() if int(d.get('parentId1040', 0)) == shipment_id),
                    None
                )

                delivery_id = int(delivery['id']) if delivery else None

                delivery_data = None
                if delivery:
                    delivery_data = {
                        'delivery': delivery,
                        'loading': next(
                            (l for l in self.cache['loading'].values()
                            if int(l.get('parentId1048', 0)) == delivery_id), None),
                        'unloading': next(
                            (u for u in self.cache['unloading'].values()
                            if int(u.get('parentId1048', 0)) == delivery_id), None),
                        'nacladnaya': next(
                            (n for n in self.cache['nacladnaya'].values()
                            if int(n.get('parentId1048', 0)) == delivery_id), None),
                        'doverennost': next(
                            (d for d in self.cache['doverennost'].values()
                            if int(d.get('parentId1048', 0)) == delivery_id), None),
                        'marchrutniy_list': self.cache['marchrutniy_list'].get(delivery_id),
                        'contact': self.cache['contact'].get(
                            int(delivery.get('ufCrm6_1729602194') or 0), None),
                        'product_rows': delivery.get('product_rows', [])
                    }

                purchases = [
                    p for p in self.cache['purchase'].values()
                    if int(p.get('parentId1040', 0)) == shipment_id
                ]

                structure[supply_id]['shipments'].append({
                    'shipment': shipment,
                    'delivery_block': delivery_data,
                    'purchases': purchases
                })

        return structure
    
    def get_deliveries_grouped_by_driver(self, search_driver_id = None, is_active_deliveries=True) -> Dict[int, Dict[str, Any]]:
        """
        Возвращает deliveries, сгруппированные по водителям.
        Структура: {driver_id: {"contact": contact_info, "deliveries": [delivery_info, ...]}}
        """
        grouped = defaultdict(lambda: {"contact": None, "deliveries": []})
        
        nested = self.build_nested_structure()
        for supply in nested.values():
            for shipment_entry in supply["shipments"]:
                delivery_block = shipment_entry.get("delivery_block")
                if not delivery_block:
                    continue

                delivery = delivery_block.get("delivery")
                contact = delivery_block.get("contact")
                if not delivery or not contact:
                    continue
                if is_active_deliveries and ('SUCCESS' in delivery['stageId'] or 'FAIL' in delivery['stageId']):
                    continue

                driver_id = int(contact["ID"])
                grouped[driver_id]["contact"] = contact
                grouped[driver_id]["deliveries"].append(delivery_block)

        grouped = dict(grouped)
        if search_driver_id is not None:
            return grouped.get(search_driver_id, {})
        else:
            return grouped
    
    def build_nested_structure_old(self) -> Dict[int, Dict[str, Any]]:
        structure = {}

        for supply_id, supply in self.cache['supply'].items():
            structure[supply_id] = {
                'supply': supply,
                'deal': self.cache['deal'].get(supply.get('UF_CRM_1728985624')),
                'shipments': []
            }

            for shipment in self.cache['shipment'].values():
                if int(shipment.get('parentId2', 0)) != supply_id:
                    continue

                shipment_id = int(shipment['id'])

                # Найдём единственную delivery
                delivery = next(
                    (d for d in self.cache['delivery'].values() if int(d.get('parentId1040', 0)) == shipment_id),
                    None
                )

                # Найдём loading/unloading, если delivery существует
                loading = None
                unloading = None
                contact = None
                if delivery:
                    delivery_id = int(delivery['id'])
                    loading = next(
                        (l for l in self.cache['loading'].values() if int(l.get('parentId1048', 0)) == delivery_id),
                        None
                    )
                    unloading = next(
                        (u for u in self.cache['unloading'].values() if int(u.get('parentId1048', 0)) == delivery_id),
                        None
                    )

                    contact = next(
                        (u for u in self.cache['contact'].values() if int(u.get('ID', 0)) == int(delivery.get('ufCrm6_1729602194', 0) or 0)),
                        None
                    )

                # Найдём все закупки
                purchases = [
                    purchase for purchase in self.cache['purchase'].values()
                    if int(purchase.get('parentId1040', 0)) == shipment_id
                ]

                shipment_entry = {
                    'shipment': shipment,
                    'delivery': {
                        'delivery': delivery,
                        'loading': loading,
                        'unloading': unloading,
                        'contact': contact if contact else None
                    } if delivery else None,
                    'purchases': purchases
                }

                structure[supply_id]['shipments'].append(shipment_entry)

        return structure

    def refresh_updates(self, since: datetime):
        iso_time = since.isoformat()
        self.update_deliveries()
        print(f"Обновление всех сущностей с {iso_time}...")
        for name in self.entity_type_ids.keys():
            entity_type_id = self.entity_type_ids[name]
            try:
                print(f"Обновляем {name}...")
                updated_items = self._paginate_list("crm.item.list.json", {
                    "entityTypeId": entity_type_id,
                    "filter": {">=DATE_MODIFY": iso_time},
                })
                for item in updated_items:
                    self.cache[name][int(item['id'])] = item
            except Exception as e:
                print(f"Ошибка при обновлении {name}: {e}")
        self._save_cache_to_file()
    
    def update_deliveries(self):
        updated_items = self._paginate_list("crm.item.list.json", {
            "entityTypeId": self.entity_type_ids['delivery']
        })
        if len(updated_items) > 0:
            for item in updated_items:
                delivery_id = int(item['id'])
                old_delivery = self.cache['delivery'].get(delivery_id)

                driver_id = item.get('ufCrm6_1729602194')
                if not driver_id or 'DT1048_9:1' != item['stageId']:
                    continue

                grouped = self.get_deliveries_grouped_by_driver(int(driver_id))
                already_has = any(
                    d['delivery']['id'] == delivery_id for d in grouped.get('deliveries', [])
                )

                if not already_has:
                    try:
                        print(item)
                        response = requests.post(
                            'https://n8n.glavsnabstroymsk.ru/webhook/send_information_about_new_deliveries',
                            json={"delivery_id": delivery_id, "driver_id": driver_id}
                        )
                        logging.info(f"Доставка {delivery_id} {self.entity_type_ids['delivery']} {driver_id} отправлена в n8n {response.text}")
                    except Exception as e:
                        logging.error(f"Ошибка при POST в n8n: {e}")
    
    def _save_cache_to_file(self):
        try:
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(self.cache, f, ensure_ascii=False, indent=4)
            print(f"Кэш сохранён в {self.cache_file}")
        except Exception as e:
            print(f"Ошибка при сохранении кэша: {e}")

    def _load_cache_from_file(self):
        try:
            with open(self.cache_file, "r", encoding="utf-8") as f:
                loaded = json.load(f)
                self.cache = {
                    k: {int(inner_k): inner_v for inner_k, inner_v in v.items()} for k, v in loaded.items()
                }
        except Exception as e:
            print(f"Ошибка при загрузке кэша: {e}")
    
    def get_driver_id_by_phone(self, phone_number: str) -> int | None:
        """
        Поиск driver_id (Bitrix Contact ID) по номеру телефона.
        Номер приводится к виду, с которым сравниваются контакты.
        """
        def normalize(p: str) -> str:
            # удалим всё кроме цифр, например: +7 (999) 123-45-67 → 79991234567
            return ''.join(filter(str.isdigit, p))

        target = normalize(phone_number)

        for driver_id, driver in self.cache['contact'].items():
            if normalize(driver.get("PHONE", "")) == target:
                return driver_id

        return None