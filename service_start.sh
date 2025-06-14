export BITRIX_WEBHOOK_URL="https://crm.glavsnabstroymsk.ru/rest/1/mwbgz3l2arc53wa5/"
export BITRIX_CACHE_FILE="webservice/src/bitrix_cache.json"

#python -m venv tmp_venv
#source tmp_venv/bin/activate
#pip install --upgrade pip
#pip install -r requirements.txt
uvicorn main:app --port 9030 --reload
