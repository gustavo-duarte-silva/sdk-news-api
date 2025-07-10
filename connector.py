import requests as rq
import traceback
import datetime
import json

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log 
from fivetran_connector_sdk import Operations as op

def schema(configuration: dict):
	top_headlines_table = {
		"table": "top_headlines",
		"primary_key": ["url"],
		"columns": {
			"source_id": "STRING",
			"source_name": "STRING",
			"published_at": "UTC_DATETIME",
			"author": "STRING",
			"title": "STRING",
			"description": "STRING",
			"url": "STRING",
			"content": "STRING"
		}
	}

	sources_table = {
		"table": "sources",
		"primary_key": ["id"],
		"columns": {
			"id": "STRING",
			"name": "STRING",
			"description": "STRING",
			"url": "STRING",
			"category": "STRING",
			"language": "STRING",
			"country": "STRING"
		}
	}

	tables = [
		top_headlines_table,
		sources_table
	]

	return tables


def update(configuration: dict, state: dict):

	top_headlines_url = "https://newsapi.org/v2/top-headlines"
	sources_url = "https://newsapi.org/v2/top-headlines/sources"
	
	try:
		if 'to_ts' in state:
			from_ts = state['to_ts']
		else:
			from_ts = datetime.datetime.now() - datetime.timedelta(days=7)
			from_ts = from_ts.strftime("%Y-%m-%dT%H:%M:%S")

		now = datetime.datetime.now()
		to_ts = now.strftime("%Y-%m-%dT%H:%M:%S")
		headers = {
			"Authorization": "Bearer {}".format(
				configuration["API_KEY"]
			),
			"accept": "application/json"
		}

		top_headlines_params = {
			"from": from_ts,
			"to": to_ts,
			"page": 1,
			"language": configuration['language'],
			"sortBy": "publishedAt",
			"pageSize": configuration['pageSize']
		}


		yield from sync_top_headlines(
			top_headlines_url,
			headers,
			top_headlines_params,
			state
		)

		sources_params = {
			"language": configuration['language'],
		}

		yield from sync_sources(
			sources_url,
			headers,
			sources_params
		)

		new_state = {
			"to_ts": to_ts
		}

		log.fine(
			f"state updated, new state: {repr(new_state)}"
		)

		yield op.checkpoint(
			state=new_state
		)

	except Exception as e:
		exception_message = str(e)
		stack_trace = traceback.format_exc()
		detailed_message = f"Error Message: {exception_message}\nStack Trace:\n{stack_trace}"
		raise RuntimeError(detailed_message)


### helper methods ###
def sync_top_headlines(base_url, headers, params, state):

	has_more_pages = True

	while has_more_pages:
		response_page = get_api_response(
			base_url,
			headers,
			params
		)

		log.info(str(response_page["totalResults"]) + " results")

		items = response_page.get("articles", [])

		if not items:
			break

		for item in items:

			yield op.upsert(
				table="top_headlines",
				data={
					"source_id": item["source"]["id"],
					"source_name": item["source"]["name"],
					"published_at": item["publishedAt"],
	                "author": item["author"],
	                "title": item["title"],
	                "description": item["description"],
	                "url": item["url"],
	                "content": item["content"]
				}
			)
		yield op.checkpoint(
			state
		)

		has_more_pages, params = pagination(
			params, response_page
		)

def sync_sources(base_url, headers, params):

	response_page = get_api_response(
		base_url,
		headers,
		params
	)

	items = response_page.get("sources", [])

	for item in items:
		yield op.upsert(
			table="sources",
			data={
				"id": item["id"],
				"name": item["name"],
                "description": item["description"],
                "url": item["url"],
                "category": item["category"],
                "language": item["language"],
                "country": item["country"]
			}
		)



def get_api_response(endpoint_path, headers, params):

	response = rq.get(
		endpoint_path,
		headers=headers,
		params=params
	)

	response.raise_for_status()
	response_page = response.json()

	return response_page


def pagination(params, response_page):

	has_more_pages = True
	
	current_page = int(
		params["page"]
	)

	total_pages = divmod(
		int(
			response_page["totalResults"]
		),
		int(
			params["pageSize"]
		)

	)[0] + 1

	increment_page_number = current_page and total_pages and current_page < total_pages and current_page * int(params["pageSize"]) < 100

	if increment_page_number:
		params["page"] = current_page + 1
	else:
		has_more_pages = False

	return has_more_pages, params

connector = Connector(
	update=update,
	schema=schema
)

