import requests
from requests import Response
import os
import pandas as pd
import datarobot as dr
from glob import glob
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any


def _call_http(url: str, apikey: str, method: str = "GET", **kwargs) -> Response:
    if "headers" in kwargs:
        headers = kwargs["headers"] | {"Authorization": f"Bearer {apikey}"}
    else:
        headers = {"Authorization": f"Bearer {apikey}"}
    if "data" in kwargs:
        headers = headers | {"Content-Type": "application/json"}
        response = requests.request(
            method=method, url=url, headers=headers, json=kwargs["data"]
        )
    elif (
        "files" in kwargs
    ):
        response = requests.request(
            "POST", url, headers=headers, data=kwargs["payload"], files=kwargs["files"]
        )
    else:
        response = requests.request(method=method, url=url, headers=headers)
    if response.headers["Content-Type"] == "application/json":
        return response.json()


def _current_user():
    return _export("account/info/")


def _export(path: str, env: str = "SOURCE", method: str = "GET", **kwargs) -> Response:
    if env == "TARGET":
        url = f"{os.environ.get('DATAROBOT_ENDPOINT')}/{path}"
        return _call_http(url, os.environ.get("DATAROBOT_API_TOKEN"), **kwargs)
    else:
        url = f"{os.environ.get('SOURCE_ENDPOINT')}/{path}"
        return _call_http(url, os.environ.get("SOURCE_API_TOKEN"), **kwargs)


def _import(path: str, method: str = "GET", admin: bool = False, **kwargs) -> Response:
    url = f"{os.environ.get('DATAROBOT_ENDPOINT')}/{path}"
    return _call_http(
        url, os.environ.get("DATAROBOT_API_TOKEN"), method=method, **kwargs
    )


def _convert_to_snake(dict_in):
    def replace(item):
        return re.sub(r"(?<!^)(?=[A-Z])", "_", item).lower()

    return {replace(k): v for (k, v) in dict_in.items()}


def _download_dataset(catalog_id: str, fileName: str, DIR: str) -> dict[str, Any]:
    url = f"{os.environ.get('SOURCE_ENDPOINT')}/datasets/{catalog_id}/file/"
    headers = {"Authorization": f"Bearer {os.environ.get('SOURCE_API_TOKEN')}"}
    complete = False
    with requests.get(url, headers=headers, stream=True) as response:
        if response.headers["Content-Type"] == "text/csv; charset=utf-8":
            with open(f"{DIR}/{fileName}", "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                complete = True
    return {"catalogId": catalog_id, "name": f"{fileName}", "complete": complete}


def download_datasets(
    catalog_items: pd.DataFrame, DIR: str, MIGRATION_THREADS: int
) -> list[str]:
    with ThreadPoolExecutor(max_workers=MIGRATION_THREADS) as executor:
        futurejobs = []
        for item in catalog_items.iterrows():
            item = item[1].to_dict()
            futurejobs.append(
                executor.submit(_download_dataset, item["catalogId"], item["name"], DIR)
            )
        complete = [j.result() for j in as_completed(futurejobs)]
        result = (
            "All downloads completed"
            if all(r["complete"] for r in complete)
            else "Some downloads failed"
        )
        print(result)
        return complete


def upload_dataset(catalog_item, DIR: str):
    return _import(
        "datasets/fromFile/",
        "POST",
        # headers={"Content-Type": "multipart/form-data"},
        files=[
            (
                "file",
                (
                    f"{catalog_item['name']}",
                    open(f"{DIR}/{catalog_item['name']}"),
                    "text/csv",
                ),
            )
        ],
        #TODO: Set this dynamically
        payload={"categories": "TRAINING"},
    )


def dataset_metadata(catalog):
    all_recs = []
    for c in catalog:
        if "catalogId" in c:
            deets = _export(f"datasets/{c['catalogId']}/versions/?orderBy=created",env="TARGET")
            data = deets["data"][0]
            all_recs.append(
                {
                    "catalogId": c["catalogId"],
                    "name": data["name"],
                    "datasetSize(MB)": int(data["datasetSize"] / 1_000_000),
                    "rowCount": data["rowCount"],
                    "categories": data["categories"],
                }
            )
    return pd.DataFrame(all_recs)


def export_projects() -> list[dict[str, Any]]:
    # 1. Export projects and spit to json
    OFFSET = 0
    LIMIT = 20
    url = f"projects/?orderBy=-projectName&offset={OFFSET}&limit={LIMIT}"
    allprojects = []
    while url:
        projects = _export(url)
        if len(projects) == 0:
            url = None
        else:
            allprojects.extend(projects)
            OFFSET += LIMIT
            url = f"projects/?orderBy=-projectName&offset={OFFSET}&limit={LIMIT}"
    return allprojects


def export_users() -> pd.DataFrame:
    orgid = os.environ.get("SOURCE_ORG_ID")
    url = f"organizations/{orgid}/users/?offset=0&limit=10"
    current_user = _current_user()["email"]
    allusers = []
    while url:
        users = _export(url, admin=True)
        if "message" in users:
            print("Message: ", users["message"])
        else:
            user_list = [
                user
                for user in users["data"]
                if user["activated"] and user["username"] != current_user
            ]
            allusers.extend(user_list)
        url = users.get("next", None)
        if url is not None:
            url = f"organizations/{orgid}/users/?offset{url.split('?offset')[1]}"

    return pd.DataFrame.from_records(allusers)


def _initiate_project(catalog_id): ...


def migrate_item(catalog_id, filename):
    _download_dataset(catalog_id, filename)
    new_catalogid = upload_dataset(filename)
    new_project = _initiate_project(new_catalogid)


def import_users(DIR: str, USER_EXPORT_FILE: str, PASSWORD: str) -> dict[str, Any]:
    orgid = os.environ.get("TARGET_ORG_ID")
    # Read user list => iterate
    # with open(f"{DIR}/user-export.json") as f:
    #     users = json.loads(f.read())
    imported_users = []
    users = pd.read_csv(f"{DIR}/{USER_EXPORT_FILE}")
    for user in users.iterrows():
        user = user[1]
        # 1. Create user
        # TODO: accessRoleIds,
        # NOTE: language not possible
        # NOTE: avoid static password not possible with the API
        print("User: ", user["username"])
        payload = dict(user[["username", "firstName", "lastName", "orgAdmin"]])
        new_user = payload
        # Need to append a default password as well.
        payload = payload | {"create": True, "password": PASSWORD}
        try:
            # 1. Create user
            user_created = _import(
                f"organizations/{orgid}/users/", "POST", admin=True, data=payload
            )
            new_user = new_user | user_created
            print("User created: ", user_created)
            # 2. Patch user (maxWorkers)
            user_updated = _import(
                f"organizations/{orgid}/users/{user_created['userId']}",
                "PATCH",
                admin=True,
                data={"maxWorkers": user["maxWorkers"]},
            )
            new_user = new_user | {"workersSet": True}
        except Exception as e:
            print("User creation did not complete fully", user["username"])
        imported_users.append(new_user)
    return imported_users



