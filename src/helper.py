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
        "headers" in kwargs
        and "Content-Type" in kwargs["headers"]
        and kwargs["headers"]["Content-Type"] == "multipart/form-data"
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


def _export(path: str, method: str = "GET", admin: bool = False, **kwargs) -> Response:
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
        headers={"Content-Type": "multipart/form-data"},
        files=[
            (
                "file",
                (
                    f"{catalog_item['name']}.csv",
                    open(f"{DIR}/{catalog_item['name']}"),
                    "text/csv",
                ),
            )
        ],
        payload={"categories": "TRAINING"},
    )


def dataset_metadata(catalog):
    all_recs = []
    for c in catalog:
        if "catalogId" in c:
            deets = _export(f"datasets/{c['catalogId']}/versions/?orderBy=created")
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


def import_projects(DIR: str, USER_EXPORT_FILE: str):
    # 1. Import datasets
    files = glob("data/*.csv")
    print("Found ", len(files), " files")
    file_dict = {}
    for f in files[0:2]:
        print("Importing projects")
        if f == USER_EXPORT_FILE:
            continue
        file = "".join(f.split(".")[:-1]).split(f"{DIR}/")[1] + ".csv"
        # 1. Check dataset does NOT exist (cheap idempotency)
        # TODO: The partial matching will cause problems
        files = _call_http(
            f"{os.environ.get('DATAROBOT_ENDPOINT')}/catalogItems/?limit=1&searchFor={file}",
            os.environ.get("DATAROBOT_API_TOKEN"),
            "POST",
        )

        if len(files) > 0:
            print("Dataset already exits.. skipping")
            print("Data: ", files)
            file_dict[file] = {k: v for (k, v) in files["data"] if f == f"{file}.csv"}
            continue
        # 2. Create dataset
        file_dict[file] = dr.Dataset.create_from_file(f)
    print("Files: ", file_dict)
    # 2. Process of the options
    herefornow = {
        "target": "販売数量",
        "target_type": "Regression",
    }
    adv_options = {}
    partition_options = {}
    # 3. Create project
    # project = dr.Project.create_from_dataset(dataset_id="667e2bdfe1201743794d30ad")
    # #4. Update the project
    # project.set_options(**adv_options)
    # project.set_partitioning_method(**partition_options)
    # # project.set_target(**options)
    # #5. Run modelling
    # project.analyze_and_model(**herefornow)

    options = {
        "positive_class": None,
        # "max_train_pct": 74.178465,
        "max_train_rows": 252706,
        # "holdout_unlocked": False,
        # "project_name": "商品の売上_学習",
        "unsupervised_mode": False,
        "use_feature_discovery": False,
        "segmentation": None,
        # ???
        # "catalog_id": "667e268f9cdebc97bd581ba3",
        "external_time_series_baseline_dataset_metadata": None,
        "is_scoring_available_for_models_trained_into_validation_holdout": False,
        "unsupervised_type": None,
        "query_generator_id": None,
        "use_gpu": False,
    }

    # partition_options = {
    #     "cv_method": "random",
    #     "validation_type": "CV",
    #     "holdout_pct": 19.999824,
    #     "reps": 5,
    #     "validation_level": None,
    #     "cv_holdout_level": None,
    #     "user_partition_col": None,
    #     "validation_pct": None,
    #     "training_level": None,
    #     "partition_key_cols": None,
    #     "holdout_level": None,
    #     # "datetime_col": None,
    #     # "use_time_series": None,
    # }

    # adv_options = {
    #     "blueprint_threshold": 3,
    #     "shap_only_mode": False,
    #     "response_cap": False,
    #     "blend_best_models": False,
    #     "smart_downsampled": False,
    #     "weights": None,
    #     "seed": None,
    #     "majority_downsampling_rate": None,
    #     "run_leakage_removed_feature_list": True,
    #     "prepare_model_for_deployment": True,
    #     "only_include_monotonic_blueprints": False,
    #     "consider_blenders_in_recommendation": False,
    # }

    print("Project: ", f"https://staging.datarobot.com/projects/{project.id}")
    # 6. Await autopilot to finish
    # project.open_in_browser()
    project.wait_for_autopilot()
