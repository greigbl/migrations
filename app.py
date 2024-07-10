
from logging import Logger
from src.helper import export_users, export_projects, import_users, import_projects, upload_dataset
import os

MIGRATION_THREADS = 4
DIR = "data"
USER_EXPORT_FILE = "user-export.csv"


if __name__ == "__main__":
    from dotenv import load_dotenv
    
    ## Project/Dataset
    load_dotenv(".env-user", override=True)
    catalog_item = {'catalogId': '667a4ff91cf349d67095e23c', 'name': 'MarketingTrain (Men).csv', 'complete': True}
    # upload_dataset(catalog_item)
    export_projects()
    #import_projects()

    ## Users
    #print(load_dotenv(".env-admin", override=True))
    #users = export_users(os.environ.get("SOURCE_ORG_ID"))
    #users[["username","firstName","lastName","orgAdmin","maxWorkers","accessRoleIds"]].to_csv(f"{DIR}/{USER_EXPORT_FILE}", index=False)



    #import_users(os.environ.get("TARGET_ORG_ID"))


