from src.helper import export_users
from dotenv import load_dotenv
from pandera import DataFrameSchema, Column, Check, Index


def test_user_schema():
    #1. Load dotenv
    print(load_dotenv(".env-admin", override=True))
    #2. Export users
    usersdf = export_users("6662bd6c0b1e5ad4aafa231f")
    #3. Check response
    schema = DataFrameSchema(
        {
             "username": Column(str),
            "firstName": Column(str),#, Check(lambda s: s < -1.2)),
            "lastName": Column(str),
            "orgAdmin": Column(bool),
            "maxWorkers": Column(int),
            "activated": Column(bool),
            "accessRoleIds": Column(nullable=True),
           
            #   []  Check(lambda s: s.str.startswith("value")),
            #     Check(lambda s: s.str.split("_", expand=True).shape[1] == 2)
            # ]),
        },
        index=Index(int),
        strict=True,
        coerce=True,
    )
    
    usersdf = usersdf[
            [
                "username",
                "firstName",
                "lastName",
                "orgAdmin",
                "maxWorkers",
                "activated",
                "accessRoleIds",
            ]
        ]#.to_csv(f"{DIR}/{USER_EXPORT_FILE}", index=False)
    schema.validate(usersdf)
