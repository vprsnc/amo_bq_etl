import os

from amocrm.v2 import tokens


def amo_auth():

    client_id = os.getenv("AMO_CLIENT_ID")
    client_secret = os.getenv("AMO_CLIENT_SECRET")
    subdomain = os.getenv("AMO_SUBDOMAIN")
    redirect_url = os.getenv("AMO_REDIRECT_URI")

    tokens.default_token_manager(
        client_id=client_id,
        client_secret=client_secret,
        subdomain=subdomain,
        redirect_url=redirect_url,
        storage=tokens.FileTokensStorage(directory_path="/home/analytics/OddJob/tokens/")
    )
