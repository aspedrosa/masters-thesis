import requests

import globals

login_token = None


def _get_new_login_token():
    global login_token

    response = requests.post(
        globals.ADMIN_PORTAL_URL + "/token/",
        json={
            "username": globals.ADMIN_PORTAL_USER,
            "password": globals.ADMIN_PORTAL_PASSWORD,
        }
    )

    login_token = response.json()["access"]


def make_request(url):
    if login_token is None:
        _get_new_login_token()

    while True:
        response = requests.get(
            url,
            headers={
                "Authorization": "Bearer " + login_token
            }
        )
        if response.status_code == 401:
            _get_new_login_token()
        else:
            break

    return response.json()
