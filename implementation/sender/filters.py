import admin_portal
import globals

filters = {}


def start_filter(filter_id, selections):
    filters[filter_id] = selections


def stop_filter(filter_id):
    try:
        del filters[filter_id]
    except KeyError:
        pass


def edit_filter(filter_id, selections):
    filters[filter_id] = selections


def fetch_active_filters():
    global filters

    filters = admin_portal.make_request(globals.ADMIN_PORTAL_URL + f"/filters?status=ACTIVE")
    filters = {filter["id"]: filter["selections"] for filter in filters}
