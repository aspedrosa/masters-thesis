import aiorwlock

import admin_portal
import globals


class Application:
    def __init__(self, id, community, request_template):
        self.id = id
        self.community = community
        self.request_template = request_template


applications = {}
applications_mtx = None

async def init_applications():
    global applications_mtx

    active_applications = admin_portal.make_request(globals.ADMIN_PORTAL_URL + f"/applications?status=ACTIVE")
    for application in active_applications:
        del application["status"]
        del application["name"]

        filter_id = application.pop("filter")

        applications[filter_id] = Application(
            **application
        )
    applications_mtx = aiorwlock.RWLock()


async def start_application(filter_id, application_id, community, request_template):
    async with applications_mtx.writer_lock:
        if filter_id not in applications:
            applications[filter_id] = {}

        applications[filter_id][application_id] = Application(application_id, community, request_template)


async def stop_application(filter_id, application_id, **kwargs):
    async with applications_mtx.writer_lock:
        del applications[filter_id][application_id]


async def edit_application(filter_id, application_id, community, request_template):
    async with applications_mtx.writer_lock:
        applications[filter_id][application_id] = Application(application_id, community, request_template)
