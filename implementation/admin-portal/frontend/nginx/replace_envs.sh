#!/bin/sh

sed -i "s/ADMIN_PORTAL_BACKEND_HOST/$ADMIN_PORTAL_BACKEND_HOST/" /etc/nginx/conf.d/default.conf
sed -i "s/ADMIN_PORTAL_BACKEND_PORT/$ADMIN_PORTAL_BACKEND_PORT/" /etc/nginx/conf.d/default.conf