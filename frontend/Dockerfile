FROM node:16.13-alpine AS build

WORKDIR /app

COPY package.json yarn.lock /app/

RUN yarn install

COPY public /app/public
COPY src /app/src
COPY .env /app

RUN yarn build

FROM nginx:1.21-alpine

EXPOSE 80

COPY --from=build /app/build /usr/share/nginx/html
COPY nginx/nginx.conf /etc/nginx/conf.d/default.conf
COPY nginx/replace_envs.sh /docker-entrypoint.d