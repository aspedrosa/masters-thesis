FROM node:16.13-alpine AS build

WORKDIR /app

COPY package.json /app
COPY public /app/public
COPY src /app/src

RUN yarn install && yarn build \
 && rm -rf /app/public /app/src /app/node_modules \
 && yarn global add serve

EXPOSE 5000

CMD serve -s build
