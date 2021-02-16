# ---- Base Node ----
FROM node:15 AS base
# Create app directory
WORKDIR /app


# --- Dev ----
FROM node:15 AS dev  
# Create app directory
WORKDIR /app
# Install ALL app dependencies
COPY package*.json ./
RUN npm install
COPY . .
CMD ["node", "--inspect=0.0.0.0", "server.js"]


# ---- Dependencies ----
FROM base AS build
# A wildcard is used to ensure both package.json AND package-lock.json are copied
COPY package*.json ./
RUN npm install --only=production


# --- Release with Alpine ----
FROM node:15-alpine AS release  
# Create app directory
WORKDIR /app
COPY --from=build /app /app
COPY ./server.js ./server.js

CMD ["node", "server.js"]
