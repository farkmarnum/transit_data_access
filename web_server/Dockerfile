ARG NODE_VERSION=12.7

# ---- Base Node ----
FROM node:${NODE_VERSION} AS base
# Create app directory
WORKDIR /app


# # ---- Dependencies ----
# FROM base AS dependencies 
# # A wildcard is used to ensure both package.json AND package-lock.json are copied
# COPY package*.json ./
# # install app dependencies for production
# RUN npm install --production


# # ---- Copy Files/Build ----
# FROM dependencies AS build
# WORKDIR /app
# COPY . /app


# --- Dev ----
FROM node:${NODE_VERSION} AS dev  
# Create app directory
WORKDIR /app
# Install ALL app dependencies
COPY package*.json ./
RUN npm install
COPY . .
CMD ["node", "--inspect=0.0.0.0", "server.js"]


# --- Release with Alpine ----
FROM node:${NODE_VERSION}-alpine AS release  
# Create app directory
WORKDIR /app
COPY . .
RUN npm install --production \
 && npm cache clean --force \
 && rm -rf /tmp/*
CMD ["node", "server.js"]
