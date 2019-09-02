# ---- Base Node ----
FROM node:12.7 AS base
# Create app directory
WORKDIR /app


# --- Dev ----
FROM node:12.7 AS dev  
# Create app directory
WORKDIR /app
# Install ALL app dependencies
COPY package*.json ./
RUN npm install
COPY . .
CMD ["node", "--inspect=0.0.0.0", "server.js"]


# --- Release with Alpine ----
FROM node:12.7-alpine AS release  
# Create app directory
WORKDIR /app
COPY . .
RUN npm install --production \
 && npm cache clean --force \
 && rm -rf /tmp/*
CMD ["node", "server.js"]
