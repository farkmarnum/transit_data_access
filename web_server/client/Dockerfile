ARG NODE_VERSION=12.7

# ---- Base Node ----
FROM node:${NODE_VERSION} AS base
# Create app directory
WORKDIR /app


# ---- Dependencies ----
FROM base AS dependencies 
# A wildcard is used to ensure both package.json AND package-lock.json are copied
COPY package*.json ./
# install app dependencies including 'devDependencies'
RUN npm install


# ---- Copy Files/Build ----
FROM dependencies AS dev
WORKDIR /app
# RUN DEV SERVER
CMD ["npm", "start"]


# ---- Copy Files/Build ----
FROM dependencies AS build
WORKDIR /app
COPY . /app
# Build react/vue/angular bundle static files
RUN npm run build

# --- Release with Alpine ----
FROM node:${NODE_VERSION}-alpine AS release  
# Create app directory
WORKDIR /app
RUN npm -g install serve

COPY --from=dependencies /app/package.json ./
# Install app dependencies
RUN npm install --only=production
COPY --from=build /app ./
CMD ["serve", "-s", "build", "-p", "3000"]
#	CMD ["node", "server.js"]




# ARG NODE_VERSION=12.7

# # ---- Base Node ----
# FROM node:${NODE_VERSION} AS base
# # Create app directory
# WORKDIR /app

# # ---- Dependencies ----
# FROM base AS dev 
# # A wildcard is used to ensure both package.json AND package-lock.json are copied
# COPY package*.json ./
# # install app dependencies including 'devDependencies'
# RUN npm install
# CMD npm start

# # ---- Copy Files/Build ----
# FROM base AS build
# WORKDIR /app
# # A wildcard is used to ensure both package.json AND package-lock.json are copied
# COPY package*.json ./
# # install app dependencies including 'devDependencies'
# RUN npm install
# COPY . /app
# # Build react/vue/angular bundle static files
# RUN npm run build

# # --- Release with Alpine ----
# FROM node:${NODE_VERSION}-alpine AS release  
# # Create app directory
# WORKDIR /app
# # optional
# RUN npm -g install serve
# COPY --from=build /app/package.json ./
# # Install app dependencies
# RUN npm install --only=production
# COPY --from=build /app ./
# #	CMD ["node", "server.js"]
# CMD ["serve", "-s", "dist", "-p", "8080"]
