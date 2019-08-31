ARG NODE_VERSION=12.7

# ---- Base Node ----
FROM node:${NODE_VERSION} AS base
# Create app directory
WORKDIR /app


# ---- Copy Files/Build ----
FROM node:${NODE_VERSION} AS dev
WORKDIR /app
# A wildcard is used to ensure both package.json AND package-lock.json are copied
COPY package*.json ./
RUN npm install
COPY . ./
# Run development server
CMD ["ls", "/app/src", "/app/public"]


# ---- Dependencies ----
FROM base AS dependencies 
# A wildcard is used to ensure both package.json AND package-lock.json are copied
COPY package*.json ./
# install app dependencies including 'devDependencies'
RUN npm install


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
COPY --from=build /app /app
CMD ["serve", "-s", "build", "-p", "80"]
