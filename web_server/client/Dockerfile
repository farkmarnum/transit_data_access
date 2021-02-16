# ---- Base Node ----
FROM node:15 AS base
# Create app directory
WORKDIR /app


# ---- Copy Files/Build ----
FROM node:15 AS dev
WORKDIR /app
# A wildcard is used to ensure both package.json AND package-lock.json are copied
COPY package*.json /app/
RUN npm install
COPY . /app/
# Run development server
CMD npm start


# ---- Dependencies ----
FROM base AS dependencies
# A wildcard is used to ensure both package.json AND package-lock.json are copied
COPY package*.json ./
RUN npm install --only=production


# ---- Copy Files/Build ----
FROM dependencies AS build
WORKDIR /app
COPY . /app
# Build react/vue/angular bundle static files
RUN npm run build


# --- Release with Alpine ----
FROM node:15-alpine AS release  
# Create app directory
WORKDIR /app
RUN npm -g install serve

COPY --from=build /app /app
CMD ["serve", "-s", "build", "-p", "80"]
