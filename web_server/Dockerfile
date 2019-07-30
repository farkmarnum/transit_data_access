# This is the version of Alpine to use for the final image.
# It must match the Apline that the elixir image uses (sourced through the erlang image)
ARG ALPINE_VERSION=3.9

FROM elixir:1.9-alpine AS builder

# Build args:
ARG APP_NAME
ARG APP_VSN
ARG MIX_ENV=prod
# ARG PHOENIX_SUBDIR=.

ENV APP_NAME=${APP_NAME} \
    APP_VSN=${APP_VSN} \
    MIX_ENV=${MIX_ENV}

WORKDIR /opt/app

# Install the build toolds, since Alpine doesn't have them yet:
RUN apk update \
 && apk upgrade --no-cache \
 && apk add --no-cache     \
        build-base \
        nodejs \
        yarn \
        git \
 && mix local.rebar --force \
 && mix local.hex --force

# Copy in our source code:
COPY . .

RUN mix do deps.get, deps.compile, compile

# This step builds assets for the Phoenix app (if there is one)
# If you aren't building a Phoenix app, pass `--build-arg SKIP_PHOENIX=true`
# This is mostly here for demonstration purposes
# RUN cd ${PHOENIX_SUBDIR}/assets \
#  && yarn install \
#  && yarn deploy \
#  && cd - \
#  && mix phx.digest

# Let's distill!
RUN mkdir -p /opt/built \
 && mix distillery.release --verbose \
 && cp _build/${MIX_ENV}/rel/${APP_NAME}/releases/${APP_VSN}/${APP_NAME}.tar.gz /opt/built \
 && cd /opt/built \
 && tar -xzf ${APP_NAME}.tar.gz

# PRODUCTION BUILD LAYER:
FROM alpine:${ALPINE_VERSION}

ARG APP_NAME

RUN apk update \
 && apk add --no-cache \
        bash \
        openssl-dev

ENV REPLACE_OS_VARS=true \
    APP_NAME=${APP_NAME}

WORKDIR /opt/app

COPY --from=builder /opt/built .

CMD /opt/app/bin/${APP_NAME} foreground