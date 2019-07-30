import Config

config :web_server,
  log_level: System.get_env("LOG_LEVEL"),
  redis_hostname: System.get_env("REDIS_HOSTNAME"),
  redis_port: System.get_env("REDIS_PORT")

import_config "#{Mix.env()}.exs"
