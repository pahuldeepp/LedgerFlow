require "active_support/core_ext/integer/time"

Rails.application.configure do
  config.enable_reloading = true
  config.eager_load = false
  config.consider_all_requests_local = true
  config.server_timing = true
  config.hosts << "web"
  config.hosts << "web:3000"
  config.hosts << "localhost"
  config.hosts << "ledger-go"

  config.cache_store = :memory_store

  config.active_support.deprecation = :log

  config.active_record.migration_error = :page_load
  config.active_record.verbose_query_logs = true
  config.active_record.query_log_tags_enabled = true
end