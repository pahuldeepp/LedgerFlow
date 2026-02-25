require "rails"

require "active_model/railtie"
require "active_job/railtie"
require "active_record/railtie"
require "action_controller/railtie"
require "action_dispatch/railtie"
require "action_view/railtie"

# Skip:
# require "action_mailer/railtie"
# require "action_mailbox/railtie"
# require "action_text/engine"
# require "active_storage/engine"
# require "action_cable/engine"
module App
  class Application < Rails::Application
    config.load_defaults 8.1

    config.autoload_lib(ignore: %w[assets tasks])

    config.autoload_paths << Rails.root.join("app/grpc")
    config.autoload_paths << Rails.root.join("app/services")
    
    $LOAD_PATH.unshift Rails.root.join("app/grpc/proto").to_s
    config.api_only = true
  end
end