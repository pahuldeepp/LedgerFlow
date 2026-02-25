Rails.application.routes.draw do
  get "/health",  to: "health#show"
  get "/up",      to: "health#up"
  get "/healthz", to: "health#healthz"

  namespace :api do
    get "health", to: "/health#show"

    # ✅ add these:
    get "me", to: "auth#me"
    get "observability/summary", to: "observability#summary"

    namespace :admin do
      get  "outbox",        to: "admin#outbox"
      get  "dlq",           to: "admin#dlq"
      post "dlq/retry-all", to: "admin#retry_all"
    end
  end
end