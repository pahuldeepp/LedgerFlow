module Api
  module Admin
    class AdminController < ApplicationController
      def outbox
        Rails.logger.info("===== AUTH DEBUG =====")
        Rails.logger.info("Authorization: #{request.headers['Authorization']}")
        Rails.logger.info("X-User-ID: #{request.headers['X-User-ID']}")
        Rails.logger.info("X-User-Groups: #{request.headers['X-User-Groups']}")
        Rails.logger.info("X-Auth-Request-Groups: #{request.headers['X-Auth-Request-Groups']}")
        Rails.logger.info("======================")

        client = LedgerClient.new
        response = client.list_outbox(
          user_id: current_user_id,
          groups: current_user_groups,
          limit: (params[:limit] || 50).to_i
        )
        render json: response.to_h
      rescue GRPC::BadStatus => e
        Rails.logger.error("gRPC error: #{e.message}")
        render json: { error: "Ledger service unavailable" }, status: 503
      end

      def dlq
        client = LedgerClient.new
        response = client.list_dlq(
          user_id: current_user_id,
          groups: current_user_groups,
          limit: (params[:limit] || 50).to_i
        )
        render json: response.to_h
      rescue GRPC::BadStatus => e
        Rails.logger.error("gRPC error: #{e.message}")
        render json: { error: "Ledger service unavailable" }, status: 503
      end

      def retry_all
        client = LedgerClient.new
        response = client.retry_all_dlq(
          user_id: current_user_id,
          groups: current_user_groups
        )
        render json: response.to_h
      rescue GRPC::BadStatus => e
        Rails.logger.error("gRPC error: #{e.message}")
        render json: { error: "Ledger service unavailable" }, status: 503
      end

      private

      def current_user_id
        request.headers["X-User-ID"] ||
          request.headers["X-Auth-Request-User"]
      end

      def current_user_groups
        request.headers["X-User-Groups"] ||
          request.headers["X-Auth-Request-Groups"]
      end
    end
  end
end