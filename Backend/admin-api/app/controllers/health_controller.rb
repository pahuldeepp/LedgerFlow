class HealthController < ApplicationController
  def show
    results = health_results
    overall = overall_status(results)
    render json: results.merge(status: overall), status: :ok
  end

  def up
    render json: { status: "up" }, status: :ok
  end

  def healthz
    results = health_results
    overall = overall_status(results)
    http_status = overall == "ok" ? :ok : :service_unavailable
    render json: results.merge(status: overall), status: http_status
  end

  private

  def health_results
    {
      database: database_status,
      ledger: ledger_status,
      kafka: kafka_status,
      outbox: outbox_status,
      dlq: dlq_status
    }
  end

  def overall_status(results)
    results.values.all? { |v| v == "ok" } ? "ok" : "degraded"
  end

  def database_status
    ActiveRecord::Base.connection.execute("SELECT 1")
    "ok"
  rescue => e
    Rails.logger.error("Database health failed: #{e.message}")
    "down"
  end

  def ledger_status
    client = LedgerClient.new
    client.list_outbox
    "ok"
  rescue => e
    Rails.logger.error("Ledger health failed: #{e.message}")
    "down"
  end

  def kafka_status
    require "socket"
    broker = ENV.fetch("KAFKA_BROKERS", "").split(",").first
    host, port = broker.split(":")
    socket = TCPSocket.new(host, port.to_i)
    socket.close
    "ok"
  rescue => e
    Rails.logger.error("Kafka health failed: #{e.message}")
    "down"
  end

  def outbox_status
    client = LedgerClient.new
    resp = client.list_outbox
    resp.respond_to?(:items) ? "ok" : "down"
  rescue => e
    Rails.logger.error("Outbox health failed: #{e.message}")
    "down"
  end

  def dlq_status
    client = LedgerClient.new
    resp = client.list_dlq
    if resp && resp.respond_to?(:items)
      "ok"
    else
      Rails.logger.warn("DLQ response unexpected: #{resp.inspect}")
      "down"
    end
  rescue => e
    Rails.logger.error("DLQ health failed: #{e.message}")
    "down"
  end
end
