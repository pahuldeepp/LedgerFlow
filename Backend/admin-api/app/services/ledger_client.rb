require "grpc"

require_relative "../grpc/proto/admin_pb"
require_relative "../grpc/proto/admin_services_pb"

class LedgerClient
  def initialize
    @stub = ::Ledgerflow::Admin::V1::AdminService::Stub.new(
      ENV.fetch("LEDGER_GRPC_HOST", "ledger:9090"),
      :this_channel_is_insecure
    )
  end

  def list_outbox(user_id:, groups:, status: :OUTBOX_STATUS_UNSPECIFIED, limit: 50)
    request = ::Ledgerflow::Admin::V1::ListOutboxRequest.new(
      status: ::Ledgerflow::Admin::V1::OutboxStatus.const_get(status),
      limit: limit
    )

    @stub.list_outbox(request, metadata: grpc_metadata(user_id, groups))
  end

  def list_dlq(user_id:, groups:, limit: 50)
    request = ::Ledgerflow::Admin::V1::ListDlqRequest.new(limit: limit)
    @stub.list_dlq(request, metadata: grpc_metadata(user_id, groups))
  end

  def retry_all_dlq(user_id:, groups:)
    request = ::Ledgerflow::Admin::V1::RetryAllDlqRequest.new
    @stub.retry_all_dlq(request, metadata: grpc_metadata(user_id, groups))
  end

  private

  def grpc_metadata(user_id, groups)
    # Go side expects:
    # X-Internal-Auth=true, X-User-ID, X-User-Groups
    {
      "x-internal-auth" => "true",
      "x-user-id" => user_id.to_s,
      "x-user-groups" => normalize_groups(groups)
    }
  end

  def normalize_groups(groups)
    case groups
    when Array
      groups.map(&:to_s).join(",")
    else
      groups.to_s
    end
  end
end