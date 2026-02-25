module Api
  class AuthController < ApplicationController
    def me
      user_id = request.headers["X-User-ID"].presence
      user_id ||= request.headers["X-Auth-Request-User"].presence
      user_id ||= "local-dev"

      groups_raw = request.headers["X-User-Groups"].to_s
      if groups_raw.blank?
        groups_raw = request.headers["X-Auth-Request-Groups"].to_s
      end
      groups = groups_raw.split(/[,\s]+/).map(&:strip).reject(&:empty?).map(&:downcase)

      role = groups.include?("admin") ? "admin" : ENV.fetch("DEFAULT_ROLE", "viewer")

      render json: { user: { id: user_id, role: role, groups: groups } }
    end
  end
end