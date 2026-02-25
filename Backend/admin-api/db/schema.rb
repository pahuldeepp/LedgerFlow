# This file is auto-generated from the current state of the database. Instead
# of editing this file, please use the migrations feature of Active Record to
# incrementally modify your database, and then regenerate this schema definition.
#
# This file is the source Rails uses to define your schema when running `bin/rails
# db:schema:load`. When creating a new database, `bin/rails db:schema:load` tends to
# be faster and is potentially less error prone than running all of your
# migrations from scratch. Old migrations may fail to apply correctly if those
# migrations use external dependencies or application code.
#
# It's strongly recommended that you check this file into your version control system.

ActiveRecord::Schema[8.1].define(version: 2026_02_21_022931) do
  # These are extensions that must be enabled in order to support this database
  enable_extension "pg_catalog.plpgsql"
  enable_extension "pgcrypto"

  create_table "accounts", id: :text, force: :cascade do |t|
    t.timestamptz "created_at", default: -> { "now()" }, null: false
    t.text "name", null: false
  end

  create_table "admins", force: :cascade do |t|
    t.datetime "created_at", null: false
    t.string "email"
    t.string "name"
    t.datetime "updated_at", null: false
  end

  create_table "entries", id: :uuid, default: -> { "gen_random_uuid()" }, force: :cascade do |t|
    t.text "account_id", null: false
    t.bigint "amount", null: false
    t.timestamptz "created_at", default: -> { "now()" }, null: false
    t.text "direction", null: false
    t.uuid "transaction_id", null: false
    t.index ["account_id"], name: "idx_entries_account"
    t.index ["transaction_id"], name: "idx_entries_tx"
    t.check_constraint "amount > 0", name: "entries_amount_check"
    t.check_constraint "direction = ANY (ARRAY['debit'::text, 'credit'::text])", name: "entries_direction_check"
  end

  create_table "outbox", id: :uuid, default: -> { "gen_random_uuid()" }, force: :cascade do |t|
    t.text "aggregate_id", null: false
    t.text "aggregate_type", null: false
    t.integer "attempts", default: 0, null: false
    t.timestamptz "created_at", default: -> { "now()" }, null: false
    t.text "event_type", null: false
    t.text "last_error"
    t.timestamptz "locked_at"
    t.text "locked_by"
    t.timestamptz "next_attempt_at"
    t.jsonb "payload", null: false
    t.timestamptz "published_at"
    t.index ["created_at"], name: "idx_outbox_created"
    t.index ["created_at"], name: "idx_outbox_unpublished_ready", where: "(published_at IS NULL)"
    t.index ["locked_at"], name: "idx_outbox_locked", where: "((published_at IS NULL) AND (locked_by IS NOT NULL))"
    t.index ["next_attempt_at"], name: "idx_outbox_next_attempt"
    t.index ["published_at", "next_attempt_at", "created_at"], name: "idx_outbox_ready"
    t.index ["published_at"], name: "idx_outbox_unpublished", where: "(published_at IS NULL)"
  end

  create_table "outbox_dlq", id: :uuid, default: nil, force: :cascade do |t|
    t.text "aggregate_id", null: false
    t.text "aggregate_type", null: false
    t.integer "attempts", default: 0, null: false
    t.timestamptz "created_at", default: -> { "now()" }, null: false
    t.text "event_type", null: false
    t.text "last_error"
    t.timestamptz "moved_to_dlq_at", default: -> { "now()" }, null: false
    t.jsonb "payload", null: false
    t.index ["created_at"], name: "idx_outbox_dlq_created_at"
  end

  create_table "transactions", id: :uuid, default: -> { "gen_random_uuid()" }, force: :cascade do |t|
    t.timestamptz "created_at", default: -> { "now()" }, null: false
    t.text "idempotency_key", null: false
    t.text "status", default: "posted", null: false

    t.unique_constraint ["idempotency_key"], name: "transactions_idempotency_key_key"
  end

  add_foreign_key "entries", "accounts", name: "entries_account_id_fkey"
  add_foreign_key "entries", "transactions", name: "entries_transaction_id_fkey", on_delete: :restrict
end
