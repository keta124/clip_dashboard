# This file is auto-generated from the current state of the database. Instead
# of editing this file, please use the migrations feature of Active Record to
# incrementally modify your database, and then regenerate this schema definition.
#
# Note that this schema.rb definition is the authoritative source for your
# database schema. If you need to create the application database on another
# system, you should be using db:schema:load, not running all the migrations
# from scratch. The latter is a flawed and unsustainable approach (the more migrations
# you'll amass, the slower it'll run and the greater likelihood for issues).
#
# It's strongly recommended that you check this file into your version control system.

ActiveRecord::Schema.define(version: 20171227173231) do

  # These are extensions that must be enabled in order to support this database
  enable_extension "plpgsql"

  create_table "cliptv_ccu_unique_ips", force: :cascade do |t|
    t.datetime "timestamp", null: false
    t.integer "ccu", default: 0
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
  end

  create_table "cliptv_channel_ccus", force: :cascade do |t|
    t.datetime "timestamp", null: false
    t.string "channel"
    t.integer "ccu", default: 0
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
  end

  create_table "cliptv_datacenter_ccus", force: :cascade do |t|
    t.datetime "timestamp", null: false
    t.string "datacenter", null: false
    t.integer "ccu_all", default: 0
    t.integer "ccu_live", default: 0
    t.integer "ccu_vod", default: 0
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
  end

end
