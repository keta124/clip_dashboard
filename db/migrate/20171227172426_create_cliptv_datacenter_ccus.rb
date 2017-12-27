class CreateCliptvDatacenterCcus < ActiveRecord::Migration[5.1]
  def change
    create_table :cliptv_datacenter_ccus do |t|
      t.datetime :timestamp, null: false
      t.string :datacenter, null: false
      t.integer :ccu_all, default: 0
      t.integer :ccu_live, default: 0
      t.integer :ccu_vod, default: 0

      t.timestamps
    end
  end
end
#timestamp: timestamp, datacenter: dc, ccu_all: ccu["all"], ccu_live: ccu["live"],ccu_vod: ccu["vod"]