class CreateCliptvCcuUniqueIps < ActiveRecord::Migration[5.1]
  def change
    create_table :cliptv_ccu_unique_ips do |t|

      t.datetime :timestamp, null: false
      t.integer :ccu, default: 0
      
      t.timestamps
    end
  end
end
