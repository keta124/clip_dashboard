class CreateCliptvChannelCcus < ActiveRecord::Migration[5.1]
  def change
    create_table :cliptv_channel_ccus do |t|
      t.datetime :timestamp, null: false
      t.string :channel
      t.integer :ccu, default: 0

      t.timestamps
    end
  end
end
