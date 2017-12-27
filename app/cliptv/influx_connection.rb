require 'influxdb'
module InfluxConnection

  def self.connection
    InfluxDB::Client.new 'cliptv',
      host: 'localhost',
      username: 'sontn',
      password: 'Son@1123',
      time_precision: 's'
  end

end
