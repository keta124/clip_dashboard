module InfluxdbConnection

  def self.connection
    InfluxDB::Client.new 'cliptv_statistic',
      host: 'localhost',
      username: 'sontn',
      password: 'Son@1123',
      time_precision: 's'
  end

end