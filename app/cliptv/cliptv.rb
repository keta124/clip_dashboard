class Cliptv
  CHANNELS = ["vtv1", "vinhlong1", "vtv3", "vtc7", "htvcthuanviet", "antv", "ch1", "htv7", "vtc9", "vtc14", "htv3", "vtv6", "htv9", "vtv5", "vtv2", "vtv4", "vinhlong2", "toonami", "ch2", "vtv9", "htvcphim", "vtc1", "dongthap1", "htvthethao", "cantho", "vtv8", "ch3", "vtv7", "vtc3", "cartoonnetwork", "htv2", "vtc16", "haugiang", "bbcearth", "vtc6", "htvcdulich", "fox1", "axn", "qpvn", "htvccanhac", "htvcplus", "vtc13", "dongnai1", "longan", "vtc11", "ch5", "travinh", "nhandan", "btv4", "hanoi1", "htvcgiadinh", "ttxvn", "camau", "quangbinh", "bacgiang", "warnertv", "cinemaworld", "vtc5", "baclieu", "fox2", "vtc12", "thanhhoa", "fbnc", "cnn", "baria", "phutho", "redbyhbo", "binhduong1", "thainguyen", "ch7", "vtc2", "htv1", "binhphuoc1", "ch4", "haiphong", "binhthuan", "bentre", "htv4", "htvcphunu", "dw", "langson", "sonla", "khanhhoa", "tuyenquang", "quochoi", "gialai", "htvccoop", "htvcshopping", "daknong", "backan", "hanoi2", "dongnai2", "yenbai", "btv9", "laocai", "hanam", "lamdong", "vtc4", "hagiang", "quangninh1", "hoabinh", "ch6", "angiang1", "bacninh", "soctrang", "bbcworldnews", "quangnam", "vtc10", "nghean", "tiengiang", "quangninh3", "tayninh", "haiduong", "bbclifestyle", "binhdinh", "ninhthuan", "vtc8", "kiengiang", "kontum", "phuyen", "ninhbinh", "caobang", "danang1", "quangngai", "hue", "vovtv", "hatinh", "hungyen", "daklak", "fashtiontv", "binhduong2", "aplus", "bloomberg", "vinhphuc", "btv1", "quangtri", "btv2", "htvcthethao", "lasta"]
  class << self
    def map_name_channel value
      CHANNELS.select{|c| value.include? c }
    end

    def get_ccu_uniq_ip
      gte = "now-1d/d"
      lt = "now/d"
      client = Elasticsearch::Client.new host:'192.168.142.100:9200'
      index = 'logstash-*'
      body = {
        "size": 0,
        "query": {
          "filtered": {
            "query": {
              "query_string": {
                "query": "response:[200 TO 299]",
                "analyze_wildcard": true
              }
            },
            "filter": {
              "range": {
                "time_write_log": {
                  "gte": "#{gte}",
                  "lt": "#{lt}",
                  "format": "epoch_second"
                }
              }
            }
          }
        },
        "aggs": {
          "timestamp": {
            "date_histogram": {
              "field": "time_write_log",
              "interval": "15m",
              "time_zone": "Asia/Jakarta",
              "min_doc_count": 1
            },
            "aggs": {
              "num_ip": {
                "cardinality": {
                  "field": "client_ip.raw",
                  "precision_threshold": 10000
                }
              }
            }
          }
        }
      }
      response = client.search index: index, body: body
      res = response["aggregations"]["timestamp"]["buckets"]
      data =[]
      # CliptvCcuUniqueIp.bulk_insert do |worker|
      #   res.each do |e|
      #     key = e["key"].to_i
      #     timestamp = Time.at(key/1000).strftime "%Y-%m-%d %H:%M:%S"
      #     ccu = e["num_ip"]["value"]
      #     worker.add timestamp: timestamp, ccu: ccu
      #   end
      # end
      res.each do |e|
        timestamp = (e["key"].to_i / 1000).to_i
        ccu = e["num_ip"]["value"]
        data << { series: 'ccu_unique_ip', values: { value: ccu }, timestamp: timestamp }
      end
      InfluxdbConnection.connection.write_points data
    end

    def get_ccu_datacenter
      datacenters =['vdc1', 'fpt1', 'vt1', 'vt2', '']
      gte = "now-1d/d"
      lt = "now/d"
      add_config_query = "response:[200 TO 299] AND (filetype.raw:\"m4s\" OR filetype.raw:\"ts\") NOT avtype.raw:\"ao\" NOT avtype.raw:\"audio\""
      datacenters.each do |dc|
        client = Elasticsearch::Client.new host:'192.168.142.100:9200'
        index = 'logstash-*'
        body = {
          "size": 0,
          "query": {
            "filtered": {
              "query": {
                "query_string": {
                  "query": "group_name:\"#{dc}\" AND #{add_config_query}",
                  "analyze_wildcard": true
                }
              },
              "filter": {
                "range": {
                  "time_write_log": {
                    "gte": "#{gte}",
                    "lt": "#{lt}",
                    "format": "epoch_millis"
                  }
                }
              }
            }
          },
          "aggs": {
            "timestamp": {
              "date_histogram": {
                "field": "time_write_log",
                "interval": "10s",
                "time_zone": "Asia/Jakarta"
              },
              "aggs": {
                "types": {
                  "terms": {
                    "field": "streaming_type",
                    "size": 5,
                    "order": {
                      "_count": "desc"
                    }
                  }
                }
              }
            }
          }
        }
        response = client.search index: index, body: body
        res = response["aggregations"]["timestamp"]["buckets"]
        res.pop(1)
        # CliptvDatacenterCcu.bulk_insert do |worker|
        #   res.each do |e|
        #     key = e["key"].to_i
        #     if key % 60000 == 0
        #       timestamp = Time.at(key/1000).strftime "%Y-%m-%d %H:%M:%S"
        #       ccu = e["types"]["buckets"].map{|k| [k["key"], k["doc_count"]] }.to_h
        #       dc = "all" if dc == ''
        #       worker.add timestamp: timestamp, datacenter: dc, ccu_all: ccu["all"], ccu_live: ccu["live"],ccu_vod: ccu["vod"]
        #     end
        #   end
        # end
        data =[]
        res.each do |e|
          timestamp = (e["key"].to_i / 1000).to_i
          if timestamp % 60 == 0
            ccu = e["types"]["buckets"].map{|k| [k["key"], k["doc_count"]] }.to_h
            dc = "all" if dc == ''
            data << { series: 'ccu_datacenter', values: { value: ccu["all"] || 0 }, tags:   { type: 'all' }, timestamp: timestamp }
            data << { series: 'ccu_datacenter', values: { value: ccu["live"] || 0}, tags:   { type: 'live' }, timestamp: timestamp }
            data << { series: 'ccu_datacenter', values: { value: ccu["vod"] || 0}, tags:   { type: 'vod' }, timestamp: timestamp }
          end
        end
        InfluxdbConnection.connection.write_points data
      end
    end

    def get_ccu_channel 
      add_config_query = "response:[200 TO 299] AND (filetype.raw:\"m4s\" OR filetype.raw:\"ts\") NOT avtype.raw:\"ao\" NOT avtype.raw:\"audio\""
      gte = "now-d/d"
      lt = "now/d"
      client = Elasticsearch::Client.new host:'192.168.142.100:9200'
      index = 'logstash-*'
      body= {
        "query": {
          "filtered": {
            "query": {
              "query_string": {
                "query": "#{add_config_query}",
                "analyze_wildcard": true
              }
            },
            "filter": {
              "range": {
                "time_write_log": {
                  "gte": "#{gte}",
                  "lt": "#{lt}",
                  "format": "epoch_millis"
                }
              }
            }
          }
        },
        "size": 0,
        "aggs": {
          "timestamp": {
            "date_histogram": {
              "field": "time_write_log",
              "interval": "10s",
              "time_zone": "Asia/Jakarta"
            },
            "aggs": {
              "channel": {
                "terms": {
                  "field": "streaming_channel.raw",
                  "size": 500,
                  "order": {
                    "_count": "desc"
                  }
                }
              }
            }
          }
        }
      }
      response = client.search index: index, body: body
      res =response["aggregations"]["timestamp"]["buckets"]
      res.pop(1)
      # CliptvChannelCcu.bulk_insert do |worker|
      #   buckets.each do |bucket|
      #     key = bucket["key"].to_i
      #     if key % 60000 == 0
      #       timestamp = Time.at(key/1000).strftime "%Y-%m-%d %H:%M:%S %z"
      #       channel_buckets = bucket["channel"]["buckets"]
      #       channels = {}
      #       channel_buckets.each do |channel_bucket|
      #         channel_map_name = map_name_channel(channel_bucket["key"])
      #         if channel_map_name.size !=0
      #           channel = channel_map_name.first
      #           channels[channel] = (channels[channel] ||0 ) + channel_bucket["doc_count"].to_i
      #         end
      #       end
      #       channels.each do |k, v|
      #         worker.add timestamp: timestamp, channel: k, ccu: v
      #       end
      #     end
      #   end
      # end
      data =[]
      res.each do |e|
        timestamp = (e["key"].to_i / 1000).to_i
        if timestamp % 60 == 0
          channel_buckets = e["channel"]["buckets"]
          channels = {}
          channel_buckets.each do |channel_bucket|
            channel_map_name = map_name_channel(channel_bucket["key"])
            if channel_map_name.size !=0
              channel = channel_map_name.first
              channels[channel] = (channels[channel] ||0 ) + channel_bucket["doc_count"].to_i
            end
          end
          channels.each do |k, v|
            data << { series: 'ccu_channel', values: { value: v }, tags:   { channel: k }, timestamp: timestamp }
          end
        end
      end
      InfluxdbConnection.connection.write_points data
    end
  end
end