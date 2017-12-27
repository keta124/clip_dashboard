require 'elasticsearch'
require_relative 'influx_connection'
require_relative 'cliptv_channel'
class Cliptv
  CHANNELS = ["vtv1", "vinhlong1", "vtv3", "vtc7", "htvcthuanviet", "antv", "ch1", "htv7", "vtc9", "vtc14", "htv3", "vtv6", "htv9", "vtv5", "vtv2", "vtv4", "vinhlong2", "toonami", "ch2", "vtv9", "htvcphim", "vtc1", "dongthap1", "htvthethao", "cantho", "vtv8", "ch3", "vtv7", "vtc3", "cartoonnetwork", "htv2", "vtc16", "haugiang", "bbcearth", "vtc6", "htvcdulich", "fox1", "axn", "qpvn", "htvccanhac", "htvcplus", "vtc13", "dongnai1", "longan", "vtc11", "ch5", "travinh", "nhandan", "btv4", "hanoi1", "htvcgiadinh", "ttxvn", "camau", "quangbinh", "bacgiang", "warnertv", "cinemaworld", "vtc5", "baclieu", "fox2", "vtc12", "thanhhoa", "fbnc", "cnn", "baria", "phutho", "redbyhbo", "binhduong1", "thainguyen", "ch7", "vtc2", "htv1", "binhphuoc1", "ch4", "haiphong", "binhthuan", "bentre", "htv4", "htvcphunu", "dw", "langson", "sonla", "khanhhoa", "tuyenquang", "quochoi", "gialai", "htvccoop", "htvcshopping", "daknong", "backan", "hanoi2", "dongnai2", "yenbai", "btv9", "laocai", "hanam", "lamdong", "vtc4", "hagiang", "quangninh1", "hoabinh", "ch6", "angiang1", "bacninh", "soctrang", "bbcworldnews", "quangnam", "vtc10", "nghean", "tiengiang", "quangninh3", "tayninh", "haiduong", "bbclifestyle", "binhdinh", "ninhthuan", "vtc8", "kiengiang", "kontum", "phuyen", "ninhbinh", "caobang", "danang1", "quangngai", "hue", "vovtv", "hatinh", "hungyen", "daklak", "fashtiontv", "binhduong2", "aplus", "bloomberg", "vinhphuc", "btv1", "quangtri", "btv2", "htvcthethao", "lasta"]
  class << self
    def map_name_channel value
      CHANNELS.select{|c| value.include? c }
    end
    def get_data_es_uniq_ip
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
                  "gte": "now-1d",
                  "lte": "now",
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
      res = response["aggregations"]["timestamp"]["buckets"].map{|e| [e["key"],e["num_ip"]["value"]]}
      res.pop(1)
      res
    end
    def get_data_es_ccu_group query  #hash group_name:vt1 vt2 fpt1 vdc1 gte lte
      group_name = query[:group_name]
      gte = query[:gte]||"now-1d"
      lte = query[:lte]||"now-1h"
      add_config_query = "response:[200 TO 299] AND (filetype.raw:\"m4s\" OR filetype.raw:\"ts\") NOT avtype.raw:\"ao\" NOT avtype.raw:\"audio\""
      client = Elasticsearch::Client.new host:'192.168.142.100:9200'
      index = 'logstash-*'
      body = {
        "size": 0,
        "query": {
          "filtered": {
            "query": {
              "query_string": {
                "query": "group_name:\"#{group_name}\" AND #{add_config_query}",
                "analyze_wildcard": true
              }
            },
            "filter": {
              "range": {
                "time_write_log": {
                  "gte": "#{gte}",
                  "lte": "#{lte}",
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
              "time_zone": "Asia/Jakarta",
              "extended_bounds": {
                "min": "#{gte}",
                "max": "#{lte}"
              }
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
      res = response["aggregations"]["timestamp"]["buckets"].map{|e| [e["key"],e["types"]["buckets"]]}
      res.pop(1)
      res
    end
    def get_data_es_channel query
      add_config_query = "response:[200 TO 299] AND (filetype.raw:\"m4s\" OR filetype.raw:\"ts\") NOT avtype.raw:\"ao\" NOT avtype.raw:\"audio\""
      gte = query[:gte]||"now-1d"
      lte = query[:lte]||"now-5m"
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
              "bool": {
                "must": [
                  {
                    "range": {
                      "time_write_log": {
                        "gte": "#{gte}",
                        "lte": "#{lte}",
                        "format": "epoch_millis"
                      }
                    }
                  }
                ],
                "must_not": []
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
              "time_zone": "Asia/Jakarta",
              "extended_bounds": {
                "min": "#{gte}",
                "max": "#{lte}"
              }
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
      buckets =response["aggregations"]["timestamp"]["buckets"]
      buckets.pop(1)
      buckets.each do |bucket|
        key = bucket["key"].to_i
        timestamp = Time.at(key/1000).strftime "%Y-%m-%d %H:%M:%S %z"
        channel_buckets = bucket["channel"]["buckets"]
        channels = {}
        CHANNELS.each do |e|
          channels[e] =0
        end
        channel_buckets.each do |channel_bucket|
          channel_map_name = map_name_channel(channel_bucket["key"])
          if channel_map_name.size !=0
            channel = channel_map_name.first
            channels[channel] += channel_bucket["doc_count"].to_i
          end
        end
        # channels.each do |k, v|
        #   object = CliptvChannelCcu.new(:timestamp => timestamp,
        #       :channel=>k,
        #       :ccu =>v)
        #   object.save
        # end
        CliptvChannelCcu.bulk_insert do |worker|
          channels.each do |k, v|
            worker.add timestamp: timestamp, channel: k, ccu: v
          end
        end
      end
    end

    def execute_count_ip
      es_data = get_data_es_uniq_ip
      es_data.each do |a|
        point = {
                values: {
                  ccu: a[1]
                },
                timestamp: (a[0]/1000).to_i
              }
      end
    end
    def execute_count_ccu_group
      group_name =[
        'vdc1',
        'fpt1',
        'vt1',
        'vt2',
        ''
      ]
      group_name.each do |group|
        es_data = get_data_es_ccu_group({group_name:group})
        es_data.each do |a|
          keys = a[1].map{|e| [e["key"],e["doc_count"]]}
          keys.each do |k|
            influx_series = "cliptv_ccu_"+group.to_s+k[0].to_s
            point = {
                  values: {
                    ccu: k[1]
                  },
                  timestamp: (a[0]/1000).to_i
                }
          end
        end
      end
    end
    def execute_count_channel
      channel = ChannelClipTV.channel
      channel.each do |ch|
        influx_series = "cliptv_ccu_"+ch.to_s
        es_data = get_data_es_ccu({streaming_type:"live", streaming_channel:ch})
        es_data.each do |a|
          point = {
                values: {
                  ccu: a[1]
                },
                timestamp: (a[0]/1000).to_i
              }
        end
      end
    end
  end
end