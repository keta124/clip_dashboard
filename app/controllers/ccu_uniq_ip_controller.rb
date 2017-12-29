class CcuUniqIpController < ApplicationController
  def show
    respond_to do |format|
      format.html
      format.json do
        data = InfluxdbConnection.connection.query 'select * from ccu_unique_ip group by value'
        render json: data.map{|d| [d.first.to_i, d.last.to_f]}.to_json
      end
    end
  end
end
