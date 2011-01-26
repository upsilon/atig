#! /opt/local/bin/ruby -w
# -*- mode:ruby; coding:utf-8 -*-

require 'json'
require 'uri'
require 'logger'
require 'atig/twitter_struct'
require 'atig/util'
require 'atig/url_escape'

module Atig
  class Stream
    include Util

    class APIFailed < StandardError; end
    def initialize(context, consumer, access)
      @log      = context.log
      @opts     = context.opts
      @consumer = consumer
      @access   = access
    end

    def watch(path, query={}, &f)
      path.sub!(%r{\A/+}, "")

      uri = api_base
      uri.path += path
      uri.path += ".json"
      uri.query = query.to_query_str unless query.empty?

      @log.debug [uri.to_s]

      http = Net::HTTP.new(uri.host, uri.port)
      http.use_ssl = true
      request = Net::HTTP::Post.new uri.request_uri
      request.oauth!(http, @consumer, @access)
      http.request(request) do |response|
        unless response.code == '200' then
          raise APIFailed,"#{response.code} #{response.message}"
        end

        begin
          buf = ''
          response.read_body do |str|
            buf << str
            buf.gsub!(/[\s\S]+?\r\n/) do |chunk|
              data = JSON.parse(chunk) rescue {}
              f.call TwitterStruct.make( data )
            end
            buf = ''
          end
        rescue => e
          raise APIFailed,e.to_s
        end
      end
    end

    def api_base
      URI(@opts.stream_api_base)
    end
  end
end
