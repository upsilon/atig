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
    def initialize(context, consumer, access, channels)
      @log      = context.log
      @opts     = context.opts
      @consumer = consumer
      @access   = access
      @channels = channels
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
              channel = @channels.key?('#twitter') ? @channels['#twitter'] : nil
              data = JSON.parse(chunk) rescue {}
              if data['user']
                source = data['user']['screen_name']
              elsif data['source']
                source = data['source']['screen_name']
              elsif data['target_object']
                source = data['target_object']['user']['screen_name']
              end

              case
              when data['text']
                f.call TwitterStruct.make( data )
              when data['event'] == 'favorite'
                if channel
                  channel.notify "%s \00311favorites\017 => @%s : %s [ http://twitter.com/%s ]" % [ source,
                    data['target_object']['user']['screen_name'],
                    data['target_object']['text'],
                    data['target_object']['user']['screen_name'] ]
                end
              when data['event'] == 'unfavorite'
                if channel
                  channel.notify "%s \00305unfavorites\017 => @%s : %s [ http://twitter.com/%s ]" % [ source,
                      data['target_object']['user']['screen_name'],
                      data['target_object']['text'],
                      data['target_object']['user']['screen_name'] ]
                 end
              end # case
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
