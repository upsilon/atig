#! /opt/local/bin/ruby -w
# -*- mode:ruby; coding:utf-8 -*-
require 'atig/util'

module Atig
  module Agent
    class UserStream
      include Util

      def initialize(context, api, db)
        @log = context.log
        @api = api
        @prev = nil

        return unless context.opts.stream

        log :info, "initialize"

        query = {}
        if context.opts.replies then
          query[:replies] = 'all'
        end

        @api.stream do|t|
          t.watch('user', query) do |status|
#            @log.debug status.inspect
            if status and status.user
              db.statuses.transaction do|d|
                d.add :status => status, :user => status.user, :source => :user_stream
              end
            end
          end
        end
      end
    end
  end
end
