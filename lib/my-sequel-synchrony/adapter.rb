# Sequel adapter for MySQL that uses the Mysql2::EM::Client and a fiber-aware connection pool.
#
# See also:
#   https://github.com/jeremyevans/sequel/blob/master/lib/sequel/adapters/mysql2.rb
#   https://github.com/brianmario/mysql2/blob/master/lib/mysql2/em.rb

require "sequel"
require "sequel/adapters/mysql2"
require "em-synchrony/mysql2"
require "my-sequel-synchrony/connection_pool"

module Sequel::Mysql2::Synchrony
  class Database < ::Sequel::Mysql2::Database
    set_adapter_scheme :mysql2synchrony

    def initialize(opts={})
      pool_class = if opts.include?(:servers)
        ::Sequel::Mysql2::Synchrony::ShardedConnectionPool
      else
        ::Sequel::Mysql2::Synchrony::ConnectionPool
      end
      super(opts.merge(:pool_class => pool_class, :connection_handling => :queue))
    end

    # Connect to the database. Similar to the Sequel::Mysql2#connect method, but uses the eventmachine
    # client.
    #
    # NOTE: this code is mostly a copy of Sequel::Mysql2#connect() method, with the only difference being
    # the connection class (Mysql::EM::Client instead of Mysql2::Client). This code could be removed if
    # the parent class accepted a :use_eventmachine option.
    def connect(server)
      opts = server_opts(server)
      opts[:host] ||= 'localhost'
      opts[:username] ||= opts[:user]
      opts[:flags] = ::Mysql2::Client::FOUND_ROWS if ::Mysql2::Client.const_defined?(:FOUND_ROWS)
      conn = ::Mysql2::EM::Client.new(opts)
      conn.query_options.merge!(:symbolize_keys=>true, :cache_rows=>false)

      sqls = mysql_connection_setting_sqls

      # Set encoding a slightly different way after connecting,
      # in case the READ_DEFAULT_GROUP overrode the provided encoding.
      # Doesn't work across implicit reconnects, but Sequel doesn't turn on
      # that feature.
      if encoding = opts[:encoding] || opts[:charset]
        sqls.unshift("SET NAMES #{conn.escape(encoding.to_s)}")
      end

      sqls.each{|sql| log_yield(sql){conn.query(sql)}}

      add_prepared_statements_cache(conn)
      conn
    end
  end
end
