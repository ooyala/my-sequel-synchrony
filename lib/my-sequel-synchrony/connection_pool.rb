require "fiber"
require "set"
require "sequel"
require "sequel/adapters/mysql2"

module Sequel::Mysql2::Synchrony
  # A Fiber-aware Sequel::ConnectionPool that works with EM::Synchrony! This version is not shard-aware.
  class ConnectionPool < ::Sequel::ConnectionPool
    # The maximum number of connections this pool will create (per shard/server
    # if sharding).
    attr_reader :max_size

    DEFAULT_MAX_SIZE = 4

    # The following additional options are respected:
    # * :connection_handling - Set how to handle available connections.  By default,
    #   uses a a stack for performance.  Can be set to :queue to use a queue, which reduces
    #   the chances of connections becoming stale. Can also be set to :disconnect, which will
    #   disconnect after every query or transaction.
    #
    # * :max_connections - The maximum number of connections the connection pool
    #   will open (default 4)
    def initialize(opts = {}, &block)
      super
      @max_size = Integer(opts[:max_connections] || DEFAULT_MAX_SIZE)
      raise(Sequel::Error, ':max_connections must be positive') if @max_size < 1
      @connection_handling = opts[:connection_handling]
      @available_connections = []
      @waiting_fibers = []
      @in_use_connections = {}
      @pending_disconnects = Set.new
      # TODO(ilyam): remove the debug prints once this is rock-solid and has good tests etc.
      @debug = opts[:debug]
    end

    # The total number of open connections, either available or in-use.
    def size
      @in_use_connections.size + @available_connections.size
    end

    # Removes all connections currently available, optionally
    # yielding each connection to the given block. This method has the effect of
    # disconnecting from the database, assuming that no connections are currently
    # being used. Schedules all in-use connections to be disconnected next time they are
    # released into the available pool.
    #
    # After a disconnect, when connections are requested using #hold, the connection pool will
    # create new connections to the database.
    def disconnect(opts={}, &block)
      if_debug? { dputs "disconnect(opts = #{opts.inspect})! Current fiber = #{Fiber.current.inspect}" }
      connections_to_disconnect, @available_connections = @available_connections, []
      @pending_disconnects += @in_use_connections.values.to_set
      block ||= @disconnection_proc
      connections_to_disconnect.each do |conn|
        disconnect_conn(conn, opts, block)
      end
    end

    # Chooses the first available connection, or if none are
    # available, creates a new connection.  Passes the connection to the supplied
    # block:
    #
    #   pool.hold {|conn| conn.execute('DROP TABLE posts')}
    #
    # Pool#hold is re-entrant, meaning it can be called recursively in
    # the same fiber safely.
    #
    # If no connection is immediately available and the pool is already using the maximum
    # number of connections, Pool#hold will block the current fiber until a connection
    # is available.
    def hold(server = nil)
      if_debug? { dputs "hold(server = #{server.inspect})! Current fiber = #{Fiber.current.inspect}" }
      fiber, conn = Fiber.current, nil
      if conn = @in_use_connections[fiber]
        return yield(conn)
      end

      begin
        conn = acquire(fiber)
        yield conn
      rescue Sequel::DatabaseDisconnectError => error
        disconnect_conn(conn) if conn
        conn = nil
        @in_use_connections.delete(fiber)
        resume_next_waiting_fiber
        raise error
      ensure
        release(fiber) if conn
      end
    end

    private

    # Assigns a connection to the given fiber. Suspends the fiber until a connection is available.
    # Suspended fibers are served in FIFO order.
    def acquire(fiber)
      if_debug? { dputs "acquire(fiber = #{fiber.inspect})! Current fiber = #{Fiber.current.inspect}" }
      conn = nil
      while conn.nil?
        if conn = available
          @in_use_connections[fiber] = conn
        else
          wait_for_connection(fiber)
        end
      end
      conn
    end

    # Releases the connection assigned to the given fiber back to the pool. May actually disconnect the
    # connection in one of two cases:
    #   1) A Pool#disconnect() was called while this connection was in use, which added it to the
    #      @pending_disconnects set.
    #   2) The pool was initialized with :connection_handling => :disconnect
    def release(fiber)
      if_debug? { dputs "release(fiber = #{fiber.inspect})! Current fiber = #{Fiber.current.inspect}" }
      conn = @in_use_connections.delete(fiber)
      raise(Sequel::Error, "release called on fiber that doesn't own a connection!") unless conn

      if (@connection_handling == :disconnect || @pending_disconnects.include?(conn))
        disconnect_conn(conn)
      elsif @connection_handling == :queue
        @available_connections.unshift(conn)
      else
        @available_connections << conn
      end
      resume_next_waiting_fiber
    end

    # Disconnects the connection and removes it from @pending_disconnects if it was in the set.
    def disconnect_conn(conn, opts={}, &block)
      if_debug? do
        dputs "disconnect_conn(conn = #{conn.inspect}, opts=#{opts.inspect}, block = #{block.inspect})! "\
              "Current fiber = #{Fiber.current.inspect}"
      end
      block ||= @disconnection_proc
      block.call(conn) if block
      @pending_disconnects.delete(conn)
    end

    # Suspends the given fiber until a connection becomes available.
    def wait_for_connection(fiber)
      if_debug? do
        dputs "wait_for_connection(fiber = #{fiber.inspect})! Current fiber = #{Fiber.current.inspect}"
      end
      Fiber.yield @waiting_fibers.push fiber
    end

    # Resumes the next fiber waiting for a connection
    def resume_next_waiting_fiber
      if_debug? { dputs "resume_next_waiting_fiber()! Current fiber = #{Fiber.current.inspect}" }
      if pending_fiber = @waiting_fibers.shift
        if_debug? { dputs "  => pending_fiber = #{pending_fiber.inspect}" }
        pending_fiber.resume
      end
    end

    # Returns an available connection. If no connection is available, tries to create a new connection if the
    # current pool size is smaller than the max pool size. May return nil.
    def available
      if_debug? { dputs "available()! Current fiber = #{Fiber.current.inspect}" }
      @available_connections.pop || make_new(DEFAULT_SERVER)
    end

    # Alias the default make_new method, so subclasses can call it directly.
    alias default_make_new make_new

    # Creates a new connection to the given server if the size of the pool for
    # the server is less than the maximum size of the pool.
    def make_new(server)
      if_debug? { dputs "make_new(server = #{server.inspect})! Current fiber = #{Fiber.current.inspect}" }
      if size >= @max_size
        dead_fibers = @in_use_connections.keys.reject { |fiber| fiber.alive? }
        dead_fibers.each { |fiber| release(fiber) }
      end
      size < @max_size ? super(server) : nil
    end

    # Calls the given block if @debug is true
    def if_debug?(&block)
      block.call if @debug
    end

    # like puts, but goes to STDERR and prepends the string "DEBUG: "
    def dputs(*args)
      args.each { |arg| STDERR.puts "DEBUG: #{arg}" }
      nil
    end
  end

  # The sharded version of the em-synchrony connection pool is automatically used if a :servers option is
  # passed to Sequel.connect(). It keeps a mapping from server ids to non-sharded connection pools, and
  # delegates calls to them as needed.
  class ShardedConnectionPool < ::Sequel::ConnectionPool
    # The following additional options are respected:
    # * :servers - A hash of servers to use.  Keys should be symbols.  If not
    #   present, will use a single :default server.  The server name symbol will
    #   be passed to the connection_proc.
    #   The values in the hash should be per-server specific options (such as different :hostname or :database)
    #
    # * :servers_hash - The base hash to use for the servers.  By default,
    #   Sequel uses Hash.new(:default).  You can use a hash with a default proc
    #   that raises an error if you want to catch all cases where a nonexistent
    #   server is used.
    def initialize(opts = {}, &block)
      # NOTE: @initialize_opts is passed to the internal non-sharded connection pools when they are created.
      @initialize_opts = opts.dup
      @initialize_opts.delete(:servers)
      @initialize_opts.delete(:servers_hash)
      @initialize_block = block

      @server_ids = opts[:servers_hash] || Hash.new(:default)
      @server_pools = {}
      add_servers([:default] + (opts[:servers] ? opts[:servers].keys : []))
    end

    # Adds new servers to the connection pool. Primarily used in conjunction with master/slave
    # or shard configurations. Allows for dynamic expansion of the potential slaves/shards
    # at runtime. servers argument should be an array of symbols.
    def add_servers(servers)
      servers.each do |server|
        next if @server_ids.has_key?(server)

        @server_ids[server] = server
        @server_pools[server] = ::Sequel::Mysql2::Synchrony::ConnectionPool.new(@initialize_opts) do
          @initialize_block.call(server) if @initialize_block
        end
      end
    end

    # Remove servers from the connection pool. Primarily used in conjunction with master/slave
    # or shard configurations.  Similar to disconnecting from all given servers,
    # except that after it is used, future requests for the server will use the
    # :default server instead.
    def remove_servers(servers)
      raise(Sequel::Error, "cannot remove default server") if servers.include?(:default)
      pools_to_disconnect = []
      servers.each do |server|
        pool = @server_pools.delete(server)
        pools_to_disconnect << pool if pool
      end
      pools_to_disconnect.each { |pool| pool.disconnect }
    end

    # The total number of connections opened for the given server, should
    # be equal to available_connections.length + allocated.length.  Nonexistent
    # servers will return the created count of the default server.
    def size(server = :default)
      @server_pools[@server_ids[server]].size
    end

    # Removes all connections currently available on all servers, optionally
    # yielding each connection to the given block. This method has the effect of
    # disconnecting from the database, assuming that no connections are currently
    # being used.  If connections are being used, they are scheduled to be
    # disconnected as soon as they are returned to the pool.
    #
    # Once a connection is requested using #hold, the connection pool
    # creates new connections to the database. Options:
    # * :server - Should be a symbol specifing the server to disconnect from,
    #   or an array of symbols to specify multiple servers. If not specified,
    #   then all servers are disconnected.
    def disconnect(opts = {}, &block)
      servers_to_disconnect = @server_pools.keys
      servers_to_disconnect &= Array(opts[:servers]) if opts[:servers]
      servers_to_disconnect.each do |server_id|
        @server_pools[server_id].disconnect(opts, &block)
      end
    end

    # Chooses the first available connection to the given server, or if none are
    # available, creates a new connection.  Passes the connection to the supplied
    # block:
    #
    #   pool.hold {|conn| conn.execute('DROP TABLE posts')}
    #
    # Pool#hold is re-entrant, meaning it can be called recursively in
    # the same thread without blocking.
    #
    # If no connection is immediately available and the pool is already using the maximum
    # number of connections, Pool#hold will block until a connection
    # is available or the timeout expires.  If the timeout expires before a
    # connection can be acquired, a Sequel::PoolTimeout is
    # raised.
    def hold(server = :default, &block)
      @server_pools[@server_ids[server]].hold(nil, &block)
    end

    # Return an array of symbols for servers in the connection pool.
    def servers
      @server_ids.keys
    end
  end
end
