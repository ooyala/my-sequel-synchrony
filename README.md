# my-sequel-synchrony

A fiber-aware MySQL adapter for [Sequel](https://github.com/jeremyevans/sequel) that works with
[em-synchrony](https://github.com/igrigorik/em-synchrony).
Uses the Mysql2::EM::Client client with the em-synchrony extensions.

Note: this code is in an early stage of active development and has not been battle-tested in any
production environments. For now, use at your own risk, and please report any bugs you find!

## Installation

Add this line to your application's Gemfile:

    gem 'my-sequel-synchrony'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install my-sequel-synchrony

## Usage

Inside an `EM.synchrony` block or a synchrony-enabled web server (such as
[sinatra-synchrony](https://github.com/kyledrake/sinatra-synchrony)), simply create the
Sequel connection as usual, and specify the `:adapter => :mysql2synchrony` option:

    @database = Sequel.connect(
      :adapter => :mysql2synchrony,
      :hostname => ...,
      ...)

And use it just like you would normally use a Sequel database object! If you have multiple DB shards
(either master/slaves, or data sharded across multiple masters), you can specify them by passing in
a `:servers` option just like you would with any other Sequel adapter that supports sharding (see
the [Sequel sharding doc](https://github.com/jeremyevans/sequel/blob/master/doc/sharding.rdoc)).

You can even run queries in parallel with `FiberIterator`:

    require 'em-synchrony/fiber_iterator'

    @shard_ids = [ :master, :slave1, :slave2 ]

    # Assuming we're in a sinatra-synchrony webapp, this will run a simple "am i connected?" check
    # on every shard in parallel and return 200 or 500 depending on the result.
    get "/healthz" do
      shard_errors = {}
      EM::Synchrony::FiberIterator.new(@shard_ids, @shard_ids.size).each do |shard_id|
        begin
          @db.run("select 1;", :server => shard_id)
        rescue Exception => e
          shard_errors[shard_id] = e
        end
      end
      halt 200, "All DB shards ok" if shard_errors.empty?

      message = "#{shard_errors.size} DB shards had errors:\n"
      message += shard_errors.map { |shard_id, error| "#{shard_id}: #{e.message}" }.join("\n")
      halt 500, message
    end

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Added some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

## Authors

* Bo Chen (bochen.chen@gmail.com)
* Ilya Maykov (ivmaykov@gmail.com)
