require 'sneakers/queue'
require 'sneakers/support/utils'
require 'sneakers/metrics/newrelic_metrics'
require 'newrelic_rpm'
require 'timeout'

module Sneakers
  module Worker
    attr_reader :queue, :id, :opts
    attr_accessor :retry_count, :args

    # For now, a worker is hardly dependant on these concerns
    # (because it uses methods from them directly.)
    include Concerns::Logging
    include Concerns::Metrics
    include ::NewRelic::Agent::Instrumentation::ControllerInstrumentation

    def initialize(queue = nil, pool = nil, opts = {})
      l = Logger.new(Rails.root.join("log", "sneakers_called.log"))
      l.info "initialize called with queue = #{queue}, pool = #{pool}, opts = #{opts}"

      opts = opts.merge(self.class.queue_opts || {})
      queue_name = self.class.queue_name
      opts = Sneakers::CONFIG.merge(opts)

      @should_ack =  opts[:ack]
      @timeout_after = opts[:timeout_job_after]
      @pool = pool || Thread.pool(opts[:threads]) # XXX config threads
      @call_with_params = respond_to?(:work_with_params)

      @queue = queue || Sneakers::Queue.new(
        queue_name,
        opts
      )

      @opts = opts
      @id = Utils.make_worker_id(queue_name)
    end

    def ack!; :ack end
    def reject!; :reject; end
    def requeue!; :requeue; end

    def work(*args)
      "::#{self.class.name.demodulize}".constantize.new.work(*args)
    end

    def publish(msg, opts)
      to_queue = opts.delete(:to_queue)
      opts[:routing_key] ||= to_queue
      return unless opts[:routing_key]
      @queue.exchange.publish(msg, opts)
    end

    def do_work(delivery_info, metadata, msg, handler)
      worker_trace "Working off: #{msg}"

      @pool.process do
        res = nil
        error = nil

        begin
          metrics.increment("work.#{self.class.name}.started")
          Timeout.timeout(@timeout_after) do
            metrics.timing("work.#{self.class.name}.time") do
              if @call_with_params
                res = work_with_params(msg, delivery_info, metadata)
              elsif handler.class.name == 'Sneakers::Handlers::Sidekiq'
                args = JSON.parse(msg)
                perform_action_with_newrelic_trace(name: 'work', category: 'OtherTransaction/SneakersJob', class_name: self.class.name, job_arguments: args) do
                  res = work(*args) # override to support *args, removing other work method
                end
              else
                res = work(msg)
              end
            end
          end
        rescue Timeout::Error
          res = :timeout
          worker_error('timeout')
        rescue Exception => ex # retrying on every exception, not only runtime ones
          res = :error
          error = ex
          worker_error('unexpected error', ex)
        end

        if @should_ack

          if res == :ack
            # note to future-self. never acknowledge multiple (multiple=true) messages under threads.
            handler.acknowledge(delivery_info, metadata, msg)
          elsif res == :timeout
            handler.timeout(delivery_info, metadata, msg)
          elsif res == :error
            handler.error(delivery_info, metadata, msg, error)
          elsif res == :reject
            handler.reject(delivery_info, metadata, msg)
          elsif res == :requeue
            handler.reject(delivery_info, metadata, msg, true)
          else
            handler.noop(delivery_info, metadata, msg)
          end
          metrics.increment("work.#{self.class.name}.handled.#{res || 'noop'}")
        end

        metrics.increment("work.#{self.class.name}.ended")
      end #process
    end

    def stop
      worker_trace "Stopping worker: unsubscribing."
      @queue.unsubscribe
      worker_trace "Stopping worker: I'm gone."
    end

    def run
      worker_trace "New worker: subscribing."
      @queue.subscribe(self)
      worker_trace "New worker: I'm alive."
    end

    # Construct a log message with some standard prefix for this worker
    def log_msg(msg)
      "[#{@id}][#{Thread.current}][#{@queue.name}][#{@queue.opts}] #{msg}"
    end

    # Helper to log an error message with an optional exception
    def worker_error(msg, exception = nil)
      s = log_msg(msg)
      if exception
        s += " [Exception error=#{exception.message.inspect} error_class=#{exception.class}"
        s += " backtrace=#{exception.backtrace.take(50).join(',')}" unless exception.backtrace.nil?
        s += "]"
      end
      logger.error(s)
    end

    def worker_trace(msg)
      logger.debug(log_msg(msg))
    end

    def self.included(base)
      base.extend ClassMethods
    end

    module ClassMethods
      attr_reader :queue_opts
      attr_reader :queue_name

      def from_queue(q, opts={})
        @queue_name = q.to_s
        @queue_opts = opts
      end

      # for the retries on sidekiq
      def enqueue(*msg, retry_count: 0)
        publisher.publish(msg.to_json, to_queue: @queue_name, headers: { retry_count: retry_count, sidekiq_class: self.name })
      end

      # for the retries on sidekiq
      def send_to_rabbitmq(jid)
        return false if jid.blank?
        enqueue(*args, retry_count: (retry_count ? retry_count.to_i + 1 : 0))
        true
      end

      private

      def publisher
        @publisher ||= Sneakers::Publisher.new(exchange: @queue_opts[:exchange].to_s)
      end
    end
  end
end
