require 'sidekiq/middleware/server/retry_jobs'
require 'sidekiq/cli'

module Sneakers
  module Handlers
    class Sidekiq
      def initialize(channel, queue, opts)
        @channel = channel
        @opts = opts

        @max_retries = @opts[:retry] || 25
        @log_exceptions_after = @opts[:log_exceptions_after] || 0
      end

      def acknowledge(hdr, props, msg)
        @channel.acknowledge(hdr.delivery_tag, false)
      end

      def reject(hdr, props, msg, requeue = false)
        if requeue
          @channel.reject(hdr.delivery_tag, requeue)
        else
          handle_retry(hdr, props, msg, :reject)
        end
      end

      def error(hdr, props, msg, err)
        handle_retry(hdr, props, msg, err)
      end

      def timeout(hdr, props, msg)
        handle_retry(hdr, props, msg, :timeout)
      end

      def noop(hdr, props, msg)
        acknowledge(hdr, props, msg)
      end

      def handle_retry(hdr, props, msg, reason)
        args = JSON.parse(msg)
        num_attempts = retry_count(props[:headers])
        queue_class = sidekiq_queue(props[:headers])
        begin
          if num_attempts >= @log_exceptions_after
            NewRelic::Agent.notice_error(reason, metric: "OtherTransaction/SneakersJob/#{queue_class}#work")
            Honeybadger.notify_or_ignore(reason)
          end
          ::Sidekiq::Middleware::Server::RetryJobs.new.send(:attempt_retry, queue_class.new, {'retry' => @max_retries, 'retry_count' => num_attempts, 'class' => queue_class, 'args' => args, 'jid' => SecureRandom.hex(12), 'failed_at' => Time.now}, 'default', reason)
        rescue Exception
        end
        @channel.acknowledge(hdr.delivery_tag, false)
      end
      private :handle_retry

      def sidekiq_queue(headers)
        headers['sidekiq_class'].constantize
      end
      private :sidekiq_queue

      def retry_count(headers)
        headers['retry_count'].to_i
      end
      private :retry_count
    end
  end
end
