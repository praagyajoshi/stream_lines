# frozen_string_literal: true

require 'httparty'

require 'stream_lines/error'

module StreamLines
  module Reading
    class Stream
      include Enumerable
      include HTTParty

      raise_on 400..599

      def initialize(url)
        @url = url
        @buffer = StringIO.new
      end

      def each(&block)
        stream_lines(&block)
      rescue HTTParty::Error => e
        raise Error, "Failed to download #{url} with code: #{e.response.code}"
      end

      private

      attr_reader :url

      def stream_lines(&block)
        self.class.get(url, stream_body: true) do |chunk|
          lines = extract_lines(chunk)
          lines.each { |line| block.call(line) }
        end

        @buffer.rewind
        block.call(@buffer.read) if @buffer.size.positive?
      end

      def extract_lines(chunk)
        # Force encoding to UTF-8 and replace all invalid and undefined characters
        # to avoid raising an invalid byte sequence error
        encoded_chunk = chunk.encode('utf-8', :invalid => :replace, :undef => :replace)
        lines = encoded_chunk.split($INPUT_RECORD_SEPARATOR, -1)

        if lines.length > 1
          @buffer.rewind
          lines.first.prepend(@buffer.read)
          @buffer = StringIO.new
        end

        @buffer << lines.pop
        lines
      end
    end
  end
end
