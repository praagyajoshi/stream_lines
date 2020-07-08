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
        # Force encoding to UTF-16 and then back to UTF-8
        # and replace all invalid and undefined characters
        # to avoid raising an invalid byte sequence error.
        # This double conversion is required because it forces
        # an actual encoding conversion which does not happen
        # when calling encoding to UTF-8 directly if the string
        # is already encoded in UTF-8.
        encoded_chunk = chunk.encode!(
          'UTF-16',
          'UTF-8',
          :invalid => :replace,
          :undef => :replace,
          :replace => ''
        )
        encoded_chunk.encode!('UTF-8', 'UTF-16')
        lines = encoded_chunk.split($INPUT_RECORD_SEPARATOR, -1)

        if lines.length > 1
          @buffer.rewind

          # To be extra cautious, encode the buffer to UTF-8 as well
          buffer_string = HTTParty::TextEncoder.new(
            @buffer.read,
            content_type: ';charset=utf-8'
          ).call
          lines.first.prepend(buffer_string)
          @buffer = StringIO.new
        end

        @buffer << lines.pop
        lines
      end
    end
  end
end
