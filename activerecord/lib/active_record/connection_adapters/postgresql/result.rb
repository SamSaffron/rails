# Just like ActiveRecord::Result, except that avoids needless copying
module ActiveRecord
  module ConnectionAdapters
    class PostgreSQLAdapter::Result

      IDENTITY_TYPE = Class.new { def type_cast(v); v; end }.new # :nodoc:

      def identity_type # :nodoc:
        IDENTITY_TYPE
      end

      class ArrayRow
        def initialize(pg_result, result, row)
          @pg_result = pg_result
          @result = result
          @row = row
        end

        def first
          self[0]
        end

        def [](col)
          @pg_result.getvalue(@row, col)
        end

        def to_ary
          ary = []
          i = 0
          columns = @pg_result.num_fields
          while i < columns
            ary << @pg_result.getvalue(@row, i)
            i += 1
          end
          ary
        end

        def to_sym
          #puts caller.join("\n")
          exit
        end

        def method_missing(meth, *args, &block)
          puts "meth #{meth}"
        end
      end

      class Rows
        def initialize(pg_result, result)
          @pg_result = pg_result
          @result = result
        end

        def first
          @pg_result.num_tuples == 0 ? nil : self[0]
        end

        def map
          i = 0
          rows = @pg_result.num_tuples
          r = []
          while i < rows
            r << yield(self[i])
            i += 1
          end

          r
        end

        def [](row)
          ArrayRow.new(@pg_result, @result, row)
        end
      end

      class Row
        def initialize(pg_result, row, result)
          @pg_result = pg_result
          @row = row
          @result = result
        end

        def first
          @pg_result.getvalue(@row, 0)
        end

        def []=(name,val)
          #puts "#{name} = #{val}"
          val
        end

        def [](name)
          if col = @result.columns[name]
            @pg_result.getvalue(@row, col)
          end
        end

        def values
          ary = []
          i = 0
          columns = @pg_result.num_fields
          while i < columns
            ary << @pg_result.getvalue(@row, i)
            i += 1
          end
          ary
        end

        def fetch(name)
          if col = @result.columns[name]
            @pg_result.getvalue(@row, col)
          else
            yield
          end
        end

        def key?(name)
          !!@result.columns[name]
        end

        def any?
          true
        end

        alias :has_key? :key?

      end

      include Enumerable

      def initialize(pg_result, adapter)
        @pg_result = pg_result
        @adapter = adapter
        @column_types = nil
      end

      def rows
        @rows ||= Rows.new(@pg_result, self)
      end

      def each
        if block_given?
          total = @pg_result.ntuples
          row = 0
          while row < total
            yield Row.new(@pg_result, row, self)
            row += 1
          end
        else
          boom
        end
      end

      def columns
        @columns ||= begin

            hash = {}
            i = 0
            count = @pg_result.num_fields
            while i < count
              hash[@pg_result.fname(i)] = i
              i += 1
            end
            hash
        end
      end

      def column_type(name)
        column_types[name] || identity_type
      end

      def column_types
        @column_types ||= begin
            types = {}
            fields = @pg_result.fields
            i = 0
            count = fields.count
            while i < count
              ftype = @pg_result.ftype i
              fmod  = @pg_result.fmod i
              fname = fields[i]
              types[fname] = @adapter.get_oid_type(ftype, fmod, fname)
              i += 1
            end
            types
        end
      end

    end
  end
end
