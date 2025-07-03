module ActiveRecord
  module ConnectionAdapters
    module ADBC
      class SchemaCreation < ConnectionAdapters::SchemaCreation
        private def quote_string(s)
          @conn.quote_string(s)
        end

        private def sequence_name(column)
          "sequence_#{column.table.name}_#{column.name}"
        end

        private def quoted_sequence_name(column)
          quote_table_name(sequence_name(column))
        end

        def visit_ColumnDefinition(o)
          sql = super
          if o.type == :primary_key and @conn.backend == "duckdb"
            sql << " DEFAULT NEXTVAL('#{quote_string(sequence_name(o))}')"
          end
          sql
        end

        def visit_TableDefinition(o)
          o.columns.each do |column|
            column.singleton_class.define_method(:table) do
              o
            end
          end
          sql = super
          if @conn.backend == "duckdb"
            o.columns.each do |column|
              if column.type == :primary_key
                s = +"CREATE SEQUENCE"
                s << " IF NOT EXISTS" if o.if_not_exists
                s << " #{quoted_sequence_name(column)}"
                s << "; #{sql}"
                sql = s
              end
            end
          end
          sql
        end
      end
    end
  end
end
