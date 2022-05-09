module Arel
  module Visitors
    class SQLServer < Arel::Visitors::ToSql
      def table_From_Statement o
        puts("GET TABLE #{Base64.encode64(Marshal.dump(o))}")
        core = o.cores.first
        if Arel::Table === core.from
          core.from
        elsif Arel::Nodes::SqlLiteral === core.from
          Arel::Table.new(core.from)
        elsif Arel::Nodes::JoinSource === core.source
          Arel::Nodes::SqlLiteral === core.source.left ? Arel::Table.new(core.source.left, @engine) : core.source.left.left
        end
      end

      def primary_Key_From_Table t
        return unless t
        puts("TABLE NAME IS #{t.name}")

        primary_keys = @connection.schema_cache.primary_keys(t.name)
        column_name = nil
        case primary_keys
        when NilClass
          column_name = @connection.schema_cache.columns_hash(t.name).first.try(:second).try(:name)
          string_marshal = Marshal.dump(@connection.schema_cache)
          puts("COLUMN NAME IF NIL: #{column_name} AND STRING FOR MARSHAL: '#{Base64.encode64(string_marshal)}'" )
        when String
          column_name = primary_keys
        when Array
          candidate_columns = @connection.schema_cache.columns_hash(t.name).slice(*primary_keys).values
          candidate_column = candidate_columns.find(&:is_identity?)
          candidate_column ||= candidate_columns.first
          column_name = candidate_column.try(:name)
        end
        puts("COLUMN NAME AS RESULT: #{column_name}")
        column_name ? t[column_name] : nil
      end
    end
  end
end
