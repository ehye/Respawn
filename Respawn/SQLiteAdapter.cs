using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Respawn.Graph;

namespace Respawn;

public class SQLiteAdapter : IDbAdapter
{
    private const char QuoteCharacter = '`';

    public string BuildTableCommandText(RespawnerOptions options)
    {
        string commandText = "SELECT t.name, t.tbl_name FROM sqlite_schema t WHERE type = 'table'";

        if (options.TablesToIgnore.Any())
        {
            var tablesToIgnoreGroups = options.TablesToIgnore
                .GroupBy(
                    t => t.Schema != null,
                    t => t,
                    (hasSchema, tables) => new { HasSchema = hasSchema, Tables = tables })
                .ToList();
            foreach (var tableGroup in tablesToIgnoreGroups)
            {
                if (tableGroup.HasSchema)
                {
                    var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Schema}.{table.Name}'"));

                    commandText += " AND t.name + '.' + t.tbl_name NOT IN (" + args + ")";
                }
                else
                {
                    var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Name}'"));

                    commandText += " AND t.tbl_name NOT IN (" + args + ")";
                }
            }
        }
        if (options.TablesToInclude.Any())
        {
            var tablesToIncludeGroups = options.TablesToInclude
                .GroupBy(
                    t => t.Schema != null,
                    t => t,
                    (hasSchema, tables) => new { HasSchema = hasSchema, Tables = tables })
                .ToList();
            foreach (var tableGroup in tablesToIncludeGroups)
            {
                if (tableGroup.HasSchema)
                {
                    var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Schema}.{table.Name}'"));

                    commandText += " AND t.name + '.' + t.tbl_name IN (" + args + ")";
                }
                else
                {
                    var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Name}'"));

                    commandText += " AND t.tbl_name IN (" + args + ")";
                }
            }
        }
        if (options.SchemasToExclude.Any())
        {
            var args = string.Join(",", options.SchemasToExclude.Select(t => $"'{t}'"));

            commandText += " AND t.name NOT IN (" + args + ")";
        }
        else if (options.SchemasToInclude.Any())
        {
            var args = string.Join(",", options.SchemasToInclude.Select(t => $"'{t}'"));

            commandText += " AND t.name IN (" + args + ")";
        }

        return commandText;
    }

    public string BuildTemporalTableCommandText(RespawnerOptions options) => throw new System.NotImplementedException();

    public string BuildRelationshipCommandText(RespawnerOptions options)
    {
        var commandText = """
SELECT 
	m.name sch_name,
	m.tbl_name,
	p."table" schema,
	p."table",
	replace( substr(m.sql, 1, instr(m.sql, '" FOREIGN KEY ')-1), substr(m.sql, 1, instr(m.sql, 'FK_')-1),'') constraint_name
FROM
	sqlite_master m
JOIN pragma_foreign_key_list(m.name) p ON m.name != p."table"
""";

        var whereText = new List<string>();

        if (options.TablesToIgnore.Any())
        {
            var tablesToIgnoreGroups = options.TablesToIgnore
                .GroupBy(
                    t => t.Schema != null,
                    t => t,
                    (hasSchema, tables) => new
                    {
                        HasSchema = hasSchema,
                        Tables = tables
                    })
                .ToList();
            foreach (var tableGroup in tablesToIgnoreGroups)
            {
                if (tableGroup.HasSchema)
                {
                    var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Schema}.{table.Name}'"));

                    whereText.Add("sch_name + '.' + tbl_name NOT IN (" + args + ")");
                }
                else
                {
                    var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Name}'"));

                    whereText.Add("tbl_name NOT IN (" + args + ")");
                }
            }
        }
        if (options.TablesToInclude.Any())
        {
            var tablesToIncludeGroups = options.TablesToInclude
                .GroupBy(
                    t => t.Schema != null,
                    t => t,
                    (hasSchema, tables) => new
                    {
                        HasSchema = hasSchema,
                        Tables = tables
                    })
                .ToList();
            foreach (var tableGroup in tablesToIncludeGroups)
            {
                if (tableGroup.HasSchema)
                {
                    var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Schema}.{table.Name}'"));

                    whereText.Add("name + '.' + TABLE_NAME IN (" + args + ")");
                }
                else
                {
                    var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Name}'"));

                    whereText.Add("tbl_name IN (" + args + ")");
                }
            }
        }
        if (options.SchemasToExclude.Any())
        {
            var args = string.Join(",", options.SchemasToExclude.Select(t => $"'{t}'"));
            whereText.Add("sch_name NOT IN (" + args + ")");
        }
        else if (options.SchemasToInclude.Any())
        {
            var args = string.Join(",", options.SchemasToInclude.Select(t => $"'{t}'"));
            whereText.Add("sch_name IN (" + args + ")");
        }

        if (whereText.Any())
            commandText += $" WHERE {string.Join(" AND ", whereText.ToArray())}";
        return commandText;
    }

    public string BuildDeleteCommandText(GraphBuilder graph)
    {
        var builder = new StringBuilder();

        builder.AppendLine("PRAGMA foreign_keys = OFF;");
        foreach (var table in graph.ToDelete)
        {
            builder.AppendLine($"DELETE FROM {table.Name};");
        }
        builder.AppendLine("PRAGMA foreign_keys = ON;");

        return builder.ToString();
    }

    public string BuildReseedSql(IEnumerable<Table> tablesToDelete)
    {
        var builder = new StringBuilder();
        foreach (var table in tablesToDelete)
        {
            builder.AppendLine($"PRAGMA automatic_index = ON;");
        }

        return builder.ToString();
    }

    public string BuildTurnOffSystemVersioningCommandText(IEnumerable<TemporalTable> tablesToTurnOffSystemVersioning) => throw new System.NotImplementedException();

    public string BuildTurnOnSystemVersioningCommandText(IEnumerable<TemporalTable> tablesToTurnOnSystemVersioning) => throw new System.NotImplementedException();

    public Task<bool> CheckSupportsTemporalTables(DbConnection connection)
    {
        return Task.FromResult(false);
    }
}