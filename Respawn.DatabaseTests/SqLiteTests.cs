using Microsoft.Data.Sqlite;
using NPoco;
using Shouldly;
using System;
using System.Linq;
using System.Threading.Tasks;
using Respawn.Graph;
using Xunit.Abstractions;

namespace Respawn.DatabaseTests
{
    public class SqLiteTests : IDisposable
    {
        private readonly ITestOutputHelper _output;
        private readonly SqliteConnection _connection;
        private readonly IDatabase _database;

        public SqLiteTests(ITestOutputHelper output)
        {
            _output = output;

            const string connString = @"Data Source=database.db";

            _connection = new SqliteConnection(connString);
            _connection.Open();

            _database = new Database(_connection, DatabaseType.SQLite);
        }

        public class Foo
        {
            public int Value { get; set; }
        }
        public class Bar
        {
            public int Value { get; set; }
        }
        public class Bob
        {
            public int Value { get; set; }
            public int FooValue { get; set; }
        }

        [SkipOnCI]
        public async Task ShouldDeleteData()
        {
            _database.Execute("drop table if exists Foo");
            _database.Execute("CREATE TABLE \"Foo\" (\"Value\" INTEGER);");

            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new Foo { Value = i }));

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(100);

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.SQLite
            });
            await checkpoint.ResetAsync(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(0);
        }

        [SkipOnCI]
        public async Task ShouldIgnoreTables()
        {
            _database.Execute("drop table if exists Foo");
            _database.Execute("drop table if exists Bar");
            _database.Execute("create table Foo (Value INTEGER)");
            _database.Execute("create table Bar (Value INTEGER)");
            
            await _database.InsertBulkAsync(Enumerable.Range(0, 100).Select(i => new Foo { Value = i }));
            await _database.InsertBulkAsync(Enumerable.Range(0, 100).Select(i => new Bar { Value = i }));
            
            var checkPoint = await Respawner.CreateAsync(_connection, new RespawnerOptions()
            {
                DbAdapter = DbAdapter.SQLite,
                TablesToIgnore = new Table[] { "Foo" }
            });
            await checkPoint.ResetAsync(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Bar").ShouldBe(0);
        }
        
        [SkipOnCI]
        public async Task ShouldIncludeTables()
        {
            _database.Execute("drop table if exists Foo");
            _database.Execute("drop table if exists Bar");
            _database.Execute("create table Foo (Value INTEGER)");
            _database.Execute("create table Bar (Value INTEGER)");

            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new Foo { Value = i }));
            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new Bar { Value = i }));

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.SQLite,
                TablesToInclude = new Table[] { "Foo" }
            });
            await checkpoint.ResetAsync(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Bar").ShouldBe(100);
        }
        
        [SkipOnCI]
        public async Task ShouldDeleteDataWithRelationships()
        {
            _database.Execute("drop table if exists Bar");
            _database.Execute("drop table if exists Foo");
            _database.Execute("drop table if exists Bob");

            _database.Execute(@"
CREATE TABLE Bob (
  BobValue INTEGER NOT NULL, 
  PRIMARY KEY (BobValue)
)");

            _database.Execute(@"
CREATE TABLE Foo (
  FooValue INTEGER NOT NULL,
  BobValue INTEGER NOT NULL,
  PRIMARY KEY (FooValue),
  -- KEY IX_BobValue (BobValue),
  CONSTRAINT FK_FOO_BOB FOREIGN KEY (BobValue) REFERENCES Bob (BobValue) ON DELETE NO ACTION ON UPDATE NO ACTION
)");

            _database.Execute(@"
CREATE TABLE Bar (
  BarValue INTEGER NOT NULL,
  PRIMARY KEY (BarValue),
  CONSTRAINT FK_BAR_FOO FOREIGN KEY (BarValue) REFERENCES Foo (FooValue) ON DELETE NO ACTION ON UPDATE NO ACTION
)");

            for (var i = 0; i < 100; i++)
            {
                _database.Execute($"INSERT INTO Bob VALUES ({i})");
                _database.Execute($"INSERT INTO Foo VALUES ({i},{i})");
                _database.Execute($"INSERT INTO Bar VALUES ({i})");
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Bar").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Bob").ShouldBe(100);

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.SQLite
            });
            await checkpoint.ResetAsync(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Bar").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Bob").ShouldBe(0);
        }

        [SkipOnCI]
        public async Task ShouldHandleSelfRelationships()
        {
            _database.Execute("DROP table if exists Foo");
            _database.Execute("CREATE TABLE \"Foo\" (\"id\"INTEGER,\"parent_id\"INTEGER,FOREIGN KEY(\"parent_id\") REFERENCES \"Foo\"(\"id\"),PRIMARY KEY(\"id\"));");
            
            _database.Execute("INSERT INTO Foo (id) VALUES (@0)", 1);
            
            for (int i = 1; i < 100; i++)
            {
                _database.Execute("INSERT INTO `foo` VALUES (@0, @1)", i + 1, i);
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM foo").ShouldBe(100);

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.SQLite
            });
            try
            {
                await checkpoint.ResetAsync(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.DeleteSql ?? string.Empty);
                throw;
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM foo").ShouldBe(0);
        }

        [SkipOnCI]
        public async Task ShouldHandleCircularRelationships()
        {
            _database.Execute("DROP TABLE if EXISTS parent");
            _database.Execute("DROP TABLE if EXISTS child");
            _database.Execute("CREATE TABLE parent ( id INTEGER, child_id INTEGER NULL, FOREIGN KEY(child_id) REFERENCES child(parent_id), PRIMARY KEY(id) );");
            _database.Execute("CREATE TABLE child ( id INTEGER, parent_id INTEGER NULL, FOREIGN KEY(parent_id) REFERENCES parent(child_id), PRIMARY KEY(id) );");

            _database.Execute("PRAGMA foreign_keys = OFF;");

            for (int i = 0; i < 100; i++)
            {
                _database.Execute($"INSERT INTO parent VALUES ({i}, null)");
                _database.Execute($"INSERT INTO child VALUES ({i}, null)");
            }

            _database.Execute("update parent set child_id = 0");
            _database.Execute("update child set parent_id = 1");
            
            _database.Execute("PRAGMA foreign_keys = OFF;");
            
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM parent").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM child").ShouldBe(100);

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.SQLite
            });
            await checkpoint.ResetAsync(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM parent").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM child").ShouldBe(0);
        }
        
        public void Dispose()
        {
            _connection.Close();
            _connection.Dispose();
        }
    }
}
