from engine import Engine

engine = Engine()

# engine.query("CREATE TABLE test_table (first_name TEXT, last_name TEXT, age INT) WITH (FORMAT=iceberg)")
# engine.query("INSERT INTO test_table (first_name, last_name, age) VALUES ('nicolas', 'jabot', 27)")
# engine.query("insert into test_table (first_name, last_name, age) values('gary', 'clark', 34), ('rick', 'vergunst', 28)")
engine.query("SELECT * FROM test_table")