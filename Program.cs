using System;
using Npgsql;

namespace Driver
{
    public class CosmosPostgresCRUD
    {
        static void Main(string[] args)
        {
            var connStr = new NpgsqlConnectionStringBuilder("Server = c-csharp-postgres-sdk.f6ffz4zwmfixzj.postgres.cosmos.azure.com; Database = citus; Port= 5432; User Id = citus; Password = kemboi590@; Ssl Mode = Require; Pooling = true; Minimum Pool Size = 0; Maximum Pool Size = 50;");

            using (var conn = new NpgsqlConnection(connStr.ToString()))
            {
                Console.Out.WriteLine("Opening connection");
                conn.Open();
                using (var command = new NpgsqlCommand("DROP TABLE IF EXISTS pharmacy;", conn))
                {
                    command.ExecuteNonQuery(); //ExecuteNoQuery is used to execute the command
                    Console.Out.WriteLine("Finished dropping table (if existed)");
                }
                using (var command = new NpgsqlCommand("CREATE TABLE pharmacy (pharmacy_id integer ,pharmacy_name text,city text,state text,zip_code integer);", conn))
                {
                    command.ExecuteNonQuery();
                    Console.Out.WriteLine("Finished creating table");
                }
                using (var command = new NpgsqlCommand("CREATE INDEX idx_pharmacy_id ON pharmacy(pharmacy_id);", conn))
                {
                    command.ExecuteNonQuery();
                    Console.Out.WriteLine("Finished creating index");
                }
                using (var command = new NpgsqlCommand("INSERT INTO  pharmacy  (pharmacy_id,pharmacy_name,city,state,zip_code) VALUES (@n1, @q1, @a, @b, @c)", conn))
                {
                    command.Parameters.AddWithValue("n1", 0);
                    command.Parameters.AddWithValue("q1", "Target");
                    command.Parameters.AddWithValue("a", "Sunnyvale");
                    command.Parameters.AddWithValue("b", "California");
                    command.Parameters.AddWithValue("c", 94001);

                    int nRows = command.ExecuteNonQuery();
                    Console.Out.WriteLine(String.Format("Number of rows inserted={0}", nRows));
                }

                // 2nd Part - Creating Distributed Tables
                using (var command = new NpgsqlCommand("select create_distributed_table('pharmacy','pharmacy_id');", conn))
                {
                    command.ExecuteNonQuery();
                    Console.Out.WriteLine("Finished distributing the table");
                }

                // 3rd Part - Read Data
                using (var command = new NpgsqlCommand("SELECT * FROM pharmacy", conn))
                {
                    var reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        Console.WriteLine(
                            string.Format(
                                "Reading from table = ({0}, {1}, {3}, {4})",
                                reader.GetInt32(0).ToString(),
                                reader.GetString(1),
                                reader.GetString(2),
                                reader.GetString(3),
                                reader.GetInt32(4).ToString()
                            )
                        );
                    }
                    reader.Close();
                }

                // 3rd Part - Update data
                using (var command = new NpgsqlCommand("UPDATE pharmacy SET city = @q WHERE pharmacy_id = @n", conn))
                {
                    command.Parameters.AddWithValue("n", 0);
                    command.Parameters.AddWithValue("q", "Nairobi");
                    int nRows = command.ExecuteNonQuery();
                    Console.Out.WriteLine(String.Format("Number of rows updated={0}", nRows));
                }

                // 4th Part - delete data
                using (var command = new NpgsqlCommand("DELETE FROM pharmacy WHERE pharmacy_id = @n", conn))
                {
                    command.Parameters.AddWithValue("n", 0);
                    int nRows = command.ExecuteNonQuery();
                    Console.Out.WriteLine(String.Format("Number of rows deleted={0}", nRows));
                }

                // Copying command to load data from a file
                 String sDestinationSchemaAndTableName = "pharmacy"; 
                 String sFromFilePath = "C:\\Users\\KEMBOI\\Documents\\pharmacies.csv";

                NpgsqlCommand cmd = new NpgsqlCommand();

                if(File.Exists(sFromFilePath))
                {
                    using (var writer = conn.BeginTextImport("COPY " + sDestinationSchemaAndTableName + " FROM STDIN WITH(FORMAT CSV, HEADER true,NULL '');"))
                    {
                        foreach (String sLine in File.ReadLines(sFromFilePath))
                        {
                            writer.WriteLine(sLine);
                        }
                    }
                    Console.WriteLine("Data loaded successfully");
                }
            }
        }
    }

}