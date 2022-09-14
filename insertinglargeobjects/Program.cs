using System;
using System.Data;
using System.IO;
using System.Collections.Generic;
using Npgsql;



namespace insertinglargeobjects
{
    class Program
    {
        static String fileLocation = "C://temp/theData";

        static NpgsqlConnection originalConnection = new NpgsqlConnection("Server=humbertocasetest.postgres.database.azure.com;Database=test;Port=5432;User Id=humberto@humbertocasetest;Password=Pa$$w0rd;");
        static NpgsqlConnection restoredServerConnection = new NpgsqlConnection("Server=humbertocasetest.postgres.database.azure.com;Database=test;Port=5432;User Id=humberto@humbertocasetest;Password=Pa$$w0rd;");

        static void Main(string[] args)
        {

            //need to implement a way to add a list of oid.
            uint[] loid = new uint[1000];

            for (int i = 0; i < loid.Length; i++)
            {
                if (doesNotExist(loid[i]))
                {
                    DownloadData(loid[i], fileLocation + loid[i]+"", restoredServerConnection);
                    InsertData(loid[i], fileLocation, originalConnection);
                }
            }
        }




        //this will check the original server to see if the oid exists
        private static bool doesNotExist(uint id)
        {
            if (originalConnection.State == ConnectionState.Closed)
            {
                originalConnection.Open();
            }
            NpgsqlLargeObjectManager manager = new NpgsqlLargeObjectManager(originalConnection);

            using (var transaction = originalConnection.BeginTransaction())
            {
                try
                {
                    manager.OpenReadWrite(id);
                }
                catch (Exception exist)
                {
                    Console.WriteLine("Doesn't exist, error message: " + exist);
                    return true;
                }
                finally
                {
                    //Save the changes to the object
                    transaction.Commit();
                }


            }
            return false;
        }





        private static void DownloadData(uint id, string filelocation, NpgsqlConnection connection)
        {
            if (connection.State == ConnectionState.Closed)
            {
                connection.Open();
            }
            //the code below can be found here: https://www.npgsql.org/doc/large-objects
            // Retrieve a Large Object Manager for this connection
            NpgsqlLargeObjectManager manager = new NpgsqlLargeObjectManager(connection);

            // Reading and writing Large Objects requires the use of a transaction
            using (var transaction = connection.BeginTransaction())
            {
                // Open the file for reading and writing
                using (var stream = manager.OpenReadWrite(id))
                {
                    //creating a byte array with max length being the default value of openreadwrite operation (4MB)
                    var buf2 = new byte[stream.Length];

                    //converting LargeObjectStream to a byte array
                    stream.Read(buf2, 0, buf2.Length);

                    //converting byte array to file and then storing file locally
                    File.WriteAllBytes(filelocation, buf2);
                }
                //Save the changes to the object
                transaction.Commit();
            }
        }





        private static void InsertData(uint id, string filelocation, NpgsqlConnection connection)
        {
            if (connection.State == ConnectionState.Closed)
            {
                connection.Open();
            }


            //the code below can be found here: https://www.npgsql.org/doc/large-objects

            // Retrieve a Large Object Manager for this connection
            NpgsqlLargeObjectManager manager = new NpgsqlLargeObjectManager(connection);

            // Create a new empty file, returning the identifier to later access it, if specified inside create, will create a specific id in the metadata
            try
            {
                uint oid = manager.Create(id);
            } catch (Exception idExists)
            {
                Console.WriteLine("Trying to add Duplicate ID: " + idExists);
                return;
            }

            // Reading and writing Large Objects requires the use of a transaction
            using (var transaction = connection.BeginTransaction())
            {

                // Open the file for reading and writing
                using (var stream = manager.OpenReadWrite(oid))
                {
                    //retrieving file that that needs to be inserted
                    FileStream fileStream = new FileStream(filelocation, FileMode.Open, FileAccess.Read, FileShare.None);
                    var buf = new byte[fileStream.Length];

                    //inserting file data into byte
                    fileStream.Read(buf, 0, buf.Length);

                    //writes the bytes into largeobject stream
                    stream.Write(buf, 0, buf.Length);
                    stream.Seek(0, System.IO.SeekOrigin.Begin);
                }
                //Save the changes to the object
                transaction.Commit();
            }

        }






    }
}

