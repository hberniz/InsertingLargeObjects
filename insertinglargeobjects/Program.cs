using System;
using System.Data;
using System.IO;
using System.Collections.Generic;
using Npgsql;
using System.Linq;

namespace insertinglargeobjects
{
    class Program
    {
        static String fileLocation = "C://temp/theData";

        static NpgsqlConnection originalConnection = new NpgsqlConnection("Server=secondcasetest.postgres.database.azure.com;Database=test;Port=5432;User Id=humberto@secondcasetest;Password=Pa$$w0rd;");
        static NpgsqlConnection restoredServerConnection = new NpgsqlConnection("Server=humbertocasetest.postgres.database.azure.com;Database=test;Port=5432;User Id=humberto@humbertocasetest;Password=Pa$$w0rd;");

        static void Main(string[] args)
        {
            uint startOid = uint.Parse(args[0]);
            uint endOid = uint.Parse(args[1]);

            System.Console.WriteLine("Start oid value: ");
            System.Console.WriteLine(startOid);
            System.Console.WriteLine("End oid value: ");
            System.Console.WriteLine(endOid);




            //adding values of the oids into an array
            uint[] loid = new uint[endOid - startOid + 1];
            for (int j=0; j<loid.Length; j++)
            {
                loid[j] = startOid++;
            }

            for (int i = 0; i < loid.Length; i++)
            {
                if (doesNotExist(loid[i]))
                {
                    DownloadData(loid[i], fileLocation + loid[i]+"", restoredServerConnection);
                    InsertData(loid[i], fileLocation + loid[i] + "", originalConnection);
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
            if (restoredServerConnection.State == ConnectionState.Closed)
            {
                restoredServerConnection.Open();
            }
            NpgsqlLargeObjectManager manager = new NpgsqlLargeObjectManager(originalConnection);

            NpgsqlLargeObjectManager restoreManager = new NpgsqlLargeObjectManager(restoredServerConnection);

            using (var transaction = originalConnection.BeginTransaction())
            {
                try
                {
                    manager.OpenReadWrite(id);
                }
                catch (Exception exist)
                {
                    try
                    {
                        restoreManager.OpenReadWrite(id);
                        //if it hits this, this means the Oid doesnt
                    } catch (Exception doesntExistInRestoredServer)
                    {
                        Console.WriteLine("Doesn't exist in the Restored server, won't insert OID: "+ id + "");
                        return false;
                    }

                    Console.WriteLine("OID" + id + "doesn't exist in orignal server, will begin transfer operation");
                    return true;
                }
                finally
                {
                    //Save the changes to the object
                    transaction.Commit();
                }


            }
            Console.WriteLine("OID" + id + " already exists, will not transfer data");

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
                using (var stream = manager.OpenReadWrite(id))
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

