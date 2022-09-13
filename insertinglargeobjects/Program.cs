using System;
using System.Data;
using System.IO;
using System.Collections.Generic;
using Npgsql;



namespace insertinglargeobjects
{
    class Program
    {
        static NpgsqlConnection originalConnection = new NpgsqlConnection("Server=humbertocasetest.postgres.database.azure.com;Database=test;Port=5432;User Id=humberto@humbertocasetest;Password=Pa$$w0rd;");
        static NpgsqlConnection restoredServerConnection = new NpgsqlConnection("Server=humbertocasetest.postgres.database.azure.com;Database=test;Port=5432;User Id=humberto@humbertocasetest;Password=Pa$$w0rd;");

        static void Main(string[] args)
        {

            for (ID[i]>= IDataAdap)
            DownloadData(ID)
            InsertData(ID);

        }

       
        private static void DownloadData()
        {
            if (connection.State == ConnectionState.Closed)
            {
                connection.Open();
            }


            //the code below can be found here: https://www.npgsql.org/doc/large-objects

            // Retrieve a Large Object Manager for this connection
            NpgsqlLargeObjectManager manager = new NpgsqlLargeObjectManager(connection);

            //the id of the large object we wish to copy
            uint infoToExtract = 16528;

            //where we will extract the data to
            string filePathtoExtract = "C://temp/script10.txt";


            // Reading and writing Large Objects requires the use of a transaction
            using (var transaction = connection.BeginTransaction())
            {

                // Open the file for reading and writing
                using (var stream = manager.OpenReadWrite(infoToExtract))
                {
 
                    var buf2 = new byte[4 * 1024 * 1024];
                    stream.Read(buf2, 0, buf2.Length);
                    File.WriteAllBytes(filePathtoExtract, buf2);     

                }
                //Save the changes to the object
                transaction.Commit();


            }


            connection.Close();

        }
        
       
        
        
       
        private static void InsertData()
        {
            if (connection.State == ConnectionState.Closed)
            {
                connection.Open();
            }

            
            //the code below can be found here: https://www.npgsql.org/doc/large-objects

            // Retrieve a Large Object Manager for this connection
            NpgsqlLargeObjectManager manager = new NpgsqlLargeObjectManager(connection);

            // Create a new empty file, returning the identifier to later access it, if specified inside create, will create a specific id in the metadata
            uint oid = manager.Create(55555);

            

            //C:\temp\tester.txt this the file we testing
            string targetFilePath = "C://temp/downloadresults.txt";




            // Reading and writing Large Objects requires the use of a transaction
            using (var transaction = connection.BeginTransaction())
            {

                // Open the file for reading and writing
                using (var stream = manager.OpenReadWrite(oid))
                {

                    FileStream fileStream = new FileStream(targetFilePath, FileMode.Open, FileAccess.Read, FileShare.None);
                    var buf = new byte[fileStream.Length];

                    fileStream.Read(buf, 0, buf.Length);
                    
                    stream.Write(buf, 0, buf.Length);
                    stream.Seek(0, System.IO.SeekOrigin.Begin);

                    //var buf2 = new byte[buf.Length];
                    //stream.Read(buf2, 0, buf2.Length);

                    // buf2 now contains 1, 2, 3
                }
                //Save the changes to the object
                transaction.Commit();


            }


            connection.Close();

        }























    }
}
