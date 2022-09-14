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
        static String fileLocation;

        static NpgsqlConnection originalConnection;
        static NpgsqlConnection restoredServerConnection;

        static void Main(string[] args)
        {


            Console.Write("Enter original server name: ");
            String originalServerName = Console.ReadLine();
            Console.Write("Enter original server database name: ");
            String originalServerDatabaseName = Console.ReadLine();
            Console.Write("Enter original server username: ");
            String originalServerUserName = Console.ReadLine();

            string originalServerUserPassword = "";
            Console.Write("Enter original server user password: ");
            ConsoleKeyInfo key;

            do
            {
                key = Console.ReadKey(true);

                // Backspace Should Not Work
                if (key.Key != ConsoleKey.Backspace)
                {
                    originalServerUserPassword += key.KeyChar;
                    Console.Write("*");
                }
                else
                {
                    Console.Write("\b");
                }
            }
            // Stops Receving Keys Once Enter is Pressed
            while (key.Key != ConsoleKey.Enter);

            Console.WriteLine();
            //Console.WriteLine("The Password You entered is : " + originalServerUserPassword);

            String originalServerConnectionString = "Server=" + originalServerName + "; Database= " + originalServerDatabaseName + ";Port=5432;User Id= " + originalServerUserName + ";Password= " + originalServerUserPassword + ";";
            //Console.WriteLine("OriginalServer Conn String is " + originalServerConnectionString);

            Console.WriteLine();
            Console.Write("Enter restored server name: ");
            String restoredServerName = Console.ReadLine();
            Console.Write("Enter restored server database name: ");
            String restoredServerDatabaseName = Console.ReadLine();
            Console.Write("Enter restored server username: ");
            String restoredServerUserName = Console.ReadLine();

            string restoredServerUserPassword = "";
            Console.Write("Enter restored server user password: ");
            ConsoleKeyInfo key1;

            do
            {
                key1 = Console.ReadKey(true);

                // Backspace Should Not Work
                if (key1.Key != ConsoleKey.Backspace)
                {
                    restoredServerUserPassword += key1.KeyChar;
                    Console.Write("*");
                }
                else
                {
                    Console.Write("\b");
                }
            }
            // Stops Receving key1s Once Enter is Pressed
            while (key1.Key != ConsoleKey.Enter);

            Console.WriteLine();
            //Console.WriteLine("The Password You entered is : " + restoredServerUserPassword);

            String restoredServerConnectionString = "Server=" + restoredServerName + "; Database= " + restoredServerDatabaseName + ";Port=5432;User Id= " + restoredServerUserName + ";Password= " + restoredServerUserPassword + ";";
            //Console.WriteLine("restoredServer Conn String is " + restoredServerConnectionString);

            originalConnection = new NpgsqlConnection(originalServerConnectionString);
            restoredServerConnection = new NpgsqlConnection(restoredServerConnectionString);

            System.Console.WriteLine();
            System.Console.Write("Enter temp folder location : ");
            fileLocation = Console.ReadLine();

            System.Console.Write("Delete temp files: ");
            bool deleteFiles = bool.Parse(Console.ReadLine());
            System.Console.WriteLine();

            System.Console.Write("Enter oid file name: ");
            string oidFile = Console.ReadLine();
            using (StreamReader reader = new StreamReader(oidFile))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    uint loid = uint.Parse(line);
                    //Console.WriteLine(line);
                    if (doesNotExist(loid))
                    {
                        DownloadData(loid, fileLocation + loid + "", restoredServerConnection);
                        System.Console.Write("Exported oid successfully to: " + fileLocation + loid + "");
                        InsertData(loid, fileLocation + loid + "", originalConnection);
                        System.Console.Write(", Imported oid successfully");
                        if (deleteFiles)
                        {
                            File.Delete(fileLocation + loid + "");
                            System.Console.WriteLine(", Deleted the oid file successfully");
                        }
                        else
                        {
                            System.Console.WriteLine();
                        }
                    }
                }

                reader.Close();
            }

            System.Console.WriteLine();
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
                    }
                    catch (Exception doesntExistInRestoredServer)
                    {
                        Console.WriteLine("Doesn't exist in the Restored server, won't insert OID: " + id + "");
                        return false;
                    }

                    Console.Write("OID " + id + " does not exist on orignal server, will begin transfer operation ");
                    return true;
                }
                finally
                {
                    //Save the changes to the object
                    transaction.Commit();
                }


            }
            Console.WriteLine("OID " + id + " already exists, will not transfer data");

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
            }
            catch (Exception idExists)
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
                    fileStream.Close();
                }
                //Save the changes to the object
                transaction.Commit();
            }

        }






    }
}

