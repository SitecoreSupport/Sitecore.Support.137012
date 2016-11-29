using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using Sitecore.Data.DataProviders;
using Sitecore.Data.DataProviders.SqlServer;
using Sitecore.Data.SqlServer;
using Sitecore.Diagnostics;

namespace Sitecore.Support.Data.SqlServer
{
  public class SqlServerDataProvider : Sitecore.Data.SqlServer.SqlServerDataProvider
  {
    public SqlServerDataProvider(string connectionString) : base(connectionString) {}

    private string ConnectionString
    {
      get
      {
        var api = Api as SqlServerDataApi;
        return api?.ConnectionString;
      }
    }

    public override Stream GetBlobStream(Guid blobId, CallContext context)
    {
      Assert.ArgumentNotNull(context, "context");
      var blobSize = GetBlobSize(blobId);
      return blobSize < 0L ? null : OpenBlobStream(blobId, blobSize);
    }

    private Stream OpenBlobStream(Guid blobId, long blobSize)
    {
      const string cmdText = "SELECT [Data]\r\n                     FROM [Blobs]\r\n                     WHERE [BlobId] = @blobId\r\n                     ORDER BY [Index]";
      var connection = OpenConnection();
      try
      {
        var command = new SqlCommand(cmdText, connection)
        {
          CommandTimeout = (int) CommandTimeout.TotalSeconds
        };
        command.Parameters.AddWithValue("@blobId", blobId);
        var reader = command.ExecuteReader(CommandBehavior.CloseConnection | CommandBehavior.SequentialAccess);
        try
        {
          return new SqlServerStream(reader, blobSize);
        }
        catch (Exception exception)
        {
          reader.Close();
          Log.Error("Error reading blob stream (blob id: " + blobId + ")", exception, this);
        }
      }
      catch (Exception exception2)
      {
        connection.Close();
        Log.Error("Error reading blob stream (blob id: " + blobId + ")", exception2, this);
      }
      return null;
    }


    private long GetBlobSize(Guid blobId)
    {
      const string cmdText = " SELECT SUM(CAST(DATALENGTH([Data]) AS BIGINT)) FROM [Blobs] WHERE [BlobId] = @blobId";
      using (var connection = OpenConnection())
      {
        var command = new SqlCommand(cmdText, connection)
        {
          CommandTimeout = (int) CommandTimeout.TotalSeconds
        };
        command.Parameters.AddWithValue("@blobId", blobId);
        using (var reader = command.ExecuteReader(CommandBehavior.CloseConnection))
        {
          if (!reader.Read()) return -1L;
          if (reader.IsDBNull(0)) return -1L;
          return SqlServerHelper.GetLong(reader, 0);
        }
      }
    }

    private SqlConnection OpenConnection()
    {
      var connection = new SqlConnection(ConnectionString);
      connection.Open();
      return connection;
    }
  }
}