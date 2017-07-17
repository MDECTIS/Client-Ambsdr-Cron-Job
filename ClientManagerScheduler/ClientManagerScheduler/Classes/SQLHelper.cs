using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClientManagerScheduler.Classes
{
    class SQLHelper : IDisposable
    {
        // Data insertion and update mode
        public SqlConnection connection;
        public SqlDataAdapter dataAdapter;
        public DataSet dataSet;

        public static object ExecuteScalar(string sql, SqlConnection conn)
        {
            SqlCommand command = new SqlCommand(sql, conn);
            return command.ExecuteScalar();
        }

        public void OpenConnection()
        {
            string connStr = GetConnectionString();
            connection = new SqlConnection(connStr);
            connection.Open();
        }

        public void Close()
        {
            if (connection == null || connection.State != ConnectionState.Open)
                return;

            connection.Close();
        }

        public static void ResetTable(string tableName, SqlConnection conn)
        {
            SQLHelper.ExecuteSql("DELETE FROM " + tableName, conn);

            try
            {
                SQLHelper.ExecuteSql("DBCC CHECKIDENT ('" + tableName + "', reseed, 0)", conn);
            }
            catch (Exception)
            {


            }

        }

        public void PrepareInsert(string tableName)
        {
            string sql = "SELECT * FROM " + tableName;
            dataAdapter = new SqlDataAdapter(sql, connection);
            SqlCommandBuilder cb = new SqlCommandBuilder(dataAdapter);
            dataSet = new DataSet();
            dataAdapter.Fill(dataSet);
        }

        public DataRow Insert(DataRow row)
        {
            DataTable table = dataSet.Tables[0];
            if (row != null)
                table.Rows.Add(row);

            DataRow output = table.NewRow();
            return output;
        }

        public void EndInsert()
        {
            dataAdapter.Update(dataSet);
        }

        public void EndUpdate()
        {
            dataAdapter.Update(dataSet);
        }

        // static functions
        public static string GetConnectionString()
        {
            string output = Properties.Settings.Default.CRMConnstr;
            return output;
        }

        public static SqlConnection GetConnection()
        {
            SqlConnection output = new SqlConnection(GetConnectionString());
            output.Open();
            return output;
        }

        public static SqlDataReader GetDataReader(string sqlQuery, SqlConnection connection)
        {
            if (connection == null)
                return null;

            if (connection.State != System.Data.ConnectionState.Open)
                connection.Open();

            SqlCommand command = new SqlCommand(sqlQuery, connection);
            SqlDataReader output = command.ExecuteReader();
            return output;
        }

        public static void ExecuteSql(string sql, SqlConnection conn)
        {
            SqlCommand command = new SqlCommand(sql, conn);
            command.ExecuteNonQuery();
        }

        #region IDisposable Members

        public void Dispose()
        {
            Close();
        }

        #endregion

        public static string InClause(List<int> ids)
        {
            string output = "";
            for (int i = 0; i < ids.Count; i++)
            {
                int id = ids[i];
                if (i > 0)
                    output += ",";
                output += id.ToString();
            }
            output = " (" + output + ") ";
            return output;
        }

        public void PrepareUpdate(string sql)
        {
            dataAdapter = new SqlDataAdapter(sql, connection);
            SqlCommandBuilder cb = new SqlCommandBuilder(dataAdapter);
            dataSet = new DataSet();
            dataAdapter.Fill(dataSet);
        }

        public DataRow FindRowFromID(int id, string idField)
        {
            DataTable table = dataSet.Tables[0];
            foreach (DataRow row in table.Rows)
            {
                if (Convert.ToInt32(row[idField]) == id)
                    return row;
            }
            return null;

        }

        public static string InClause(List<string> items)
        {
            string output = "";
            for (int i = 0; i < items.Count; i++)
            {
                string item = items[i];
                if (i > 0)
                    output += ",";
                output += "'" + item + "'";
            }
            output = " (" + output + ") ";
            return output;
        }
    }
}
