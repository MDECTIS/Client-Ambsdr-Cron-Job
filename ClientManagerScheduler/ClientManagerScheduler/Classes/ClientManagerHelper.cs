using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ClientManagerScheduler.Classes
{

    class ClientManagerHelper
    {
        public static ArrayList logList = new ArrayList();
        internal static void Start()
        {
            string MSCFileID = "";
            string EEManagerName = "";
            try
            {
                //No1. get Excel Filename
                string ExcelFileName = getLatestExcelFile();
                logList.Add(string.Format("{0} : Get data from Excel file : {1}", DateTime.Now, ExcelFileName));
                //No2. Convert into Datatable format
                DataTable dtClientManager = getClientManagerFromExcel(ExcelFileName);
                logList.Add(string.Format("{0} : Done convert data from Excel file : {1} int data table format.", DateTime.Now, ExcelFileName));
                int TotalRecord = dtClientManager.Rows.Count;
                int Counter = 0;
                logList.Add(string.Format("{0} :Total record from Excel file : {1}", DateTime.Now, TotalRecord));
                foreach (DataRow dr in dtClientManager.Rows)
                {
                    Counter++;
                    MSCFileID = dr["File ID"].ToString();
                    string AccountID = getAccountID(MSCFileID);
                    EEManagerName = dr["New CA Assignment"].ToString();
                    string EEManagerNameEmail = dr["CM Email"].ToString();
                    logList.Add(string.Format("{0} : MSCFileID : {1} , AccountID : {2} , EEManagerName : {3}", DateTime.Now, MSCFileID, AccountID, EEManagerName));
                    //No. 3 Search for Contact Interaction Manager and In-Active it
                    string ExistingManager = "";
                    InActivePreviousCA(AccountID, out ExistingManager, EEManagerName);
                    logList.Add(string.Format("{0} : Done In-Active current EEManager : {1} in AccountManagerAssignment table.", DateTime.Now, ExistingManager));
                    //No. 4 re-assign with new Client Manager & insert new data into ContactInteraction table
                    AssignNewCA(AccountID, MSCFileID, EEManagerName, EEManagerNameEmail, ref logList);
                    logList.Add(string.Format("{0} : Done Insert new EEManager : {1} in AccountManagerAssignment table.", DateTime.Now, EEManagerName));
                    logList.Add(string.Format("{0} : Compete record {1} / {2}", DateTime.Now, Counter, TotalRecord));

                }
            }
            catch (Exception ex)
            {

                logList.Add(string.Format("{0} : Error occurred for MSCFileID  : {1} and CA Assignment {2}", DateTime.Now, MSCFileID, EEManagerName));
                if (logList.Count > 0)
                {
                    WriteLogFile();
                }
                throw;
            }
            if (logList.Count > 0)
            {
                WriteLogFile();
            }
        }

        private static void WriteLogFile()
        {
            string[] strarr = null;
            strarr = (String[])logList.ToArray(typeof(string));
            string FILE_Path = "";
            string FolderPath = ConfigurationSettings.AppSettings["LogFileLocation"].ToString();
            //ConfigurationSettings.AppSettings["LogFileLocation"].ToString() + "CMAssignment" + DateTime.Now.ToString("dd-MMM-yyyy") + ".txt";
            if (IsDirectoryEmpty(FolderPath))
            {
                FILE_Path = ConfigurationSettings.AppSettings["LogFileLocation"].ToString() + "CMAssignment_" + DateTime.Now.ToString("dd -MMM-yyyy") + ".txt";
            }
            else
            {
                int LastCounter = getLastFileCounter(FolderPath);
                strarr = (String[])logList.ToArray(typeof(string));

                if (LastCounter == 0)
                    FILE_Path = ConfigurationSettings.AppSettings["LogFileLocation"].ToString() + "CMAssignment_" + DateTime.Now.ToString("dd-MMM-yyyy") + ".txt";
                else
                    FILE_Path = ConfigurationSettings.AppSettings["LogFileLocation"].ToString() + "CMAssignment_" + DateTime.Now.ToString("dd-MMM-yyyy") + "_" + LastCounter.ToString() + ".txt";


            }
            System.IO.StreamWriter objWriter = new System.IO.StreamWriter(FILE_Path);
            if (strarr != null)
            {
                foreach (string row1 in strarr)
                {
                    objWriter.WriteLine(row1);
                }
                objWriter.Close();
            }
        }

        private static int getLastFileCounter(string folderPath)
        {
            int output = 0;
            var directory = new DirectoryInfo(folderPath);
            var fileName = directory.GetFiles()
            .OrderByDescending(f => f.LastWriteTime)
            .First();

            string[] arrFile = fileName.ToString().Split('_');
            string sDate = arrFile[1].ToString().Substring(0, 11); //21-Mar-2016
            DateTime dDate = getDateTime(sDate);
            //1.txt, 10.txt
            string lastCounter = "";
            if (dDate.ToString("dd-MMM-yyyy") == DateTime.Now.ToString("dd-MMM-yyyy"))
            {
                if (arrFile.Count() > 2)
                {
                    if (arrFile[2].Length <= 5)
                        lastCounter = arrFile[2].Substring(0, 1);
                    else
                        lastCounter = arrFile[2].Substring(0, 2);
                }
                if (arrFile.Length == 2)
                    output = 1;
                else if (Convert.ToInt32(lastCounter) == 10)
                {
                    output = 0;
                }
                else
                {
                    output = Convert.ToInt32(lastCounter) + 1;
                }
            }
            else
                output = 0;

            return output;
        }

        private static DateTime getDateTime(string sDate)
        {
            DateTime myDate = new DateTime();
            string[] formats = { "dd-MMM-yyyy" };
            return myDate = DateTime.ParseExact(sDate, formats, new CultureInfo(Thread.CurrentThread.CurrentCulture.Name), DateTimeStyles.None);
        }

        private static bool IsDirectoryEmpty(string path)
        {
            return !Directory.EnumerateFileSystemEntries(path).Any();
        }

        private static void AssignNewCA(string AccountID, string MSCFileID, string EEManagerName, string EEManagerNameEmail, ref ArrayList logList)
        {
            string username = EEManagerNameEmail.Replace("@mdec.com.my", "");
            string UserID = getUserIDByEmail(username);
            logList.Add(string.Format("{0} : Get existing for MSCFile : {1} and CA Manager :{2}", DateTime.Now, UserID, EEManagerName));
            try
            {
                if (UserID != "" && !AlreadyThere(UserID, AccountID))
                {
                    using (SqlConnection Connection = SQLHelper.GetConnection())
                    {
                        SqlCommand com = new SqlCommand();
                        StringBuilder sql = new StringBuilder();
                        sql.AppendLine("INSERT INTO [dbo].[AccountManagerAssignment]");
                        sql.AppendLine("(");
                        sql.AppendLine("[AccountManagerAssignmentID]");
                        sql.AppendLine(",[AccountID]");
                        sql.AppendLine(",[UserID]");
                        sql.AppendLine(",[FinancialAnalystID]");
                        sql.AppendLine(" ,[StartDate]");
                        sql.AppendLine(",[EndDate]");
                        sql.AppendLine(",[CreatedDate]");
                        sql.AppendLine(",[ModifiedDate]");
                        sql.AppendLine(",[CreatedBy]");
                        sql.AppendLine(",[ModifiedBy]");
                        sql.AppendLine(",[CreatedByName]");
                        sql.AppendLine(",[ModifiedByName]");
                        sql.AppendLine(",[AccountManagerTypeCID]");
                        sql.AppendLine(",[EEManagerName]");
                        sql.AppendLine(",[AssignmentDate]");
                        sql.AppendLine(",[Active]");
                        sql.AppendLine(",[DataSource] ");
                        sql.AppendLine(")");
                        sql.AppendLine(" VALUES(");
                        sql.AppendLine(" NEWID(),'" + AccountID + "','" + UserID + "', NULL, getdate(), NULL, getdate(), NULL");
                        sql.AppendLine(", '74425431-65A4-498E-A6ED-910A9E20B6FC', NULL, 'admin'");
                        sql.AppendLine(", NULL, 'F3538447-717D-4FBC-B32A-65C06D3AD294','" + EEManagerName + "', getdate(), 1, 'INFO')");

                        com.CommandText = sql.ToString();
                        com.CommandType = CommandType.Text;
                        com.Connection = Connection;
                        com.CommandTimeout = int.MaxValue;
                        try
                        {
                            //con.Open()
                            com.ExecuteNonQuery();
                        }
                        catch (Exception ex)
                        {
                            logList.Add(string.Format("{0} : Error occurred while insert new record in AccountManager Assignment table for MSCFileID  : {1} and CA Assignment {2}", DateTime.Now, MSCFileID, EEManagerName));
                        }
                    }
                }
                else
                {
                    logList.Add(string.Format("{0} : Existing UserID not found Skipped for MSCFile : {1} and CA Manager :{2}", DateTime.Now, UserID, EEManagerName));
                }
            }
            catch (Exception ex)
            {

                logList.Add(string.Format("{0} : Error occurred while insert new record in AccountManager Assignment table for MSCFileID  : {1} and CA Assignment {2}, error : {4}", DateTime.Now, MSCFileID, EEManagerName, ex.Message));
            }


        }

        private static bool AlreadyThere(string userID, string accountID)
        {
            bool Exist = false;
            using (SqlConnection Connection = SQLHelper.GetConnection())
            {
                SqlCommand com = new SqlCommand();
                SqlDataAdapter ad = new SqlDataAdapter(com);
                StringBuilder sql = new StringBuilder();
                sql.AppendLine("Select UserID from AccountManagerAssignment Where UserID='" + userID + "' AND AccountID='" + accountID + "' AND Active=1");
                com.CommandText = sql.ToString();
                com.CommandType = CommandType.Text;
                com.Connection = Connection;
                com.CommandTimeout = int.MaxValue;
                DataTable dt = new DataTable();
                ad.Fill(dt);

                if (dt.Rows.Count > 0)
                {
                    return Exist = true;
                }
            }
            return Exist;
        }

        private static string getUserIDByEmail(string username)
        {
            string UserID = "";
            try
            {
                using (SqlConnection Connection = SQLHelper.GetConnection())
                {
                    SqlCommand com = new SqlCommand();
                    SqlDataAdapter ad = new SqlDataAdapter(com);
                    StringBuilder sql = new StringBuilder();
                    sql.AppendLine("Select UserID from SecurityUser Where UPPER(UserName)=@Username");
                    com.CommandText = sql.ToString();
                    com.CommandType = CommandType.Text;
                    com.Connection = Connection;
                    com.CommandTimeout = int.MaxValue;
                    com.Parameters.Add(new SqlParameter("@UserName", username.ToUpper().Trim()));
                    DataTable dt = new DataTable();
                    ad.Fill(dt);

                    if (dt.Rows.Count > 0)
                    {
                        return UserID = dt.Rows[0][0].ToString();
                    }
                }
            }
            catch (Exception)
            {

                throw;
            }
            return UserID;
        }

        private static string getAccountID(string MSCFileID)
        {
            string AccountID = "";
            try
            {
                using (SqlConnection Connection = SQLHelper.GetConnection())
                {
                    SqlCommand com = new SqlCommand();
                    SqlDataAdapter ad = new SqlDataAdapter(com);

                    System.Text.StringBuilder sql = new System.Text.StringBuilder();
                    sql.AppendLine("SELECT AccountID from Account");
                    sql.AppendLine("WHERE MSCFileID = @MSCFileID");
                    com.CommandText = sql.ToString();
                    com.CommandType = CommandType.Text;
                    com.Connection = Connection;
                    com.CommandTimeout = int.MaxValue;
                    com.Parameters.Add(new SqlParameter("@MSCFileID", MSCFileID));
                    DataTable dt = new DataTable();
                    ad.Fill(dt);

                    if (dt.Rows.Count > 0)
                    {
                        return AccountID = dt.Rows[0][0].ToString();
                    }
                }
            }
            catch (Exception)
            {

                throw;
            }
            return AccountID;
        }

        private static void InActivePreviousCA(string AccountID, out string ExistingManager, string newEEManager)
        {
            ExistingManager = "";
            try
            {
                //GET AccountManagerAssignmentID
                string AccountManagerAssignmentID = "";
                using (SqlConnection Connection = SQLHelper.GetConnection())
                {
                    SqlCommand com = new SqlCommand();
                    SqlDataAdapter ad = new SqlDataAdapter(com);
                    StringBuilder sbSql = new StringBuilder();
                    sbSql.AppendLine(string.Format("SELECT [AccountManagerAssignmentID],[EEManagerName]"));
                    sbSql.AppendLine(string.Format("FROM [CRM_PRD].[dbo].[AccountManagerAssignment]"));
                    sbSql.AppendLine(string.Format("where AccountID ='{0}'", AccountID));
                    sbSql.AppendLine(string.Format("And AccountManagerTypeCID = 'F3538447-717D-4FBC-B32A-65C06D3AD294' AND EEManagerName <> '" + newEEManager + "' AND Active=1"));
                    sbSql.AppendLine(string.Format("ORDER BY CreatedDate DESC"));
                    DataTable dt = new DataTable();
                    com.CommandText = sbSql.ToString();
                    com.CommandType = CommandType.Text;
                    com.Connection = Connection;
                    com.CommandTimeout = int.MaxValue;
                    ad.Fill(dt);

                    if (dt.Rows.Count > 0)
                    {
                        AccountManagerAssignmentID = dt.Rows[0][0].ToString();
                        ExistingManager = dt.Rows[0][1].ToString();
                    }


                }
                if (AccountManagerAssignmentID != "" && ExistingManager != "")
                {
                    //Set it to In-Active
                    using (SqlConnection Connection = SQLHelper.GetConnection())
                    {
                        SqlCommand com = new SqlCommand();
                        SqlDataAdapter ad = new SqlDataAdapter(com);
                        System.Text.StringBuilder sql = new System.Text.StringBuilder();
                        sql.AppendLine("UPDATE AccountManagerAssignment SET Active =0 , EndDate=@EndDate");
                        sql.AppendLine("WHERE AccountManagerAssignmentID = @AccountManagerAssignmentID");

                        com.CommandText = sql.ToString();
                        com.CommandType = CommandType.Text;
                        com.Connection = Connection;
                        com.CommandTimeout = int.MaxValue;

                        com.Parameters.Add(new SqlParameter("@AccountManagerAssignmentID", AccountManagerAssignmentID));
                        com.Parameters.Add(new SqlParameter("@EndDate", DateTime.Now));
                        try
                        {
                            com.ExecuteNonQuery();
                        }
                        catch (Exception ex)
                        {

                        }

                    }
                }
            }
            catch (Exception ex)
            {

                throw;
            }
        }

        private static string getLatestExcelFile()
        {
            string FileName = "";
            string folderPath = ConfigurationSettings.AppSettings["ExcelLocation"].ToString();
            if (!Directory.Exists(folderPath))
            {
                Directory.CreateDirectory(folderPath);
            }
            string ExcelFile = "";
            var directory = new DirectoryInfo(folderPath);
            var fileName = directory.GetFiles()
            .OrderByDescending(f => f.LastWriteTime)
            .First();
            if (fileName != null)
                FileName = ConfigurationSettings.AppSettings["ExcelLocation"].ToString() + fileName.ToString();

            return FileName;
        }

        private static DataTable getClientManagerFromExcel(string ExcelFileName)
        {
            DataTable dt = ExcelToDT.exceldata(ExcelFileName);

            return dt;
        }
    }
}
