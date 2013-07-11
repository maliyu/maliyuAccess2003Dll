using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.OleDb;

namespace maliyuAccess2003Dll
{
    /// <summary>
    /// DLL to manipulate Microsoft Access 2003 written by maliyu.
    /// </summary>
    public class maliyuAccess
    {
        #region Internal variable 
        internal OleDbConnection dbOleConn = null;
        internal string inputString = null;
        internal DataSet outputResult = null;
        #endregion

        /// <summary>
        /// An OleDbConnection object represents a unique connection to a data source.Client/server database system is not supported.
        /// <param name="oleConn"> An instance of OleDbConnection. </param>
        /// </summary>
        public maliyuAccess(OleDbConnection oleConn)
        {
            if (oleConn == null)
            {
                throw (new ArgumentNullException());
            }

            dbOleConn = oleConn;
        }

        /// <summary>
        /// Search the string in whole database 
        /// </summary>
        /// <param name="searchString"> search string. If string is null or empty, then exception will be thrown out</param>
        /// <returns >search result store as dataset
        /// since search string could be exist in multi tables and multi records in the database
        /// </returns>
        public DataSet QueryWholeDB(string searchString)
        {
            if (searchString == null)
            {
                throw (new ArgumentNullException());
            }

            if (searchString.Length == 0)
            {
                throw (new SystemException("Search string is empty!"));
            }

            if (dbOleConn == null)
            {
                throw new ArgumentNullException();
            }

            dbOleConn.Open();

            inputString = searchString;
            outputResult = new DataSet();

            /* Since we are doing whole database searching, we need to know every table name and table column name */
            /* Retrieve those info through schema table */
            DataTable dtDBtables = dbOleConn.GetOleDbSchemaTable(System.Data.OleDb.OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });
            foreach (DataRow drDBtable in dtDBtables.Rows)
            {
                /* Query in every table */
                DataTable dtDBtableCols = dbOleConn.GetOleDbSchemaTable(System.Data.OleDb.OleDbSchemaGuid.Columns, new object[] { null, null, drDBtable["TABLE_NAME"].ToString(), null });
                /* Get primary key */
                DataTable schemaTbl = dbOleConn.GetOleDbSchemaTable(System.Data.OleDb.OleDbSchemaGuid.Primary_Keys, new object[] { null, null, drDBtable["TABLE_NAME"].ToString() });
                string priKey = schemaTbl.Rows[0]["COLUMN_NAME"].ToString();
                DataTable newRstTable = null;

                foreach (DataRow drDBcol in dtDBtableCols.Rows)
                {
                    /* Query in every column of the table */
                    string sqlQueryString = string.Format("SELECT * FROM [{0}] WHERE [{1}] LIKE \"%{2}%\"", drDBtable["TABLE_NAME"].ToString(), drDBcol["COLUMN_NAME"].ToString(), searchString);
                    System.Data.OleDb.OleDbCommand oleDbCmd = new System.Data.OleDb.OleDbCommand(sqlQueryString, dbOleConn);
                    System.Data.OleDb.OleDbDataReader dataReader = oleDbCmd.ExecuteReader();
                    while (dataReader.Read())
                    {
                        if (newRstTable == null)
                        {
                            newRstTable = outputResult.Tables.Add(drDBtable["TABLE_NAME"].ToString());
                            for (int i = 0; i < dataReader.FieldCount; i++)
                            {
                                newRstTable.Columns.Add(dataReader.GetName(i), dataReader.GetFieldType(i));
                            }
                        }
                        DataRow newRstTableRow = newRstTable.NewRow();
                        for (int i = 0; i < dataReader.FieldCount; i++)
                        {
                            newRstTableRow[dataReader.GetName(i)] = dataReader[dataReader.GetName(i)];
                        }

                        if (newRstTable.Rows.Count > 0)
                        {
                            bool isSameRow = false;
                            foreach (DataRow row in newRstTable.Rows)
                            {
                                if (row[priKey].Equals(newRstTableRow[priKey]))
                                {
                                    isSameRow = true;
                                    break;
                                }
                            }
                            if (isSameRow == false)
                            {
                                newRstTable.Rows.Add(newRstTableRow);
                            }
                        } 
                        else
                        {
                            newRstTable.Rows.Add(newRstTableRow);
                        }
                    }

                    if (dataReader.HasRows)
                    {
                        dataReader.Close();
                    }
                }
            }

            dbOleConn.Close();

            return outputResult;
        }

        /// <summary>
        /// get record from whole database based on field name and field content 
        /// </summary>
        /// <param name="FieldName"> Table field name. If database does not contain it, then null will be return </param>
        /// <param name="FieldContent"> Table field content. If database does not contain it, then null will be return </param>
        /// <returns >It may return multi table and record
        /// </returns>
        public DataSet GetDBRecord(string FieldName, string FieldContent)
        {
            if (FieldName == null || FieldContent == null)
            {
                throw (new ArgumentNullException());
            }

            if (FieldName.Length == 0 || FieldContent.Length == 0)
            {
                throw (new SystemException("Search string is empty!"));
            }

            if (dbOleConn == null)
            {
                throw new ArgumentNullException();
            }

            dbOleConn.Open();

            DataSet resultDataSet = null;

            /* Since we are doing whole database searching, we need to know every table name and table column name */
            /* Retrieve those info through schema table */
            DataTable dtDBtables = dbOleConn.GetOleDbSchemaTable(System.Data.OleDb.OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });
            foreach (DataRow drDBtable in dtDBtables.Rows)
            {
                DataTable dtDBtableCols = dbOleConn.GetOleDbSchemaTable(System.Data.OleDb.OleDbSchemaGuid.Columns, new object[] { null, null, drDBtable["TABLE_NAME"].ToString(), null });
                string colName = null;
                foreach (DataRow drDBcol in dtDBtableCols.Rows)
                {
                    if (FieldName.Equals(drDBcol["COLUMN_NAME"].ToString()))
                    {
                        colName = FieldName;
                    }
                }

                if (colName != null)
                {
                    string sqlQueryString = string.Format("SELECT * FROM [{0}] WHERE [{1}] = \"{2}\"", drDBtable["TABLE_NAME"].ToString(), FieldName, FieldContent);
                    OleDbCommand oleDbCmd = new System.Data.OleDb.OleDbCommand(sqlQueryString, dbOleConn);
                    OleDbDataReader dataReader = oleDbCmd.ExecuteReader();

                    while (dataReader.Read())
                    {
                        if (resultDataSet == null)
                        {
                            resultDataSet = new DataSet();
                        }
                        DataTable newRstTable = resultDataSet.Tables.Add(drDBtable["TABLE_NAME"].ToString());
                        for (int i = 0; i < dataReader.FieldCount; i++)
                        {
                            newRstTable.Columns.Add(dataReader.GetName(i), dataReader.GetFieldType(i));
                        }

                        DataRow newRstTableRow = newRstTable.NewRow();
                        for (int i = 0; i < dataReader.FieldCount; i++)
                        {
                            newRstTableRow[dataReader.GetName(i)] = dataReader[dataReader.GetName(i)];
                        }
                        newRstTable.Rows.Add(newRstTableRow);
                    }

                    if (dataReader.HasRows)
                    {
                        dataReader.Close();
                    }
                }
            }

            dbOleConn.Close();

            return resultDataSet;
        }

        /// <summary>
        /// get record from specific table based on table name, field name and field content 
        /// </summary>
        /// <param name="TableName"> Table name. If database does not contain it, then null will be return </param>
        /// <param name="FieldName"> Table field name. If database does not contain it, then null will be return </param>
        /// <param name="FieldContent"> Table field content. If database does not contain it, then null will be return </param>
        /// <returns >It may return multi table and record
        /// </returns>
        public DataSet GetDBRecord(string TableName, string FieldName, string FieldContent)
        {
            if (TableName == null || FieldName == null || FieldContent == null)
            {
                throw (new ArgumentNullException());
            }

            if (TableName.Length == 0 || FieldName.Length == 0 || FieldContent.Length == 0)
            {
                throw (new SystemException("Search string is empty!"));
            }

            if (dbOleConn == null)
            {
                throw new ArgumentNullException();
            }

            dbOleConn.Open();

            DataSet resultDataSet = null;

            string sqlQueryString = string.Format("SELECT * FROM [{0}] WHERE [{1}] = \"{2}\"", TableName, FieldName, FieldContent);
            OleDbCommand oleDbCmd = new System.Data.OleDb.OleDbCommand(sqlQueryString, dbOleConn);
            OleDbDataReader dataReader = oleDbCmd.ExecuteReader();
            while (dataReader.Read())
            {
                if (resultDataSet == null)
                {
                    resultDataSet = new DataSet();
                }
                DataTable newRstTable = resultDataSet.Tables.Add(TableName);
                for (int i = 0; i < dataReader.FieldCount; i++)
                {
                    newRstTable.Columns.Add(dataReader.GetName(i), dataReader.GetFieldType(i));
                }

                DataRow newRstTableRow = newRstTable.NewRow();
                for (int i = 0; i < dataReader.FieldCount; i++)
                {
                    newRstTableRow[dataReader.GetName(i)] = dataReader[dataReader.GetName(i)];
                }
                newRstTable.Rows.Add(newRstTableRow);
            }

            if (dataReader.HasRows)
            {
                dataReader.Close();
            }

            dbOleConn.Close();

            return resultDataSet;
        }
    }
}
