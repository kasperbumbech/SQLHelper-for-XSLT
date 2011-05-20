using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Xml;
using System.Xml.XPath;
using System.Data;
using System.Data.SqlClient;
using Microsoft.ApplicationBlocks.Data;
using System.IO;

namespace Xslt.ApplicationBlocks.Data
{
    public class SqlHelper
    {
        /// <summary>
        /// Executes a query against a database - returns the result for pretty displaying of the data in xslt
        /// </summary>
        /// <param name="args">
        /// Accepts a piece of xml that can be mapped to the  Microsoft.ApplicationBlocks.Data.SqlHelper.ExecuteDataset method.
        /// 
        /// Read about it here: http://projekt.chainbox.dk/default.asp?W258
        /// </param>
        /// <returns>
        /// The first datatable returned by the query 
        /// 
        /// <root>
        ///  <item>
        ///    <fieldname>fieldvalue</fieldname>
        ///    <fieldname>fieldvalue</fieldname>
        ///    <fieldname>fieldvalue</fieldname>
        ///  </item>
        ///  <item>
        ///    <fieldname>fieldvalue</fieldname>
        ///    <fieldname>fieldvalue</fieldname>
        ///    <fieldname>fieldvalue</fieldname>
        ///  </item>
        /// </root>
        /// 
        /// </returns>
        public static XPathNodeIterator ExecuteDataset(XPathNodeIterator args)
        {
            try
            {
                XPathNodeIterator retval;
                SqlHelperArgs sqlargs = ParseArgs(args);
                DataSet ds = Microsoft.ApplicationBlocks.Data.SqlHelper.ExecuteDataset(sqlargs.ConnectionString, sqlargs.CommandType, sqlargs.CommandText, sqlargs.Params.ToArray());
                XmlDocument xd = new XmlDocument();
                ds.DataSetName = "root";
                // this is bad - the query might not return anything - but hey this is webdevelopment.
                ds.Tables[0].TableName = "item";

                using (StringWriter sw = new StringWriter())
                {
                    ds.WriteXml(sw);
                    xd.LoadXml(sw.ToString());
                }
                retval = xd.CreateNavigator().Select(".");

                WriteToTrace("ExecuteDataset: Returning data from:" + sqlargs.CommandText + " without using cache");
                return retval;
            }
            catch (Exception ex)
            {
                WriteToTrace(ex.ToString());
                return EmptyXpathNodeIterator(ex, "Xslt.ApplicationBlocks.Data.ExecuteDataset", true);
            }
        }

        /// <summary>
        /// Executes a query against a database without results
        /// </summary>
        /// <param name="args">
        /// Accepts a piece of xml that can be mapped to the  Microsoft.ApplicationBlocks.Data.SqlHelper.ExecuteDataset method.
        /// 
        /// Read about it here: http://projekt.chainbox.dk/default.asp?W258
        /// </param>
        public static void ExecuteNonQuery(XPathNodeIterator args)
        {
            try
            {
                SqlHelperArgs sqlargs = ParseArgs(args);
                Microsoft.ApplicationBlocks.Data.SqlHelper.ExecuteNonQuery(sqlargs.ConnectionString, sqlargs.CommandType, sqlargs.CommandText, sqlargs.Params.ToArray());
            }
            catch (Exception ex)
            {
                WriteToTrace(ex.ToString());
            }
        }

        /// <summary>
        /// Executes a query against a database with a single result
        /// </summary>
        /// <param name="args">
        /// Accepts a piece of xml that can be mapped to the  Microsoft.ApplicationBlocks.Data.SqlHelper.ExecuteDataset method.
        /// 
        /// Read about it here: http://projekt.chainbox.dk/default.asp?W258
        /// </param>
        /// <returns>
        /// A string representation of the scalar sql result
        /// </returns>
        public static string ExecuteScalar(XPathNodeIterator args)
        {
            try
            {
                SqlHelperArgs sqlargs = ParseArgs(args);
                return Microsoft.ApplicationBlocks.Data.SqlHelper.ExecuteScalar(sqlargs.ConnectionString, sqlargs.CommandType, sqlargs.CommandText, sqlargs.Params.ToArray()).ToString();
            }
            catch (Exception ex)
            {
                WriteToTrace(ex.ToString());
            }
            return "Error executing sql - investigate trace";
        }

        private static XPathNodeIterator EmptyXpathNodeIterator(Exception ex, string caller, bool logAsError)
        {
            XmlDocument xd = new XmlDocument();
            xd.AppendChild(xd.CreateElement("root"));
            xd.FirstChild.AppendChild(xd.CreateCDataSection("An error has occured in method: " + caller + "\n" + ex.Message));
            return xd.FirstChild.CreateNavigator().Select(".");
        }

        private static void WriteToTrace(string message)
        {
            System.Web.HttpContext.Current.Trace.Warn(message);
        }

        private static SqlHelperArgs ParseArgs(XPathNodeIterator args) {
            
            args.MoveNext();

            XmlDocument xd = new XmlDocument();
            xd.LoadXml(args.Current.OuterXml);

            XmlElement argsel = (XmlElement)xd.FirstChild;

            SqlHelperArgs retval = new SqlHelperArgs();
            
            retval.CommandText = argsel.SelectSingleNode("commandtext").InnerText;

            if (argsel.SelectSingleNode("commandtype").InnerText.ToLower() == "storedprocedure")
            {
                retval.CommandType = CommandType.StoredProcedure;
            }
            else {
                retval.CommandType = CommandType.Text;
            }

            foreach (XmlElement el in argsel.SelectNodes("parameter"))
            {
                string name = el.GetAttribute("name");
                string value = el.InnerText;
                retval.Params.Add(new SqlParameter(name, value));
            }

            // A connectionstring can either be specified in a custom app setting
            // Be specified in the XSLT
            // Or by default fall back to the umbraco connectionstring
            XmlElement connectionstringel = (XmlElement)argsel.SelectSingleNode("connectionstring");

            if (connectionstringel != null) {
                if (connectionstringel.HasAttribute("appkey")) {
                    retval.ConnectionString = System.Configuration.ConfigurationManager.AppSettings[connectionstringel.GetAttribute("appkey")];
                } else { 
                    // you can specify the DBDSN directly in the XSLT file
                    retval.ConnectionString = connectionstringel.InnerText;
                }
            } 
            // If no connectionstring is specified
            // this baby falls back to the umbraco connectionstring
            else {
                retval.ConnectionString = System.Configuration.ConfigurationManager.AppSettings["umbracoDbDSN"];
            }
            
            return retval;
        }

        private class SqlHelperArgs
        {
            public string ConnectionString = "";
            public string CommandText = "";
            public CommandType CommandType = CommandType.StoredProcedure;
            public List<SqlParameter> Params = new List<SqlParameter>();
            public bool UseCache = false;
            public int CacheExpiration = 0;

            /// <summary>
            /// Generates a unique key for inserting in a cachestorage
            /// </summary>
            public string CacheKey
            {
                get
                {
                    string retval = ConnectionString + CommandText;
                    foreach (SqlParameter param in Params)
                    {
                        retval += param.ParameterName + param.Value.ToString();
                    }
                    return retval;
                }
            }
        }
    }
}
