using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.IO;
using GfdbFramework.Core;
using GfdbFramework.DataSource;
using GfdbFramework.Interface;

namespace GfdbFramework.Sqlite
{
    /// <summary>
    /// Sqlite 数据库的数据操作上下文类。
    /// </summary>
    public class DataContext : Core.DataContext
    {
        private static readonly Type _NullableType = typeof(int?).GetGenericTypeDefinition();

        /// <summary>
        /// 使用指定的发行版本号以及数据库连接字符串初始化一个新的 <see cref="DataContext"/> 类实例。
        /// </summary>
        /// <param name="version">数据库发行版本号。</param>
        /// <param name="connectionString">连接字符串。</param>
        public DataContext(string version, string connectionString)
            : base(new DatabaseOperation(connectionString), new SqlFactory())
        {
            if (string.IsNullOrWhiteSpace(version))
                throw new ArgumentNullException(nameof(version), "初始化数据库操作上下文对象时数据库的发行版本号不能为空或纯空白字符串");

            string[] versionInfo = version.Split('.');

            if (double.TryParse($"{versionInfo[0].Trim()}.{(versionInfo.Length > 1 ? versionInfo[1].Trim() : "")}", out double mainVersion))
                BuildNumber = mainVersion;
            else
                throw new ArgumentNullException(nameof(version), "初始化数据库操作上下文对象时数据库的发行版本号格式错误，只支持 X.X.X 格式的版本号，其中 X 必须是整数数字");

            ReleaseName = version;
            Version = version;
        }

        /// <summary>
        /// 使用指定的内部版本号、版本号、发行版本名称以及数据库连接字符串初始化一个新的 <see cref="DataContext"/> 类实例。
        /// </summary>
        /// <param name="buildNumber">数据库内部版本号。</param>
        /// <param name="version">数据库版本号。</param>
        /// <param name="releaseName">数据库发行版本号。</param>
        /// <param name="connectionString">连接字符串。</param>
        public DataContext(double buildNumber, string version, string releaseName, string connectionString)
            : base(new DatabaseOperation(connectionString), new SqlFactory())
        {
            BuildNumber = buildNumber;
            Version = version;
            ReleaseName = releaseName;
        }

        /// <summary>
        /// 获取当前所操作数据库的内部版本号。
        /// </summary>
        public override double BuildNumber { get; }

        /// <summary>
        /// 获取当前所操作数据库的版本号。
        /// </summary>
        public override string Version { get; }

        /// <summary>
        /// 获取当前所操作数据库的发行版本名称。
        /// </summary>
        public override string ReleaseName { get; }

        /// <summary>
        /// 校验指定的数据库是否存在。
        /// </summary>
        /// <param name="databaseInfo">需要校验是否存在的数据库信息。</param>
        /// <returns>若存在则返回 true，否则返回 false。</returns>
        public override bool ExistsDatabase(DatabaseInfo databaseInfo)
        {
            return File.Exists(GetDatabaseFile(databaseInfo));
        }

        /// <summary>
        /// 删除指定的数据库。
        /// </summary>
        /// <param name="databaseInfo">需要删除的数据库信息。</param>
        /// <returns>删除成功返回 true，否则返回 false。</returns>
        public override bool DeleteDatabase(DatabaseInfo databaseInfo)
        {
            string databaseFile = GetDatabaseFile(databaseInfo);

            if (File.Exists(databaseFile))
            {
                File.Delete(databaseFile);

                return true;
            }
            else
            {
                throw new Exception($"所需删除的数据库不存在，对应文件路径为：{databaseFile}");
            }
        }

        /// <summary>
        /// 创建指定的数据库。
        /// </summary>
        /// <param name="databaseInfo">需要创建的数据库信息。</param>
        /// <returns>创建成功返回 true，否则返回 false。</returns>
        public override bool CreateDatabase(DatabaseInfo databaseInfo)
        {
            string databaseFile = GetDatabaseFile(databaseInfo);

            if (File.Exists(databaseFile))
            {
                throw new Exception($"要创建的数据库已存在，对应文件路径为：{databaseFile}");
            }
            else
            {
                string directory = Path.GetDirectoryName(databaseFile);

                if (!Directory.Exists(directory))
                    Directory.CreateDirectory(directory);

                SQLiteConnection.CreateFile(databaseFile);

                return true;
            }
        }

        /// <summary>
        /// 校验指定的数据库表是否存在。
        /// </summary>
        /// <param name="tableSource">所需校验的表数据源。</param>
        /// <returns>若存在则返回 true，否则返回 false。</returns>
        protected override bool ExistsTable(TableDataSource tableSource)
        {
            IParameterContext parameterContext = CreateParameterContext(true);

            object result = ((IDataContext)this).DatabaseOperation.ExecuteScalar(((SqlFactory)SqlFactory).GenerateExistsTableSql(parameterContext, tableSource.Name), CommandType.Text, parameterContext.ToList());

            return result != null && result != DBNull.Value;
        }

        /// <summary>
        /// 校验指定的数据库视图是否存在。
        /// </summary>
        /// <param name="viewSource">所需校验的视图数据源。</param>
        /// <returns>若存在则返回 true，否则返回 false。</returns>
        protected override bool ExistsView(ViewDataSource viewSource)
        {
            IParameterContext parameterContext = CreateParameterContext(true);

            return (int)((IDataContext)this).DatabaseOperation.ExecuteScalar(((SqlFactory)SqlFactory).GenerateExistsViewSql(parameterContext, viewSource.Name), CommandType.Text, parameterContext.ToList()) == 1;
        }

        /// <summary>
        /// 创建指定的数据库表。
        /// </summary>
        /// <param name="tableSource">需创建表的数据源对象。</param>
        /// <returns>创建成功返回 true，否则返回 false。</returns>
        protected override bool CreateTable(TableDataSource tableSource)
        {
            IParameterContext parameterContext = CreateParameterContext(true);

            ((IDataContext)this).DatabaseOperation.ExecuteScalar(((SqlFactory)SqlFactory).GenerateCreateTableSql(parameterContext, tableSource), CommandType.Text, parameterContext.ToList());

            return true;
        }

        /// <summary>
        /// 创建指定的数据库视图。
        /// </summary>
        /// <param name="viewSource">需创建视图的数据源。</param>
        /// <returns>创建成功返回 true，否则返回 false。</returns>
        protected override bool CreateView(ViewDataSource viewSource)
        {
            IParameterContext parameterContext = CreateParameterContext(true);

            ((IDataContext)this).DatabaseOperation.ExecuteScalar(((SqlFactory)SqlFactory).GenerateCreateViewSql(parameterContext, viewSource), CommandType.Text, parameterContext.ToList());

            return true;
        }

        /// <summary>
        /// 删除指定的数据库表。
        /// </summary>
        /// <param name="tableSource">需删除表的数据源对象。</param>
        /// <returns>删除成功返回 true，否则返回 false。</returns>
        protected override bool DeleteTable(TableDataSource tableSource)
        {
            IParameterContext parameterContext = CreateParameterContext(true);

            ((IDataContext)this).DatabaseOperation.ExecuteScalar(((SqlFactory)SqlFactory).GenerateDeleteTableSql(parameterContext, tableSource), CommandType.Text, parameterContext.ToList());

            return true;
        }

        /// <summary>
        /// 删除指定的数据库视图。
        /// </summary>
        /// <param name="viewSource">需删除视图的数据源对象。</param>
        /// <returns>删除成功返回 true，否则返回 false。</returns>
        protected override bool DeleteView(ViewDataSource viewSource)
        {
            IParameterContext parameterContext = CreateParameterContext(true);

            ((IDataContext)this).DatabaseOperation.ExecuteScalar(((SqlFactory)SqlFactory).GenerateDeleteViewSql(parameterContext, viewSource), CommandType.Text, parameterContext.ToList());

            return true;
        }

        /// <summary>
        /// 获取所操作数据库中所有的视图名称集合。
        /// </summary>
        /// <returns>当前上下文所操作数据库中所有存在的视图名称集合。</returns>
        public override ReadOnlyList<string> GetAllViews()
        {
            IParameterContext parameterContext = CreateParameterContext(true);

            List<string> result = new List<string>();

            ((IDataContext)this).DatabaseOperation.ExecuteReader(((SqlFactory)SqlFactory).GenerateSelectAllViewNameSql(parameterContext), CommandType.Text, parameterContext.ToList(), dr =>
            {
                result.Add((string)dr.GetValue(0));

                return true;
            });

            return result;
        }

        /// <summary>
        /// 获取所操作数据库中所有的表名称集合。
        /// </summary>
        /// <returns>当前上下文所操作数据库中所有存在的表名称集合。</returns>
        public override ReadOnlyList<string> GetAllTables()
        {
            IParameterContext parameterContext = CreateParameterContext(true);

            List<string> result = new List<string>();

            ((IDataContext)this).DatabaseOperation.ExecuteReader(((SqlFactory)SqlFactory).GenerateSelectAllTableNameSql(parameterContext), CommandType.Text, parameterContext.ToList(), dr =>
            {
                result.Add((string)dr.GetValue(0));

                return true;
            });

            return result;
        }

        /// <summary>
        /// 创建一个新的参数上下文对象。
        /// </summary>
        /// <param name="enableParametric">是否应当启用参数化操作。</param>
        /// <returns>创建好的参数上下文。</returns>
        public override IParameterContext CreateParameterContext(bool enableParametric)
        {
            return new ParameterContext(enableParametric);
        }

        /// <summary>
        /// 将指定的 .NET 基础数据类型转换成映射到数据库后的默认数据类型（如：System.Int32 应当返回 int，System.String 可返回 varchar(255)）。
        /// </summary>
        /// <param name="type">待转换成数据库数据类型的框架类型。</param>
        /// <returns>该框架类型映射到数据库的默认数据类型。</returns>
        public override string NetTypeToDBType(Type type)
        {
            switch (type.FullName)
            {
                case "System.Int16":
                    return "int16";
                case "System.UInt16":
                    return "uint16";
                case "System.Int32":
                    return "int32";
                case "System.UInt32":
                    return "uint32";
                case "System.Int64":
                case "System.DateTimeOffset":
                case "System.TimeSpan":
                    return "int64";
                case "System.UInt64":
                    return "uint64";
                case "System.DateTime":
                    return "datetime";
                case "System.Guid":
                    return "varchar(36)";
                case "System.Single":
                    return "float";
                case "System.Double":
                case "System.Decimal":
                    return "double";
                case "System.Boolean":
                    return "boolean";
                case "System.SByte":
                    return "int8";
                case "System.Byte":
                    return "uint8";
                case "System.String":
                    return "varchar(255)";
            }

            if (type.IsEnum)
                return "int";
            else if (type.IsGenericType && type.GetGenericTypeDefinition() == _NullableType)
                return NetTypeToDBType(type.GetGenericArguments()[0]);
            else
                throw new Exception(string.Format("未能将 .NET 框架中 {0} 类型转换成 Sqlite 对应的数据类型", type.FullName));
        }

        /// <summary>
        /// 从指定的数据库信息中获取该数据库的文件路径。
        /// </summary>
        /// <param name="databaseInfo">需要获取数据库文件路径的信息对象。</param>
        /// <returns>获取到的数据库文件路径。</returns>
        private string GetDatabaseFile(DatabaseInfo databaseInfo)
        {
            string databaseFile = null;

            if (databaseInfo.Files != null && databaseInfo.Files.Count > 0)
            {
                foreach (var item in databaseInfo.Files)
                {
                    if (item.Type == Enum.FileType.Data)
                    {
                        databaseFile = item.Path;

                        break;
                    }
                }
            }

            if (string.IsNullOrWhiteSpace(databaseFile))
            {
                if (string.IsNullOrWhiteSpace(databaseInfo.Name))
                    throw new Exception("数据库文件路径为空时，数据库名称不能为 null 或纯空白字符串");

                databaseFile = Path.Combine(Path.GetDirectoryName(GetType().Assembly.Location), $"Databases\\{databaseInfo.Name}.db");
            }

            return databaseFile;
        }
    }
}
