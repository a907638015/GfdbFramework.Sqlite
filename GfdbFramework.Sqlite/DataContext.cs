using System;

namespace GfdbFramework.Sqlite
{
    /// <summary>
    /// SqlServer 数据库的数据操作上下文类。
    /// </summary>
    public class DataContext : Realize.DataContext
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
                case "System.Int32":
                    return "int32";
                case "System.Int64":
                case "System.DateTimeOffset":
                case "System.TimeSpan":
                    return "int64";
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
    }
}
