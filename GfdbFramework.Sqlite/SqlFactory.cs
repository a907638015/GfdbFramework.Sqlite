using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SQLite;
using System.IO;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using GfdbFramework.Core;
using GfdbFramework.DataSource;
using GfdbFramework.Enum;
using GfdbFramework.Field;
using GfdbFramework.Interface;

namespace GfdbFramework.Sqlite
{
    /// <summary>
    /// SqlServer 数据库的 Sql 创建工厂类。
    /// </summary>
    public class SqlFactory : ISqlFactory
    {
        private const string _CASE_SENSITIVE_MARK = "collate nocase";
        private const string _BOOL_TYPE_NAME = "System.Boolean";
        private const string _STRING_TYPE_NAME = "System.String";
        private const string _DATETIME_TYPE_NAME = "System.DateTime";
        private const string _INT_TYPE_NAME = "System.Int32";
        private const string _DOUBLE_TYPE_NAME = "System.Double";
        private const string _FLOAT_TYPE_NAME = "System.Single";
        private const string _SHORT_TYPE_NAME = "System.Int16";
        private const string _SBYTE_TYPE_NAME = "System.SByte";
        private const string _LONG_TYPE_NAME = "System.Int64";
        private const string _DECIMAL_TYPE_NAME = "System.Decimal";
        private const string _GUID_TYPE_NAME = "System.Guid";
        private const string _MATH_TYPE_NAME = "System.Math";
        private static readonly string _DBFunCountMethodName = nameof(DBFun.Count);
        private static readonly string _DBFunMaxMethodName = nameof(DBFun.Max);
        private static readonly string _DBFunMinMethodName = nameof(DBFun.Min);
        private static readonly string _DBFunSumMethodName = nameof(DBFun.Sum);
        private static readonly string _DBFunAvgMethodName = nameof(DBFun.Avg);
        private static readonly string _DBFunSTDevMethodName = nameof(DBFun.STDev);
        private static readonly string _DBFunSTDevPMethodName = nameof(DBFun.STDevP);
        private static readonly string _DBFunVarMethodName = nameof(DBFun.Var);
        private static readonly string _DBFunVarPMethodName = nameof(DBFun.VarP);
        private static readonly string _DBFunNowTimeMethodName = nameof(DBFun.NowTime);
        private static readonly string _DBFunNewIntMethodName = nameof(DBFun.NewInt);
        private static readonly string _DBFunNewLongMethodName = nameof(DBFun.NewLong);
        private static readonly string _DBFunDiffYearMethodName = nameof(DBFun.DiffYear);
        private static readonly string _DBFunDiffMonthMethodName = nameof(DBFun.DiffMonth);
        private static readonly string _DBFunDiffDayMethodName = nameof(DBFun.DiffDay);
        private static readonly string _DBFunDiffHourMethodName = nameof(DBFun.DiffHour);
        private static readonly string _DBFunDiffMinuteMethodName = nameof(DBFun.DiffMinute);
        private static readonly string _DBFunDiffSecondMethodName = nameof(DBFun.DiffSecond);
        private static readonly string _DBFunAddDayMethodName = nameof(DBFun.AddDay);
        private static readonly string _DBFunAddHourMethodName = nameof(DBFun.AddHour);
        private static readonly string _DBFunAddMinuteMethodName = nameof(DBFun.AddMinute);
        private static readonly string _DBFunAddSecondMethodName = nameof(DBFun.AddSecond);
        private static readonly string _DBFunAddMillisecondMethodName = nameof(DBFun.AddMillisecond);
        private static readonly Type _DBFunType = typeof(DBFun);

        /// <summary>
        /// 对指定的原始字段名、表名或视图名称进行编码。
        /// </summary>
        /// <param name="name">需要编码的原始字段名、表名或视图名称名称。</param>
        /// <param name="type">名称类型。</param>
        /// <returns>编码后的名称。</returns>
        public string EncodeName(string name, NameType type)
        {
            return $"[{name}]";
        }

        /// <summary>
        /// 使用指定的别名下标生成一个别名（必须保证不同下标生成的别名不同，相同下标生成的别名相同，且所生成的别名不得是数据库中的关键字）。
        /// </summary>
        /// <param name="aliasIndex">生成别名时的下标。</param>
        /// <param name="type">需要生成别名的名称类型。</param>
        /// <returns>使用指定别名下标生成好的别名。</returns>
        public string GenerateAlias(int aliasIndex, NameType type)
        {
            return type == NameType.Field ? $"F{aliasIndex}" : type == NameType.View ? $"V{aliasIndex}" : $"T{aliasIndex}";
        }

        /// <summary>
        /// 生成指定数据源所对应的 Sql 查询语句。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">待生成查询 Sql 的数据源信息。</param>
        /// <param name="parameters">生成 Sql 所需使用的参数集合。</param>
        /// <returns>生成好的 Sql 查询语句。</returns>
        public string GenerateQuerySql(IDataContext dataContext, BasicDataSource dataSource, out Interface.IReadOnlyList<DbParameter> parameters)
        {
            return GenerateQuerySql(dataContext, dataSource, true, out parameters);
        }

        /// <summary>
        /// 生成指定数据源所对应的 Sql 查询语句。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">待生成查询 Sql 的数据源信息。</param>
        /// <param name="useFieldAlias">生成的查询语句是否应当应用上字段别名。</param>
        /// <param name="parameters">生成 Sql 所需使用的参数集合。</param>
        /// <returns>生成好的 Sql 查询语句。</returns>
        public string GenerateQuerySql(IDataContext dataContext, BasicDataSource dataSource, bool useFieldAlias, out Interface.IReadOnlyList<DbParameter> parameters)
        {
            Dictionary<object, DbParameter> pars = new Dictionary<object, DbParameter>();

            string sql = GenerateQuerySql(dataContext, dataSource, dataSource.SelectField ?? dataSource.RootField, useFieldAlias, item =>
            {
                if (!pars.TryGetValue(item, out DbParameter dbParameter))
                {
                    dbParameter = new SQLiteParameter($"P{pars.Count}", item);

                    pars.Add(item, dbParameter);
                }

                return $"@{dbParameter.ParameterName}";
            });

            parameters = new Realize.ReadOnlyList<DbParameter>(pars.Values);

            return sql;
        }

        /// <summary>
        /// 生成从数据源中查询某一字段的 Sql 查询语句。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">查询字段所归属的数据源。</param>
        /// <param name="selectField">待生成 Sql 的查询字段。</param>
        /// <param name="useFieldAlias">生成的查询语句是否应当应用上字段别名。</param>
        /// <param name="addParameter">添加 Sql 所需的参数方法（参数为需要添加的参数，返回值代表该参数的变量名）。</param>
        /// <returns>生成好的 Sql 查询语句。</returns>
        private string GenerateQuerySql(IDataContext dataContext, BasicDataSource dataSource, Field.Field selectField, bool useFieldAlias, Func<object, string> addParameter)
        {
            StringBuilder sqlFields = new StringBuilder();
            StringBuilder orderBy = new StringBuilder();
            StringBuilder groupBy = new StringBuilder();
            string sqlFrom = GenerateFromSql(dataContext, dataSource, false, addParameter);
            string where = string.Empty;
            string distinct = dataSource.IsDistinctly ? "distinct " : string.Empty;
            string limit = string.Empty;

            AppendSelectField(dataContext, dataSource, selectField, sqlFields, new HashSet<Field.Field>(), useFieldAlias, addParameter);
            dataSource.Where?.InitExpressionSQL(dataContext, dataSource, addParameter);

            if (dataSource.SortItems != null && dataSource.SortItems.Count > 0)
            {
                orderBy.Append(" order by ");

                foreach (var item in dataSource.SortItems)
                {
                    if (orderBy.Length > 10)
                        orderBy.Append(", ");

                    item.Field.InitExpressionSQL(dataContext, dataSource, addParameter);

                    if (item.Field.Type == FieldType.Subquery)
                        orderBy.AppendFormat("({0})", item.Field.ExpressionInfo.SQL);
                    else
                        orderBy.Append(item.Field.ExpressionInfo.SQL);

                    if (item.Type == SortType.Descending)
                        orderBy.Append(" desc");
                }
            }

            if (dataSource.GroupFields != null && dataSource.GroupFields.Count > 0)
            {
                groupBy.Append(" group by ");

                foreach (var item in dataSource.GroupFields)
                {
                    if (groupBy.Length > 10)
                        groupBy.Append(", ");

                    item.InitExpressionSQL(dataContext, dataSource, addParameter);

                    if (item.Type == FieldType.Subquery)
                        groupBy.AppendFormat("({0})", item.ExpressionInfo.SQL);
                    else
                        groupBy.Append(item.ExpressionInfo.SQL);
                }
            }

            if (dataSource.Where != null)
            {
                dataSource.Where.InitExpressionSQL(dataContext, dataSource, addParameter);

                where = $" where {dataSource.Where.BooleanInfo.SQL}";
            }

            if (dataSource.Limit != null && dataSource.Limit.HasValue)
            {
                if (dataSource.Limit.Value.Start == 0)
                    limit = $" limit {dataSource.Limit.Value.Count}";
                else
                    limit = $" limit {dataSource.Limit.Value.Count} offset {dataSource.Limit.Value.Start}";
            }

            return $"select {distinct}{sqlFields} from {sqlFrom}{where}{groupBy}{orderBy}{limit}";
        }

        /// <summary>
        /// 生成指定数据源在被用作 Select 查询中的 From 数据源时的 Sql。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">查询字段所归属的数据源。</param>
        /// <param name="forceQuery">若是原生数据源是否强制启用查询。</param>
        /// <param name="addParameter">添加 Sql 所需的参数方法（参数为需要添加的参数，返回值代表该参数的变量名）。</param>
        /// <returns>生成好的 Sql 查询语句。</returns>
        private string GenerateFromSql(IDataContext dataContext, DataSource.DataSource dataSource, bool forceQuery, Func<object, string> addParameter)
        {
            if (dataSource.Type == DataSourceType.Table || dataSource.Type == DataSourceType.View)
            {
                OriginalDataSource originalDataSource = (OriginalDataSource)dataSource;

                if (forceQuery && originalDataSource.SelectField != null)
                    return $"({GenerateQuerySql(dataContext, originalDataSource, originalDataSource.SelectField, true, addParameter)}) as {originalDataSource.Alias}";
                else
                    return $"{originalDataSource.Name} as {originalDataSource.Alias}";
            }
            else if (dataSource.Type == DataSourceType.QueryResult)
            {
                ResultDataSource resultDataSource = (ResultDataSource)dataSource;

                if (forceQuery)
                    return $"({GenerateQuerySql(dataContext, resultDataSource, resultDataSource.SelectField ?? resultDataSource.RootField, true, addParameter)}) as {resultDataSource.Alias}";
                else
                    return GenerateFromSql(dataContext, resultDataSource.FromDataSource, true, addParameter);
            }
            else
            {
                JoinDataSource joinDataSource = (JoinDataSource)dataSource;

                string left = GenerateFromSql(dataContext, joinDataSource.Left, true, addParameter);
                string right = GenerateFromSql(dataContext, joinDataSource.Right, true, addParameter);

                if (joinDataSource.Type == DataSourceType.CrossJoin)
                {
                    return $"{left} cross join {right}";
                }
                else
                {
                    string joinType;

                    switch (joinDataSource.Type)
                    {
                        case DataSourceType.LeftJoin:
                            joinType = "left join";
                            break;
                        case DataSourceType.RightJoin:
                            string temp = left;

                            left = right;

                            right = temp;

                            joinType = "left join";
                            break;
                        case DataSourceType.FullJoin:
                            throw new Exception("Sqlite 不支持全外连接查询方式");
                        default:
                            joinType = "inner join";
                            break;
                    }

                    joinDataSource.On.InitExpressionSQL(dataContext, joinDataSource, addParameter);

                    if (joinDataSource.On.Type == FieldType.Subquery)
                        return $"{left} {joinType} {right} on ({joinDataSource.On.BooleanInfo.SQL})";
                    else
                        return $"{left} {joinType} {right} on {joinDataSource.On.BooleanInfo.SQL}";
                }
            }
        }

        /// <summary>
        /// 追加待查询字段信息到字段集合中。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">查询字段所归属的数据源。</param>
        /// <param name="selectField">待追加的查询字段。</param>
        /// <param name="sqlFields">用于保存查询字段信息的字符串构造器。</param>
        /// <param name="appendedFields">已经追加过的字段集合。</param>
        /// <param name="useFieldAlias">生成的查询字段是否应当应用上字段别名。</param>
        /// <param name="addParameter">若查询字段需要添加参数时的添加方法。</param>
        private void AppendSelectField(IDataContext dataContext, DataSource.DataSource dataSource, Field.Field selectField, StringBuilder sqlFields, HashSet<Field.Field> appendedFields, bool useFieldAlias, Func<object, string> addParameter)
        {
            if (!appendedFields.Contains(selectField))
            {
                if (selectField.Type == FieldType.Object)
                {
                    ObjectField objectField = (ObjectField)selectField;

                    if (objectField.ConstructorInfo.Parameters != null && objectField.ConstructorInfo.Parameters.Count > 0)
                    {
                        foreach (var item in objectField.ConstructorInfo.Parameters)
                        {
                            AppendSelectField(dataContext, dataSource, item, sqlFields, appendedFields, useFieldAlias, addParameter);
                        }
                    }

                    if (objectField.Members != null && objectField.Members.Count > 0)
                    {
                        foreach (var item in objectField.Members)
                        {
                            AppendSelectField(dataContext, dataSource, item.Value.Field, sqlFields, appendedFields, useFieldAlias, addParameter);
                        }
                    }
                }
                else if (selectField.Type == FieldType.Collection)
                {
                    CollectionField collectionField = (CollectionField)selectField;

                    if (collectionField.ConstructorInfo.Parameters != null && collectionField.ConstructorInfo.Parameters.Count > 0)
                    {
                        foreach (var item in collectionField.ConstructorInfo.Parameters)
                        {
                            AppendSelectField(dataContext, dataSource, item, sqlFields, appendedFields, useFieldAlias, addParameter);
                        }
                    }

                    foreach (var item in collectionField)
                    {
                        AppendSelectField(dataContext, dataSource, item, sqlFields, appendedFields, useFieldAlias, addParameter);
                    }
                }
                else if (selectField is BasicField basicField)
                {
                    basicField.InitExpressionSQL(dataContext, dataSource, addParameter);

                    if (sqlFields.Length > 0)
                        sqlFields.Append(", ");

                    if (basicField.Type == FieldType.Subquery)
                        sqlFields.Append($"({basicField.ExpressionInfo.SQL})");
                    else
                        sqlFields.Append(basicField.ExpressionInfo.SQL);

                    if (useFieldAlias && !string.IsNullOrWhiteSpace(basicField.Alias))
                        sqlFields.Append($" as {basicField.Alias}");
                }

                appendedFields.Add(selectField);
            }
        }

        /// <summary>
        /// 初始化指定二元操作字段的 Sql 表示信息。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">待生成 Sql 表示信息字段所归属的数据源信息。</param>
        /// <param name="field">待生成 Sql 表示信息的字段。</param>
        /// <param name="addParameter">添加 Sql 所需的参数方法（参数为需要添加的参数，返回值代表该参数的变量名）。</param>
        /// <returns>生成好的表示 Sql 信息。</returns>
        public ExpressionInfo InitBinaryField(IDataContext dataContext, DataSource.DataSource dataSource, BinaryField field, Func<object, string> addParameter)
        {
            string rightSql = null;
            string leftSql = null;
            string sql = null;

            if (field.DataType.FullName != _BOOL_TYPE_NAME)
            {
                field.Left.InitExpressionSQL(dataContext, dataSource, addParameter);
                field.Right.InitExpressionSQL(dataContext, dataSource, addParameter);

                leftSql = field.Left.Type == FieldType.Subquery || (field.OperationType != OperationType.Coalesce && field.OperationType != OperationType.Power && Helper.CheckIsPriority(field.OperationType, field.Left.ExpressionInfo.Type, false)) ? $"({field.Left.ExpressionInfo.SQL})" : field.Left.ExpressionInfo.SQL;
                rightSql = field.Right.Type == FieldType.Subquery || (field.OperationType != OperationType.Coalesce && field.OperationType != OperationType.Power && Helper.CheckIsPriority(field.OperationType, field.Right.ExpressionInfo.Type, true)) ? $"({field.Right.ExpressionInfo.SQL})" : field.Right.ExpressionInfo.SQL;
            }

            OperationType useType = field.OperationType;

            switch (field.OperationType)
            {

                case OperationType.Add:
                    if (field.Left.DataType.FullName == _STRING_TYPE_NAME || field.Right.DataType.FullName == _STRING_TYPE_NAME)
                        sql = $"{leftSql} || {rightSql}";
                    else
                        sql = $"{leftSql} + {rightSql}";
                    break;
                case OperationType.And:
                    sql = $"{leftSql} & {rightSql}";
                    break;
                case OperationType.Divide:
                    sql = $"{leftSql} / {rightSql}";
                    break;
                case OperationType.Coalesce:
                    sql = $"ifnull({leftSql}, {rightSql})";

                    useType = OperationType.Call;
                    break;
                case OperationType.ExclusiveOr:
                    throw new Exception("Sqlite 不支持按位异或操作");
                case OperationType.GreaterThan:
                case OperationType.GreaterThanOrEqual:
                case OperationType.LessThan:
                case OperationType.LessThanOrEqual:
                case OperationType.Equal:
                case OperationType.NotEqual:
                case OperationType.NotIn:
                case OperationType.In:
                case OperationType.Like:
                case OperationType.NotLike:
                case OperationType.AndAlso:
                case OperationType.OrElse:
                    return field.BooleanInfo;
                case OperationType.LeftShift:
                    sql = $"{leftSql} << {rightSql})";
                    break;
                case OperationType.Modulo:
                    sql = $"{leftSql} % {rightSql}";
                    break;
                case OperationType.Multiply:
                    sql = $"{leftSql} * {rightSql}";
                    break;
                case OperationType.Or:
                    sql = $"{leftSql} | {rightSql}";
                    break;
                case OperationType.Power:
                    sql = $"power({leftSql}, {rightSql})";

                    useType = OperationType.Call;
                    break;
                case OperationType.RightShift:
                    sql = $"{leftSql} >> {rightSql})";
                    break;
                case OperationType.Subtract:
                    sql = $"{leftSql} - {rightSql}";
                    break;
                case OperationType.ArrayIndex:
                    throw new Exception("Sql Server 不支持数组或集合类型的字段操作");
            }

            return new ExpressionInfo(sql, useType);
        }

        /// <summary>
        /// 初始化指定三元操作（条件操作）字段的 Sql 表示信息。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">待生成 Sql 表示信息字段所归属的数据源信息。</param>
        /// <param name="field">待生成 Sql 表示信息的字段。</param>
        /// <param name="addParameter">添加 Sql 所需的参数方法（参数为需要添加的参数，返回值代表该参数的变量名）。</param>
        /// <returns>生成好的表示 Sql 信息。</returns>
        public ExpressionInfo InitConditionalField(IDataContext dataContext, DataSource.DataSource dataSource, ConditionalField field, Func<object, string> addParameter)
        {
            field.Test.InitExpressionSQL(dataContext, dataSource, addParameter);
            field.IfTrue.InitExpressionSQL(dataContext, dataSource, addParameter);
            field.IfFalse.InitExpressionSQL(dataContext, dataSource, addParameter);

            string testSql = field.Test.Type == FieldType.Subquery ? $"({field.Test.BooleanInfo.SQL})" : field.Test.BooleanInfo.SQL;
            string ifTrueSql = field.IfTrue.Type == FieldType.Subquery ? $"({field.IfTrue.ExpressionInfo.SQL})" : field.IfTrue.ExpressionInfo.SQL;
            string ifFalseSql = field.IfFalse.Type == FieldType.Subquery ? $"({field.IfFalse.ExpressionInfo.SQL})" : field.IfFalse.ExpressionInfo.SQL;

            return new ExpressionInfo($"iif({testSql}, {ifTrueSql}, {ifFalseSql})", OperationType.Call);
        }

        /// <summary>
        /// 初始化指定常量字段的 Sql 表示信息。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">待生成 Sql 表示信息字段所归属的数据源信息。</param>
        /// <param name="field">待生成 Sql 表示信息的字段。</param>
        /// <param name="addParameter">添加 Sql 所需的参数方法（参数为需要添加的参数，返回值代表该参数的变量名）。</param>
        /// <returns>生成好的表示 Sql 信息。</returns>
        public ExpressionInfo InitConstantField(IDataContext dataContext, DataSource.DataSource dataSource, ConstantField field, Func<object, string> addParameter)
        {
            //只有基础数据类型才能作为参数传入 Sql Server
            if (Helper.CheckIsBasicType(field.DataType))
                return new ExpressionInfo(addParameter(field.Value), OperationType.Default);
            else if (field.Value is Queryable)
                throw new Exception("Sqlite 子查询不支持多行数据返回，若要使用子查询，可在 Queryable 对象最后调用一次 First() 函数或 Last() 函数将其限定只返回一行数据");
            else if (field.Value is MultipleJoin)
                throw new Exception("子查询不支持直接返回多表关联查询对象（MultipleJoin），若要使用子查询，可在 MultipleJoin 对象上调用一次 Select 函数并再次调用 First 方法即可");
            else
                throw new Exception("Sqlite 只支持基础数据类型的常量作为参数传入");
        }

        /// <summary>
        /// 初始化指定基础数据类型字段被直接用做 Where、On、Case 等条件判定时的 Sql 表示信息（如原始 Bit 类型字段直接用作 Where 条件时需要写成 Table.FieldName = 1）。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">待生成 Sql 表示信息字段所归属的数据源信息。</param>
        /// <param name="field">需要生成 Where、On、Case 等条件判定表示 Sql 信息的字段。</param>
        /// <param name="addParameter">添加 Sql 所需的参数方法（参数为需要添加的参数，返回值代表该参数的变量名）。</param>
        /// <returns>生成好用于在 Where、On、Case 等条件判定时的 Sql 表示信息。</returns>
        public ExpressionInfo InitFieldWhere(IDataContext dataContext, DataSource.DataSource dataSource, BasicField field, Func<object, string> addParameter)
        {
            if (field.Type == FieldType.Binary)
            {
                BinaryField binaryField = (BinaryField)field;

                //只有二元运算中的这几种操作可直接用于条件判定
                if (binaryField.OperationType == OperationType.Equal
                    || binaryField.OperationType == OperationType.NotEqual
                    || binaryField.OperationType == OperationType.LessThan
                    || binaryField.OperationType == OperationType.LessThanOrEqual
                    || binaryField.OperationType == OperationType.GreaterThan
                    || binaryField.OperationType == OperationType.GreaterThanOrEqual
                    || binaryField.OperationType == OperationType.AndAlso
                    || binaryField.OperationType == OperationType.OrElse
                    || binaryField.OperationType == OperationType.In
                    || binaryField.OperationType == OperationType.NotIn
                    || binaryField.OperationType == OperationType.Like
                    || binaryField.OperationType == OperationType.NotLike)
                {
                    binaryField.Left.InitExpressionSQL(dataContext, dataSource, addParameter);

                    if (binaryField.OperationType == OperationType.In || binaryField.OperationType == OperationType.NotIn)
                    {
                        string containType = binaryField.OperationType == OperationType.NotIn ? "not in" : "in";
                        string leftSql = binaryField.Left.Type == FieldType.Subquery || Helper.CheckIsPriority(binaryField.OperationType, binaryField.Left.ExpressionInfo.Type, false) ? $"({binaryField.Left.ExpressionInfo.SQL})" : binaryField.Left.ExpressionInfo.SQL;

                        if (binaryField.Left.DataType.FullName == _STRING_TYPE_NAME && !dataContext.IsCaseSensitive)
                            leftSql = $"{leftSql} {_CASE_SENSITIVE_MARK}";

                        if (binaryField.Right.Type == FieldType.Subquery)
                        {
                            binaryField.Right.InitExpressionSQL(dataContext, dataSource, addParameter);

                            return new ExpressionInfo($"{leftSql} {containType} ({binaryField.Right.ExpressionInfo.SQL})", binaryField.OperationType);
                        }
                        //如果是常量字段且常量字段的值是 IEnumerable 类型
                        else if (binaryField.Right.Type == FieldType.Constant && ((binaryField.Right.DataType.IsArray && binaryField.Right.DataType.GetArrayRank() == 1 && Helper.CheckIsBasicType(binaryField.Right.DataType.GetElementType())) || (binaryField.Right.DataType.IsGenericType && binaryField.Right.DataType.GetGenericArguments().Length == 1)))
                        {
                            IEnumerable enumerable = (IEnumerable)((ConstantField)binaryField.Right).Value;

                            StringBuilder collection = new StringBuilder();

                            foreach (var item in enumerable)
                            {
                                if (collection.Length > 0)
                                    collection.Append(", ");

                                collection.Append(addParameter(item));
                            }

                            return new ExpressionInfo($"{leftSql} {containType} ({collection})", OperationType.Default);
                        }

                        throw new Exception("对用于子查询或 Where 条件的 Contains 方法，被调用对象 IEnumerable<T> 的成员（T）类型必须是基础数据类型");
                    }
                    else
                    {
                        binaryField.Right.InitExpressionSQL(dataContext, dataSource, addParameter);

                        if (binaryField.OperationType == OperationType.AndAlso || binaryField.OperationType == OperationType.OrElse)
                        {
                            string leftSql = binaryField.Left.Type == FieldType.Subquery || Helper.CheckIsPriority(binaryField.OperationType, binaryField.Left.BooleanInfo.Type, false) ? $"({binaryField.Left.BooleanInfo.SQL})" : binaryField.Left.BooleanInfo.SQL;
                            string rightSql = binaryField.Right.Type == FieldType.Subquery || Helper.CheckIsPriority(binaryField.OperationType, binaryField.Right.BooleanInfo.Type, true) ? $"({binaryField.Right.BooleanInfo.SQL})" : binaryField.Right.BooleanInfo.SQL;

                            if (binaryField.OperationType == OperationType.AndAlso)
                                return new ExpressionInfo($"{leftSql} and {rightSql}", OperationType.AndAlso);
                            else
                                return new ExpressionInfo($"{leftSql} or {rightSql}", OperationType.OrElse);
                        }
                        else
                        {
                            string leftSql = binaryField.Left.Type == FieldType.Subquery || Helper.CheckIsPriority(binaryField.OperationType, binaryField.Left.ExpressionInfo.Type, false) ? $"({binaryField.Left.ExpressionInfo.SQL})" : binaryField.Left.ExpressionInfo.SQL;
                            string rightSql = binaryField.Right.Type == FieldType.Subquery || Helper.CheckIsPriority(binaryField.OperationType, binaryField.Right.ExpressionInfo.Type, true) ? $"({binaryField.Right.ExpressionInfo.SQL})" : binaryField.Right.ExpressionInfo.SQL;

                            switch (binaryField.OperationType)
                            {
                                case OperationType.Equal:
                                    if (!dataContext.IsCaseSensitive && binaryField.Left.DataType.FullName == _STRING_TYPE_NAME && binaryField.Right.DataType.FullName == _STRING_TYPE_NAME)
                                        return new ExpressionInfo($"{leftSql} {_CASE_SENSITIVE_MARK} = {rightSql}", OperationType.Equal);
                                    else
                                        return new ExpressionInfo($"{leftSql} = {rightSql}", OperationType.Equal);
                                case OperationType.NotEqual:
                                    if (!dataContext.IsCaseSensitive && binaryField.Left.DataType.FullName == _STRING_TYPE_NAME && binaryField.Right.DataType.FullName == _STRING_TYPE_NAME)
                                        return new ExpressionInfo($"{leftSql} {_CASE_SENSITIVE_MARK} != {rightSql}", OperationType.Equal);
                                    else
                                        return new ExpressionInfo($"{leftSql} != {rightSql}", OperationType.NotEqual);
                                case OperationType.LessThan:
                                    return new ExpressionInfo($"{leftSql} < {rightSql}", OperationType.LessThan);
                                case OperationType.LessThanOrEqual:
                                    return new ExpressionInfo($"{leftSql} <= {rightSql}", OperationType.LessThanOrEqual);
                                case OperationType.GreaterThan:
                                    return new ExpressionInfo($"{leftSql} > {rightSql}", OperationType.GreaterThan);
                                case OperationType.GreaterThanOrEqual:
                                    return new ExpressionInfo($"{leftSql} >= {rightSql}", OperationType.GreaterThanOrEqual);
                                case OperationType.Like:
                                case OperationType.NotLike:
                                    if (!dataContext.IsCaseSensitive)
                                        return new ExpressionInfo($"{leftSql} {(binaryField.OperationType == OperationType.Like ? "like" : "not like")} {rightSql} {_CASE_SENSITIVE_MARK}", OperationType.Like);
                                    else
                                        return new ExpressionInfo($"{leftSql} {(binaryField.OperationType == OperationType.Like ? "like" : "not like")} {rightSql}", OperationType.Like);
                            }
                        }
                    }
                }
            }
            else if (field.Type == FieldType.Unary && field.DataType.FullName == _BOOL_TYPE_NAME && ((UnaryField)field).OperationType == OperationType.Not)
            {
                UnaryField unaryField = (UnaryField)field;

                unaryField.Operand.InitExpressionSQL(dataContext, dataSource, unaryField.Operand.Type != FieldType.Original && unaryField.Operand.Type != FieldType.Quote, addParameter);

                string operandSql = unaryField.Operand.Type == FieldType.Subquery || (unaryField.Operand.Type != FieldType.Original && unaryField.Operand.Type != FieldType.Quote) || Helper.CheckIsPriority(OperationType.Equal, unaryField.Operand.ExpressionInfo.Type, false) ? $"({unaryField.Operand.ExpressionInfo.SQL})" : unaryField.Operand.ExpressionInfo.SQL;

                return new ExpressionInfo($"{operandSql} = false", OperationType.Equal);
            }
            else if (field.Type == FieldType.Method)
            {
                MethodField methodField = (MethodField)field;

                //string 类型的 StartsWith 或 Contains 方法，不支持多参数的 StartsWith 或 Contains 方法
                if (methodField.ObjectField != null && methodField.ObjectField is BasicField basicField && (methodField.MethodInfo.Name == "StartsWith" || methodField.MethodInfo.Name == "Contains") && methodField.Parameters != null && methodField.Parameters.Count == 1 && methodField.Parameters[0] is BasicField parameter)
                {
                    basicField.InitExpressionSQL(dataContext, dataSource, addParameter);
                    parameter.InitExpressionSQL(dataContext, dataSource, addParameter);

                    string objectSql = basicField.Type == FieldType.Subquery || Helper.CheckIsPriority(basicField.ExpressionInfo.Type, OperationType.Subtract, true) ? $"({basicField.ExpressionInfo.SQL})" : basicField.ExpressionInfo.SQL;
                    string searchString = parameter.Type == FieldType.Subquery ? $"({parameter.ExpressionInfo.SQL})" : parameter.ExpressionInfo.SQL;
                    string checkString = methodField.MethodInfo.Name == "StartsWith" ? "=" : ">=";

                    if (!dataContext.IsCaseSensitive)
                        return new ExpressionInfo($"instr(lower({objectSql}), lower({searchString})) {checkString} 1", OperationType.Call);
                    else
                        return new ExpressionInfo($"instr({objectSql}, {searchString}) {checkString} 1", OperationType.Call);
                }
            }

            field.InitExpressionSQL(dataContext, dataSource, false, addParameter);

            return field.ExpressionInfo;
        }

        /// <summary>
        /// 初始化指定成员调用字段的 Sql 表示信息。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">待生成 Sql 表示信息字段所归属的数据源信息。</param>
        /// <param name="field">待生成 Sql 表示信息的字段。</param>
        /// <param name="addParameter">添加 Sql 所需的参数方法（参数为需要添加的参数，返回值代表该参数的变量名）。</param>
        /// <returns>生成好的表示 Sql 信息。</returns>
        public ExpressionInfo InitMemberField(IDataContext dataContext, DataSource.DataSource dataSource, MemberField field, Func<object, string> addParameter)
        {
            if (field.ObjectField != null && field.ObjectField is BasicField basicField)
            {
                //若调用实例为 string 类型
                if (field.ObjectField.DataType.FullName == _STRING_TYPE_NAME)
                {
                    if (field.MemberInfo.Name == "Length" && field.MemberInfo.MemberType == MemberTypes.Property)
                    {
                        basicField.InitExpressionSQL(dataContext, dataSource, addParameter);

                        string objectSql = field.Type == FieldType.Subquery ? $"({basicField.ExpressionInfo.SQL})" : basicField.ExpressionInfo.SQL;

                        return new ExpressionInfo($"length({objectSql})", OperationType.Call);
                    }
                }
                //若调用实例为 DateTime 类型且成员类型为属性
                else if (field.ObjectField.DataType.FullName == _DATETIME_TYPE_NAME && field.MemberInfo.MemberType == MemberTypes.Property)
                {
                    if (field.MemberInfo.Name == "Year"
                        || field.MemberInfo.Name == "Month"
                        || field.MemberInfo.Name == "Day"
                        || field.MemberInfo.Name == "Hour"
                        || field.MemberInfo.Name == "Minute"
                        || field.MemberInfo.Name == "Second"
                        || field.MemberInfo.Name == "Millisecond")
                    {
                        basicField.InitExpressionSQL(dataContext, dataSource, addParameter);

                        string dateTimeSql = basicField.Type == FieldType.Subquery ? $"({basicField.ExpressionInfo.SQL})" : basicField.ExpressionInfo.SQL;
                        string format = null;

                        switch (field.MemberInfo.Name)
                        {
                            case "Year":
                                format = "%Y";
                                break;
                            case "Month":
                                format = "%m";
                                break;
                            case "Day":
                                format = "%d";
                                break;
                            case "Hour":
                                format = "%H";
                                break;
                            case "Minute":
                                format = "%M";
                                break;
                            case "Second":
                                format = "%S";
                                break;
                        }

                        if (format != null)
                            return new ExpressionInfo($"cast(strftime('{format}', {dateTimeSql}) as int)", OperationType.Call);
                        else
                            return new ExpressionInfo($"cast(substr(strftime('%f', {dateTimeSql}), 4) as int)", OperationType.Call);
                    }
                    else if (field.MemberInfo.Name == "Date")
                    {
                        basicField.InitExpressionSQL(dataContext, dataSource, addParameter);

                        return new ExpressionInfo($"date({basicField.ExpressionInfo.SQL})", OperationType.Call);
                    }
                }
            }

            throw new Exception($"未能将调用 {field.MemberInfo.ReflectedType.FullName} 类中的 {field.MemberInfo.Name} 成员转换成 Sql 表示信息");
        }

        /// <summary>
        /// 初始化指定方法调用字段的 Sql 表示信息。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">待生成 Sql 表示信息字段所归属的数据源信息。</param>
        /// <param name="field">待生成 Sql 表示信息的字段。</param>
        /// <param name="addParameter">添加 Sql 所需的参数方法（参数为需要添加的参数，返回值代表该参数的变量名）。</param>
        /// <returns>生成好的表示 Sql 信息。</returns>
        public ExpressionInfo InitMethodField(IDataContext dataContext, DataSource.DataSource dataSource, MethodField field, Func<object, string> addParameter)
        {
            if (field.ObjectField != null)
            {
                //调用实例字段是基础数据字段
                if (field.ObjectField is BasicField basicField)
                {
                    //若调用实例为 string 类型
                    if (field.ObjectField.DataType.FullName == _STRING_TYPE_NAME)
                    {
                        //IndexOf 方法
                        if (field.MethodInfo.Name == "IndexOf" && field.Parameters != null && field.Parameters.Count == 1 && field.Parameters[0] is BasicField parameter)
                        {
                            basicField.InitExpressionSQL(dataContext, dataSource, addParameter);
                            parameter.InitExpressionSQL(dataContext, dataSource, addParameter);

                            string objectSql = basicField.Type == FieldType.Subquery ? $"({basicField.ExpressionInfo.SQL})" : basicField.ExpressionInfo.SQL;
                            string searchString = parameter.Type == FieldType.Subquery ? $"({parameter.ExpressionInfo.SQL})" : parameter.ExpressionInfo.SQL;

                            if (!dataContext.IsCaseSensitive)
                            {
                                objectSql = $"lower({objectSql})";
                                searchString = $"lower({searchString})";
                            }

                            return new ExpressionInfo($"instr({objectSql}, {searchString}) - 1", OperationType.Subtract);
                        }
                        //Substring 方法
                        else if (field.MethodInfo.Name == "Substring" && field.Parameters != null && (field.Parameters.Count == 1 || field.Parameters.Count == 2))
                        {
                            BasicField startField = (BasicField)field.Parameters[0];

                            startField.InitExpressionSQL(dataContext, dataSource, addParameter);
                            basicField.InitExpressionSQL(dataContext, dataSource, addParameter);

                            string parameterStartSql = startField.Type == FieldType.Subquery || Helper.CheckIsPriority(OperationType.Add, startField.ExpressionInfo.Type, false) ? $"({startField.ExpressionInfo.SQL})" : startField.ExpressionInfo.SQL;
                            string parameterStringSql = basicField.Type == FieldType.Subquery ? $"({basicField.ExpressionInfo.SQL})" : basicField.ExpressionInfo.SQL;

                            if (field.Parameters.Count == 1)
                            {
                                return new ExpressionInfo($"substr({parameterStringSql}, {parameterStartSql} + 1)", OperationType.Call);
                            }
                            else
                            {
                                BasicField lengthField = (BasicField)field.Parameters[1];

                                lengthField.InitExpressionSQL(dataContext, dataSource, addParameter);

                                string lengthSql = lengthField.Type == FieldType.Subquery ? $"({lengthField.ExpressionInfo.SQL})" : lengthField.ExpressionInfo.SQL;

                                return new ExpressionInfo($"substring({parameterStringSql}, {parameterStartSql} + 1, {lengthSql})", OperationType.Call);
                            }
                        }
                        //Trim、ToUpper 或 ToLower 方法
                        else if ((field.MethodInfo.Name == "Trim" || field.MethodInfo.Name == "ToUpper" || field.MethodInfo.Name == "ToLower") && (field.Parameters == null || field.Parameters.Count < 1))
                        {
                            basicField.InitExpressionSQL(dataContext, dataSource, addParameter);

                            string objectSql = basicField.Type == FieldType.Subquery ? $"({basicField.ExpressionInfo.SQL})" : basicField.ExpressionInfo.SQL;

                            string methodName = field.MethodInfo.Name == "Trim" ? "trim" : field.MethodInfo.Name == "ToLower" ? "lower" : "upper";

                            return new ExpressionInfo($"{methodName}({objectSql})", OperationType.Call);
                        }
                        //StartsWith 或 Contains 方法，不支持多参数的 StartsWith 或 Contains 方法
                        else if ((field.MethodInfo.Name == "StartsWith" || field.MethodInfo.Name == "Contains") && field.Parameters != null && field.Parameters.Count == 1)
                        {
                            return field.BooleanInfo;
                        }
                        //Insert 或 Replace 方法
                        else if ((field.MethodInfo.Name == "Insert" || field.MethodInfo.Name == "Replace") && field.Parameters != null && field.Parameters.Count == 2 && field.Parameters[0] is BasicField && field.Parameters[1] is BasicField)
                        {
                            basicField.InitExpressionSQL(dataContext, dataSource, addParameter);
                            ((BasicField)field.Parameters[0]).InitExpressionSQL(dataContext, dataSource, addParameter);
                            ((BasicField)field.Parameters[1]).InitExpressionSQL(dataContext, dataSource, addParameter);

                            string objectSql = basicField.Type == FieldType.Subquery ? $"({basicField.ExpressionInfo.SQL})" : basicField.ExpressionInfo.SQL;
                            string parameter1 = field.Parameters[0].Type == FieldType.Subquery ? $"({((BasicField)field.Parameters[0]).ExpressionInfo.SQL})" : ((BasicField)field.Parameters[0]).ExpressionInfo.SQL;
                            string parameter2 = field.Parameters[1].Type == FieldType.Subquery ? $"({((BasicField)field.Parameters[1]).ExpressionInfo.SQL})" : ((BasicField)field.Parameters[1]).ExpressionInfo.SQL;

                            if (field.MethodInfo.Name == "Replace")
                                return new ExpressionInfo($"replace({objectSql}, {parameter1}, {parameter2})", OperationType.Call);
                            else
                                return new ExpressionInfo($"substr({objectSql}, 1, {parameter1}) || {parameter2} || substr({objectSql}, {(field.Parameters[0].Type != FieldType.Subquery && Helper.CheckIsPriority(OperationType.Add, ((BasicField)field.Parameters[0]).ExpressionInfo.Type, false) ? $"({parameter1})" : parameter1)} + 1, length({objectSql}) - {(field.Parameters[0].Type != FieldType.Subquery && Helper.CheckIsPriority(OperationType.Subtract, ((BasicField)field.Parameters[0]).ExpressionInfo.Type, true) ? $"({parameter1})" : parameter1)})", OperationType.Add);
                        }
                        else if (field.MethodInfo.Name == "Replace" && field.Parameters != null && field.Parameters.Count == 2 && field.Parameters[0] is BasicField && field.Parameters[1] is BasicField)
                        {
                            basicField.InitExpressionSQL(dataContext, dataSource, addParameter);
                            ((BasicField)field.Parameters[0]).InitExpressionSQL(dataContext, dataSource, addParameter);
                            ((BasicField)field.Parameters[1]).InitExpressionSQL(dataContext, dataSource, addParameter);

                            string objectSql = basicField.Type == FieldType.Subquery ? $"({basicField.ExpressionInfo.SQL})" : basicField.ExpressionInfo.SQL;
                            string index = field.Parameters[0].Type == FieldType.Subquery ? $"({((BasicField)field.Parameters[0]).ExpressionInfo.SQL})" : ((BasicField)field.Parameters[0]).ExpressionInfo.SQL;
                            string insertString = field.Parameters[1].Type == FieldType.Subquery ? $"({((BasicField)field.Parameters[1]).ExpressionInfo.SQL})" : ((BasicField)field.Parameters[1]).ExpressionInfo.SQL;

                            return new ExpressionInfo($"substr({objectSql}, 1, {index}) + {insertString} + substr({objectSql}, {(field.Parameters[0].Type != FieldType.Subquery && Helper.CheckIsPriority(OperationType.Add, ((BasicField)field.Parameters[0]).ExpressionInfo.Type, false) ? $"({index})" : index)} + 1, length({objectSql}) - {(field.Parameters[0].Type != FieldType.Subquery && Helper.CheckIsPriority(OperationType.Subtract, ((BasicField)field.Parameters[0]).ExpressionInfo.Type, true) ? $"({index})" : index)})", OperationType.Add);
                        }
                    }
                    //若调用实例为 DateTime 类型
                    else if (field.ObjectField.DataType.FullName == _DATETIME_TYPE_NAME)
                    {
                        //ToString 方法
                        if (field.MethodInfo.Name == "ToString")
                        {
                            string format = null;

                            if (field.Parameters != null && field.Parameters.Count > 0)
                            {
                                if (field.Parameters.Count == 1 && field.Parameters[0] is BasicField formatField)
                                {
                                    if (formatField.DataType.FullName != _STRING_TYPE_NAME)
                                        throw new Exception("Sql Server 日期格式化只支持固定的字符串格式参数");

                                    if (field.Type == FieldType.Constant)
                                    {
                                        format = ((ConstantField)formatField).Value?.ToString()
                                            .Replace("ss.fff", "%f")
                                            .Replace("ss", "%S")
                                            .Replace("mm", "%M")
                                            .Replace("hh", "%H")
                                            .Replace("yyyy", "%Y")
                                            .Replace("MM", "%m")
                                            .Replace("dd", "%d");
                                    }
                                    else
                                    {
                                        formatField.InitExpressionSQL(dataContext, dataSource, addParameter);

                                        format = formatField.Type == FieldType.Subquery ? $"({formatField.ExpressionInfo.SQL})" : formatField.ExpressionInfo.SQL;

                                        format = $"replace(replace(replace(replace(replace(replace(replace({format}, 'ss.fff', '%f'), 'ss', '%S'), 'mm', '%M'), 'hh', '%H'), 'yyyy', '%Y'), 'MM', '%m'), 'dd', '%d')";
                                    }
                                }
                                else
                                {
                                    throw new Exception("Sqlite 不支持多参数的日期格式化函数");
                                }
                            }

                            basicField.InitExpressionSQL(dataContext, dataSource, addParameter);

                            string dateTimeSql = basicField.Type == FieldType.Subquery ? $"({basicField.ExpressionInfo.SQL})" : basicField.ExpressionInfo.SQL;

                            return new ExpressionInfo($"strftime({format}, {dateTimeSql})", OperationType.Call);
                        }
                    }
                    //ToString 方法
                    else if (field.MethodInfo.Name == "ToString")
                    {
                        basicField.InitExpressionSQL(dataContext, dataSource, addParameter);

                        string objectSql = basicField.Type == FieldType.Subquery ? $"({basicField.ExpressionInfo.SQL})" : basicField.ExpressionInfo.SQL;

                        return new ExpressionInfo($"cast({objectSql} as varchar)", OperationType.Call);
                    }
                }
            }
            //若为静态方法且调用类型为 Convert 类型
            else if (field.MethodInfo.DeclaringType.FullName == "System.Convert")
            {
                //各种类型转换函数，如：ToInt32、ToDateTime 等
                if (field.MethodInfo.Name.StartsWith("To") && field.Parameters != null && field.Parameters.Count == 1 && field.Parameters[0] is BasicField basicField)
                {
                    string parameterSql = basicField.Type == FieldType.Subquery ? $"({basicField.ExpressionInfo.SQL})" : basicField.ExpressionInfo.SQL;

                    switch (field.MethodInfo.Name)
                    {
                        case "ToInt16":
                        case "ToIntU16":
                        case "ToInt32":
                        case "ToIntU32":
                        case "ToInt64":
                        case "ToIntU64":
                        case "ToBoolean":
                        case "ToByte":
                        case "ToSByte":
                        case "ToSingle":
                        case "ToDouble":
                        case "ToDecimal":
                            return new ExpressionInfo($"cast({parameterSql} as {dataContext.NetTypeToDBType(field.DataType)})", OperationType.Call);
                        case "ToDateTime":
                            return new ExpressionInfo($"datetime({parameterSql})", OperationType.Call);
                    }
                }
            }
            //如果是 Math 数学函数类
            else if (field.MethodInfo.ReflectedType.FullName == _MATH_TYPE_NAME)
            {
                //Math.Round 函数
                if (field.MethodInfo.Name == "Round" && field.Parameters != null && (field.Parameters.Count == 1 || (field.Parameters.Count == 2 && field.Parameters[1].DataType.FullName == _INT_TYPE_NAME)))
                {
                    ((BasicField)field.Parameters[0]).InitExpressionSQL(dataContext, dataSource, addParameter);

                    string numericSql = ((BasicField)field.Parameters[0]).Type == FieldType.Subquery ? $"({((BasicField)field.Parameters[0]).ExpressionInfo.SQL})" : ((BasicField)field.Parameters[0]).ExpressionInfo.SQL;

                    if (field.Parameters.Count == 1)
                    {
                        return new ExpressionInfo($"round({numericSql})", OperationType.Call);
                    }
                    else
                    {
                        ((BasicField)field.Parameters[1]).InitExpressionSQL(dataContext, dataSource, addParameter);

                        string lengthSql = ((BasicField)field.Parameters[1]).Type == FieldType.Subquery ? $"({((BasicField)field.Parameters[1]).ExpressionInfo.SQL})" : ((BasicField)field.Parameters[1]).ExpressionInfo.SQL;

                        return new ExpressionInfo($"round({numericSql}, {lengthSql})", OperationType.Call);
                    }
                }
                //Math.Floor 、 Math.Ceiling 或 Math.Abs 函数
                else if ((field.MethodInfo.Name == "Floor" || field.MethodInfo.Name == "Ceiling" || field.MethodInfo.Name == "Abs") && field.Parameters != null && field.Parameters.Count == 1)
                {
                    ((BasicField)field.Parameters[0]).InitExpressionSQL(dataContext, dataSource, addParameter);

                    string numericSql = ((BasicField)field.Parameters[0]).Type == FieldType.Subquery ? $"({((BasicField)field.Parameters[0]).ExpressionInfo.SQL})" : ((BasicField)field.Parameters[0]).ExpressionInfo.SQL;

                    return new ExpressionInfo($"{field.MethodInfo.Name.ToLower()}({numericSql})", OperationType.Call);
                }
                //Math.Pow 函数
                else if (field.MethodInfo.Name == "Pow" && field.Parameters != null && field.Parameters.Count == 2)
                {
                    ((BasicField)field.Parameters[0]).InitExpressionSQL(dataContext, dataSource, addParameter);
                    ((BasicField)field.Parameters[1]).InitExpressionSQL(dataContext, dataSource, addParameter);

                    string numericSql = ((BasicField)field.Parameters[0]).Type == FieldType.Subquery ? $"({((BasicField)field.Parameters[0]).ExpressionInfo.SQL})" : ((BasicField)field.Parameters[0]).ExpressionInfo.SQL;
                    string powerSql = ((BasicField)field.Parameters[1]).Type == FieldType.Subquery ? $"({((BasicField)field.Parameters[1]).ExpressionInfo.SQL})" : ((BasicField)field.Parameters[1]).ExpressionInfo.SQL;

                    return new ExpressionInfo($"power({numericSql}, {powerSql})", OperationType.Call);
                }
            }
            //如果是 DBFun 类的函数
            else if (field.MethodInfo.ReflectedType.FullName == _DBFunType.FullName)
            {
                //DBFun.Count 方法
                if (field.MethodInfo.Name == _DBFunCountMethodName)
                {
                    if (field.Parameters == null || field.Parameters.Count < 1)
                    {
                        return new ExpressionInfo("count(1)", OperationType.Call);
                    }
                    else if (field.Parameters.Count == 1)
                    {
                        if (field.Parameters[0] is BasicField basicField)
                        {
                            basicField.InitExpressionSQL(dataContext, dataSource, addParameter);

                            return new ExpressionInfo($"count({basicField.ExpressionInfo.SQL})", OperationType.Call);
                        }
                    }
                }
                //其他聚合方法（Max、Min、Sum、Avg）
                else if (field.Parameters != null && field.Parameters.Count == 1 && (field.MethodInfo.Name == _DBFunMaxMethodName || field.MethodInfo.Name == _DBFunSTDevMethodName || field.MethodInfo.Name == _DBFunSTDevPMethodName || field.MethodInfo.Name == _DBFunMinMethodName || field.MethodInfo.Name == _DBFunSumMethodName || field.MethodInfo.Name == _DBFunAvgMethodName || field.MethodInfo.Name == _DBFunVarMethodName || field.MethodInfo.Name == _DBFunVarPMethodName) && field.Parameters[0] is BasicField basicField)
                {
                    basicField.InitExpressionSQL(dataContext, dataSource, addParameter);

                    string methodName;

                    if (field.MethodInfo.Name == _DBFunMaxMethodName)
                        methodName = "max";
                    else if (field.MethodInfo.Name == _DBFunMinMethodName)
                        methodName = "min";
                    else if (field.MethodInfo.Name == _DBFunSumMethodName)
                        methodName = "sum";
                    else if (field.MethodInfo.Name == _DBFunAvgMethodName)
                        methodName = "avg";
                    else
                        throw new Exception($"Sqlite 不支持 DBFun.{field.MethodInfo.Name} 函数");

                    return new ExpressionInfo($"{methodName}({basicField.ExpressionInfo.SQL})", OperationType.Call);
                }
                //DBFun.NowTime 函数
                else if ((field.Parameters == null || field.Parameters.Count < 1) && field.MethodInfo.Name == _DBFunNowTimeMethodName)
                {
                    return new ExpressionInfo("datetimt('now', 'localtime')", OperationType.Call);
                }
                //DBFun.NewInt 函数
                else if ((field.Parameters == null || field.Parameters.Count < 1 || field.Parameters.Count == 2) && field.MethodInfo.Name == _DBFunNewIntMethodName)
                {
                    if (field.Parameters == null || field.Parameters.Count < 1)
                    {
                        return new ExpressionInfo("-2147483648 + round(abs(random() / 9223372036854775807.0) * 4294967295)", OperationType.Add);
                    }
                    else
                    {
                        ((BasicField)field.Parameters[0]).InitExpressionSQL(dataContext, dataSource, addParameter);
                        ((BasicField)field.Parameters[1]).InitExpressionSQL(dataContext, dataSource, addParameter);

                        string minSql = field.Parameters[0].Type == FieldType.Subquery || Helper.CheckIsPriority(OperationType.Add, ((BasicField)field.Parameters[0]).ExpressionInfo.Type, false) ? $"{((BasicField)field.Parameters[0]).ExpressionInfo.SQL}" : ((BasicField)field.Parameters[0]).ExpressionInfo.SQL;
                        string maxSql = field.Parameters[1].Type == FieldType.Subquery || Helper.CheckIsPriority(OperationType.Subtract, ((BasicField)field.Parameters[1]).ExpressionInfo.Type, false) ? $"{((BasicField)field.Parameters[1]).ExpressionInfo.SQL}" : ((BasicField)field.Parameters[1]).ExpressionInfo.SQL;

                        return new ExpressionInfo($"{minSql} + round(abs(random() / 9223372036854775807.0) * ({maxSql} - 1))", OperationType.Add);
                    }
                }
                //DBFun.NewLong 函数
                else if ((field.Parameters == null || field.Parameters.Count < 1) && field.MethodInfo.Name == _DBFunNewLongMethodName)
                {
                    return new ExpressionInfo("random()", OperationType.Call);
                }
                //DBFun 的各种日期差值计算函数
                else if (field.Parameters != null && field.MethodInfo.ReturnType.FullName == _INT_TYPE_NAME && field.Parameters.Count == 2 && field.Parameters[0].DataType.FullName == _DATETIME_TYPE_NAME && field.Parameters[1].DataType.FullName == _DATETIME_TYPE_NAME &&
                    (field.MethodInfo.Name == _DBFunDiffYearMethodName
                    || field.MethodInfo.Name == _DBFunDiffMonthMethodName
                    || field.MethodInfo.Name == _DBFunDiffDayMethodName
                    || field.MethodInfo.Name == _DBFunDiffHourMethodName
                    || field.MethodInfo.Name == _DBFunDiffMinuteMethodName
                    || field.MethodInfo.Name == _DBFunDiffSecondMethodName))
                {
                    BasicField objectField = (BasicField)field.Parameters[0];
                    BasicField compareField = (BasicField)field.Parameters[1];

                    objectField.InitExpressionSQL(dataContext, dataSource, addParameter);
                    compareField.InitExpressionSQL(dataContext, dataSource, addParameter);

                    string objectSql = objectField.Type == FieldType.Subquery || Helper.CheckIsPriority(OperationType.Subtract, objectField.ExpressionInfo.Type, false) ? $"({objectField.ExpressionInfo.SQL})" : objectField.ExpressionInfo.SQL;
                    string compareSql = compareField.Type == FieldType.Subquery || Helper.CheckIsPriority(OperationType.Subtract, compareField.ExpressionInfo.Type, true) ? $"({compareField.ExpressionInfo.SQL})" : compareField.ExpressionInfo.SQL;

                    if (field.MethodInfo.Name == _DBFunDiffYearMethodName)
                        return new ExpressionInfo($"strftime('%Y', {compareSql}) - strftime('%Y', {objectSql})", OperationType.Subtract);
                    else if (field.MethodInfo.Name == _DBFunDiffMonthMethodName)
                        return new ExpressionInfo($"(strftime('%Y', {compareSql}) - strftime('%Y', {objectSql})) * 12 + (strftime('%m', {compareSql}) - strftime('%m', {objectSql}))", OperationType.Add);
                    else if (field.MethodInfo.Name == _DBFunDiffDayMethodName)
                        return new ExpressionInfo($"(strftime('%s',date({compareSql})) - strftime('%s',date({objectSql}))) / 86400", OperationType.Divide);
                    else if (field.MethodInfo.Name == _DBFunDiffHourMethodName)
                        return new ExpressionInfo($"(strftime('%s',strftime('%Y-%m-%d %H:00:00', {compareSql})) - strftime('%s',strftime('%Y-%m-%d %H:00:00', {objectSql}))) / 3600", OperationType.Divide);
                    else if (field.MethodInfo.Name == _DBFunDiffMinuteMethodName)
                        return new ExpressionInfo($"(strftime('%s',strftime('%Y-%m-%d %H:%M:00', {compareSql})) - strftime('%s',strftime('%Y-%m-%d %H:%M:00', {objectSql}))) / 3600", OperationType.Divide);
                    else
                        return new ExpressionInfo($"strftime('%s',{compareSql}) - strftime('%s',{objectSql})", OperationType.Subtract);
                }
                //DNFun 的各种日期添加函数
                else if (field.Parameters != null && field.MethodInfo.ReturnType.FullName == _DATETIME_TYPE_NAME && field.Parameters.Count == 2 && field.Parameters[0].DataType.FullName == _DATETIME_TYPE_NAME && field.Parameters[1].DataType.FullName == _INT_TYPE_NAME &&
                    (field.MethodInfo.Name == _DBFunAddDayMethodName
                    || field.MethodInfo.Name == _DBFunAddHourMethodName
                    || field.MethodInfo.Name == _DBFunAddMinuteMethodName
                    || field.MethodInfo.Name == _DBFunAddSecondMethodName
                    || field.MethodInfo.Name == _DBFunAddMillisecondMethodName))
                {
                    BasicField objectField = (BasicField)field.Parameters[0];
                    BasicField valueField = (BasicField)field.Parameters[1];

                    objectField.InitExpressionSQL(dataContext, dataSource, addParameter);
                    valueField.InitExpressionSQL(dataContext, dataSource, addParameter);

                    string objectSql = objectField.Type == FieldType.Subquery ? $"({objectField.ExpressionInfo.SQL})" : objectField.ExpressionInfo.SQL;
                    string valueSql = valueField.Type == FieldType.Subquery || (field.MethodInfo.Name != _DBFunAddMillisecondMethodName && Helper.CheckIsPriority(OperationType.Multiply, valueField.ExpressionInfo.Type, true)) ? $"({valueField.ExpressionInfo.SQL})" : valueField.ExpressionInfo.SQL;

                    objectSql = $"cast(strftime('%s',{objectSql}) as int) * 1000 + cast(substr(strftime('%f',{objectSql}), 4) as int)";

                    if (field.MethodInfo.Name == _DBFunAddDayMethodName)
                        objectSql = $"{objectSql} + {valueSql} * 86400000";
                    else if (field.MethodInfo.Name == _DBFunAddHourMethodName)
                        objectSql = $"{objectSql} + {valueSql} * 3600000";
                    else if (field.MethodInfo.Name == _DBFunAddMinuteMethodName)
                        objectSql = $"{objectSql} + {valueSql} * 60000";
                    else if (field.MethodInfo.Name == _DBFunAddSecondMethodName)
                        objectSql = $"{objectSql} + {valueSql} * 1000";
                    else
                        objectSql = $"{objectSql} + {valueSql}";

                    return new ExpressionInfo($"datetime(({objectSql}) / 1000, 'unixepoch')", OperationType.Call);
                }
            }
            //各种 Parse 方法
            else if (field.MethodInfo.Name == "Parse" && field.Parameters != null && field.Parameters.Count == 1 && field.Parameters[0].DataType.FullName == _STRING_TYPE_NAME && field.Parameters[0] is BasicField parameterField)
            {
                //DateTime.Parse 方法
                if (field.DataType.FullName == _DATETIME_TYPE_NAME && field.MethodInfo.ReflectedType.FullName == _DATETIME_TYPE_NAME)
                {
                    parameterField.InitExpressionSQL(dataContext, dataSource, addParameter);

                    string parameterSql = parameterField.Type == FieldType.Subquery ? $"({parameterField.ExpressionInfo.SQL})" : parameterField.ExpressionInfo.SQL;

                    return new ExpressionInfo($"datetime({parameterSql})", OperationType.Call);
                }
                //int.Parse 方法
                else if (field.DataType.FullName == _INT_TYPE_NAME && field.MethodInfo.ReflectedType.FullName == _INT_TYPE_NAME)
                {
                    parameterField.InitExpressionSQL(dataContext, dataSource, addParameter);

                    string parameterSql = parameterField.Type == FieldType.Subquery ? $"({parameterField.ExpressionInfo.SQL})" : parameterField.ExpressionInfo.SQL;

                    return new ExpressionInfo($"cast({parameterSql} as int32)", OperationType.Call);
                }
                //double.Parse 或 decimal.Parse 方法
                else if ((field.DataType.FullName == _DOUBLE_TYPE_NAME || field.DataType.FullName == _DECIMAL_TYPE_NAME) && field.MethodInfo.ReflectedType.FullName == _DOUBLE_TYPE_NAME)
                {
                    parameterField.InitExpressionSQL(dataContext, dataSource, addParameter);

                    string parameterSql = parameterField.Type == FieldType.Subquery ? $"({parameterField.ExpressionInfo.SQL})" : parameterField.ExpressionInfo.SQL;

                    return new ExpressionInfo($"cast({parameterSql} as double)", OperationType.Call);
                }
                //long.Parse 方法
                else if (field.DataType.FullName == _LONG_TYPE_NAME && field.MethodInfo.ReflectedType.FullName == _LONG_TYPE_NAME)
                {
                    parameterField.InitExpressionSQL(dataContext, dataSource, addParameter);

                    string parameterSql = parameterField.Type == FieldType.Subquery ? $"({parameterField.ExpressionInfo.SQL})" : parameterField.ExpressionInfo.SQL;

                    return new ExpressionInfo($"cast({parameterSql} as int64)", OperationType.Call);
                }
                //short.Parse 方法
                else if (field.DataType.FullName == _SHORT_TYPE_NAME && field.MethodInfo.ReflectedType.FullName == _SHORT_TYPE_NAME)
                {
                    parameterField.InitExpressionSQL(dataContext, dataSource, addParameter);

                    string parameterSql = parameterField.Type == FieldType.Subquery ? $"({parameterField.ExpressionInfo.SQL})" : parameterField.ExpressionInfo.SQL;

                    return new ExpressionInfo($"cast({parameterSql} as int16)", OperationType.Call);
                }
                //sbyte.Parse 方法
                else if (field.DataType.FullName == _SBYTE_TYPE_NAME && field.MethodInfo.ReflectedType.FullName == _SBYTE_TYPE_NAME)
                {
                    parameterField.InitExpressionSQL(dataContext, dataSource, addParameter);

                    string parameterSql = parameterField.Type == FieldType.Subquery ? $"({parameterField.ExpressionInfo.SQL})" : parameterField.ExpressionInfo.SQL;

                    return new ExpressionInfo($"cast({parameterSql} as int8)", OperationType.Call);
                }
            }

            throw new Exception($"未能将调用 {field.MethodInfo.DeclaringType.FullName} 类中的 {field.MethodInfo.Name} 方法字段转换成 Sql 表示信息");
        }

        /// <summary>
        /// 初始化指定原始数据字段的 Sql 表示信息。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">待生成 Sql 表示信息字段所归属的数据源信息。</param>
        /// <param name="field">待生成 Sql 表示信息的字段。</param>
        /// <param name="addParameter">添加 Sql 所需的参数方法（参数为需要添加的参数，返回值代表该参数的变量名）。</param>
        /// <returns>生成好的表示 Sql 信息。</returns>
        public ExpressionInfo InitOriginalField(IDataContext dataContext, DataSource.DataSource dataSource, OriginalField field, Func<object, string> addParameter)
        {
            return new ExpressionInfo($"{((BasicDataSource)dataSource).Alias}.{field.FieldName}", OperationType.Default);
        }

        /// <summary>
        /// 初始化指定引用字段的 Sql 表示信息。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">待生成 Sql 表示信息字段所归属的数据源信息。</param>
        /// <param name="field">待生成 Sql 表示信息的字段。</param>
        /// <param name="addParameter">添加 Sql 所需的参数方法（参数为需要添加的参数，返回值代表该参数的变量名）。</param>
        /// <returns>生成好的表示 Sql 信息。</returns>
        public ExpressionInfo InitQuoteField(IDataContext dataContext, DataSource.DataSource dataSource, QuoteField field, Func<object, string> addParameter)
        {
            return new ExpressionInfo($"{field.UsingDataSource.Alias}.{field.UsingFieldName}", OperationType.Default);
        }

        /// <summary>
        /// 初始化指定子查询字段的 Sql 表示信息。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">待生成 Sql 表示信息字段所归属的数据源信息。</param>
        /// <param name="field">待生成 Sql 表示信息的字段。</param>
        /// <param name="addParameter">添加 Sql 所需的参数方法（参数为需要添加的参数，返回值代表该参数的变量名）。</param>
        /// <returns>生成好的表示 Sql 信息。</returns>
        public ExpressionInfo InitSubqueryField(IDataContext dataContext, DataSource.DataSource dataSource, SubqueryField field, Func<object, string> addParameter)
        {
            return new ExpressionInfo(GenerateQuerySql(dataContext, field.QueryDataSource, field.QueryField, false, addParameter), OperationType.Default);
        }

        /// <summary>
        /// 初始化指定一元操作字段的 Sql 表示信息。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">待生成 Sql 表示信息字段所归属的数据源信息。</param>
        /// <param name="field">待生成 Sql 表示信息的字段。</param>
        /// <param name="addParameter">添加 Sql 所需的参数方法（参数为需要添加的参数，返回值代表该参数的变量名）。</param>
        /// <returns>生成好的表示 Sql 信息。</returns>
        public ExpressionInfo InitUnaryField(IDataContext dataContext, DataSource.DataSource dataSource, UnaryField field, Func<object, string> addParameter)
        {
            field.Operand.InitExpressionSQL(dataContext, dataSource, addParameter);

            if (field.OperationType == OperationType.Not)
            {
                if (field.DataType.FullName == _BOOL_TYPE_NAME)
                {
                    return field.BooleanInfo;
                }
                else
                {
                    string operandSql = field.Operand.Type == FieldType.Subquery || Helper.CheckIsPriority(OperationType.Not, field.Operand.ExpressionInfo.Type, true) ? $"({field.Operand.ExpressionInfo.SQL})" : field.Operand.ExpressionInfo.SQL;

                    return new ExpressionInfo($"~{operandSql}", OperationType.Not);
                }
            }
            else if (field.OperationType == OperationType.Negate)
            {
                string operandSql = field.Operand.Type == FieldType.Subquery || Helper.CheckIsPriority(OperationType.Negate, field.Operand.ExpressionInfo.Type, true) ? $"({field.Operand.ExpressionInfo.SQL})" : field.Operand.ExpressionInfo.SQL;

                return new ExpressionInfo($"-{operandSql}", OperationType.Negate);
            }
            else if (field.OperationType == OperationType.Convert)
            {
                string operandSql = field.Operand.Type == FieldType.Subquery ? $"({field.Operand.ExpressionInfo.SQL})" : field.Operand.ExpressionInfo.SQL;

                if (field.DataType.FullName == _DATETIME_TYPE_NAME)
                    return new ExpressionInfo($"datetime({operandSql})", OperationType.Call);
                else
                    return new ExpressionInfo($"cast({operandSql} as {dataContext.NetTypeToDBType(field.DataType)})", OperationType.Call);
            }

            throw new Exception("未能将指定的一元操作字段转换成 Sql 表示信息");
        }

        /// <summary>
        /// 生成指定数据表的插入 Sql 语句。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">待生成插入 Sql 语句的原始数据源对象。</param>
        /// <param name="fields">需要插入的字段集合。</param>
        /// <param name="values">需要插入字段对应的值集合。</param>
        /// <param name="parameters">执行生成 Sql 所需使用的参数集合。</param>
        /// <returns>生成好的插入 Sql 语句。</returns>
        public string GenerateInsertSql(IDataContext dataContext, OriginalDataSource dataSource, Interface.IReadOnlyList<OriginalField> fields, Interface.IReadOnlyList<BasicField> values, out Interface.IReadOnlyList<DbParameter> parameters)
        {
            StringBuilder insertFields = new StringBuilder();
            StringBuilder insertValues = new StringBuilder();
            Dictionary<object, DbParameter> pars = new Dictionary<object, DbParameter>();

            string addParameterFun(object item)
            {
                if (!pars.TryGetValue(item, out DbParameter dbParameter))
                {
                    dbParameter = new SQLiteParameter($"P{pars.Count}", item);

                    pars.Add(item, dbParameter);
                }

                return $"@{dbParameter.ParameterName}";
            };

            for (int i = 0; i < fields.Count; i++)
            {
                OriginalField field = fields[i];
                BasicField valueField = values[i];

                valueField.InitExpressionSQL(dataContext, dataSource, addParameterFun);

                if (i > 0)
                {
                    insertFields.Append(",");
                    insertValues.Append(",");
                }

                insertFields.Append(field.FieldName);

                if (valueField.Type == FieldType.Subquery)
                    insertValues.Append($"({values[i].ExpressionInfo.SQL})");
                else
                    insertValues.Append($"{values[i].ExpressionInfo.SQL}");
            }

            parameters = new Realize.ReadOnlyList<DbParameter>(pars.Values);

            return $"insert into {dataSource.Name}({insertFields}) values ({insertValues})";
        }

        /// <summary>
        /// 生成指定数据表的插入 Sql 语句。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">待生成插入 Sql 语句的原始数据源对象。</param>
        /// <param name="fields">需要插入的字段集合。</param>
        /// <param name="entitys">需要插入的查询结果数据源。</param>
        /// <param name="parameters">执行生成 Sql 所需使用的参数集合。</param>
        /// <returns>生成好的插入 Sql 语句。</returns>
        public string GenerateInsertSql(IDataContext dataContext, OriginalDataSource dataSource, Interface.IReadOnlyList<OriginalField> fields, BasicDataSource entitys, out Interface.IReadOnlyList<DbParameter> parameters)
        {
            StringBuilder insertFields = new StringBuilder();

            foreach (var item in fields)
            {
                if (insertFields.Length > 0)
                    insertFields.Append(",");

                insertFields.Append(item.FieldName);
            }

            return $"insert into {dataSource.Name}({insertFields}) {GenerateQuerySql(dataContext, entitys, false, out parameters)}";
        }

        /// <summary>
        /// 生成指定数据表的数据删除 Sql 语句。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="deleteSources">待删除数据行的源数组。</param>
        /// <param name="fromSource">待删除数据行的来源数据。</param>
        /// <param name="where">删除时的条件限定字段信息。</param>
        /// <param name="parameters">执行生成 Sql 所需使用的参数集合。</param>
        /// <returns>生成好的数据删除 Sql 语句。</returns>
        public string GenerateDeleteSql(IDataContext dataContext, OriginalDataSource[] deleteSources, DataSource.DataSource fromSource, BasicField where, out Interface.IReadOnlyList<DbParameter> parameters)
        {
            if (deleteSources.Length == 1 && deleteSources[0] == fromSource)
            {
                Dictionary<object, DbParameter> pars = new Dictionary<object, DbParameter>();

                where?.InitExpressionSQL(dataContext, fromSource, item =>
                {
                    if (!pars.TryGetValue(item, out DbParameter dbParameter))
                    {
                        dbParameter = new SQLiteParameter($"P{pars.Count}", item);

                        pars.Add(item, dbParameter);
                    }

                    return $"@{dbParameter.ParameterName}";
                });

                string sql;

                if (where == null)
                    sql = $"delete from {deleteSources[0].Name}";
                else
                    sql = $"delete from {deleteSources[0].Name} as {deleteSources[0].Alias} where {where.BooleanInfo.SQL}";

                parameters = pars.Count > 0 ? new Realize.ReadOnlyList<DbParameter>(pars.Values) : null;

                return sql;
            }

            throw new Exception("Sqlite 不支持多表关联删除");
        }

        /// <summary>
        /// 生成指定数据表的数据更新 Sql 语句。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="modifyFields">需要修改的字段信息集合。</param>
        /// <param name="dataSource">待修改数据的来源数据。</param>
        /// <param name="where">修改时的条件限定字段信息。</param>
        /// <param name="parameters">执行生成 Sql 所需使用的参数集合。</param>
        /// <returns>生成好的数据更新 Sql 语句。</returns>
        public string GenerateUpdateSql(IDataContext dataContext, Interface.IReadOnlyList<ModifyInfo> modifyFields, DataSource.DataSource dataSource, BasicField where, out Interface.IReadOnlyList<DbParameter> parameters)
        {
            StringBuilder setSql = new StringBuilder();

            Dictionary<object, DbParameter> pars = new Dictionary<object, DbParameter>();

            string addParameterFun(object item)
            {
                if (!pars.TryGetValue(item, out DbParameter dbParameter))
                {
                    dbParameter = new SQLiteParameter($"P{pars.Count}", item);

                    pars.Add(item, dbParameter);
                }

                return $"@{dbParameter.ParameterName}";
            };

            if (dataSource.Type != DataSourceType.Table)
                throw new Exception("Sqlite 不支持关联修改操作");

            foreach (var item in modifyFields)
            {
                item.Value.InitExpressionSQL(dataContext, dataSource, addParameterFun);

                if (setSql.Length > 0)
                    setSql.Append(",");

                setSql.Append(item.Field.FieldName);
                setSql.Append("=");

                if (item.Value.Type == FieldType.Subquery)
                {

                    setSql.Append("(");
                    setSql.Append(item.Value.ExpressionInfo.SQL);
                    setSql.Append(")");
                }
                else
                {
                    setSql.Append(item.Value.ExpressionInfo.SQL);
                }
            }

            where?.InitExpressionSQL(dataContext, dataSource, addParameterFun);

            string result;

            if (where == null)
                result = $"update {((OriginalDataSource)dataSource).Name} as {((OriginalDataSource)dataSource).Alias} set {setSql}";
            else
                result = $"update {((OriginalDataSource)dataSource).Name} as {((OriginalDataSource)dataSource).Alias} set {setSql} where {where.BooleanInfo.SQL}";

            parameters = pars.Count > 0 ? new Realize.ReadOnlyList<DbParameter>(pars.Values) : null;

            return result;
        }

        /// <summary>
        /// 使用指定的原始数据源信息生成创建该数据表或视图的 T-Sql 语句。
        /// </summary>
        /// <param name="dataSource">待生成创建 T-Sql 语句的数据源信息。</param>
        /// <returns>生成好用于创建该数据表或视图的 T-Sql 语句。</returns>
        internal string GenerateCreateTableSql(OriginalDataSource dataSource)
        {
            StringBuilder fields = new StringBuilder();
            StringBuilder indices = new StringBuilder();

            foreach (var item in ((ObjectField)dataSource.RootField).Members)
            {
                OriginalField field = (OriginalField)item.Value.Field;

                if (fields.Length > 0)
                    fields.AppendFormat(",{0}", Environment.NewLine);

                fields.Append($"{field.FieldName} {field.FieldType}");

                if (field.IsAutoincrement && (!field.IsPrimaryKey || field.FieldType?.Trim().ToLower() != "integer"))
                    throw new Exception("Sqlite 的自增字段只能设置在类型为 integer 的主键字段上");

                if (field.IsPrimaryKey)
                {
                    fields.Append(" primary key");

                    if (field.IsAutoincrement)
                        fields.Append(" autoincrement");

                    fields.Append(" not null");
                }
                else
                {
                    if (field.IsUnique)
                        fields.Append(" unique");

                    if (!field.IsNullable)
                        fields.Append(" not null");
                }

                if (field.DefaultValue != null)
                {
                    string valueType = field.DataType.FullName;

                    if (valueType == _INT_TYPE_NAME
                        || valueType == "System.UInt32"
                        || valueType == "System.UInt64"
                        || valueType == "System.UInt16"
                        || valueType == "System.Byte"
                        || valueType == _LONG_TYPE_NAME
                        || valueType == _SBYTE_TYPE_NAME
                        || valueType == _SHORT_TYPE_NAME
                        || valueType == _FLOAT_TYPE_NAME
                        || valueType == _DOUBLE_TYPE_NAME
                        || valueType == _BOOL_TYPE_NAME
                        || valueType == _DECIMAL_TYPE_NAME
                        || field.DataType.IsEnum
                        || (new Regex(@"^\s*(?:(?:date)|(?:datetime))\s*\(\S|\s*\)\s*$", RegexOptions.IgnoreCase).IsMatch(field.DefaultValue.ToString()) && valueType == _DATETIME_TYPE_NAME))
                    {
                        fields.AppendFormat(" default ({0})", field.DefaultValue);
                    }
                    else
                    {
                        fields.AppendFormat(" default ('{0}')", field.DefaultValue);
                    }
                }

                if (!string.IsNullOrWhiteSpace(field.CheckConstraint))
                    fields.AppendFormat(" check({0})", field.CheckConstraint);
            }

            if (dataSource.Indices != null && dataSource.Indices.Count > 0)
            {
                foreach (var item in dataSource.Indices)
                {
                    if (indices.Length > 0)
                        indices.Append(Environment.NewLine);

                    if (item.Type == IndexType.Unique)
                        indices.Append("create unique index ");
                    else
                        indices.Append("create index ");

                    indices.Append($"{item.Name} on {dataSource.Name}(");

                    for (int i = 0; i < item.Fields.Count; i++)
                    {
                        if (i > 0)
                            indices.Append(",");

                        indices.Append(item.Fields[i].FieldName);

                        if (item.Sort == SortType.Ascending)
                            indices.Append(" asc");
                        else
                            indices.Append(" desc");
                    }

                    indices.Append(");");
                }
            }

            StringBuilder createSql = new StringBuilder("create table ");

            createSql.Append(dataSource.Name);

            createSql.AppendLine("(");

            createSql.Append(fields);

            createSql.AppendFormat("{0});{0}", Environment.NewLine);

            if (indices.Length > 0)
                createSql.AppendFormat("{0}{1}", Environment.NewLine, indices);

            return createSql.ToString();
        }
    }
}
