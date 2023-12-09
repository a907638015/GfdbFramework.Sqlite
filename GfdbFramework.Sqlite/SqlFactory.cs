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
    /// Sqlite 数据库的 Sql 创建工厂类。
    /// </summary>
    public class SqlFactory : ISqlFactory
    {
        private const string _CASE_SENSITIVE_MARK = "collate nocase";
        private readonly string _DBFunCountMethodName = nameof(DBFun.Count);
        private readonly string _DBFunMaxMethodName = nameof(DBFun.Max);
        private readonly string _DBFunMinMethodName = nameof(DBFun.Min);
        private readonly string _DBFunSumMethodName = nameof(DBFun.Sum);
        private readonly string _DBFunAvgMethodName = nameof(DBFun.Avg);
        private readonly string _DBFunNowTimeMethodName = nameof(DBFun.NowTime);
        private readonly string _DBFunNewIntMethodName = nameof(DBFun.NewInt);
        private readonly string _DBFunNewLongMethodName = nameof(DBFun.NewLong);
        private readonly string _DBFunDiffYearMethodName = nameof(DBFun.DiffYear);
        private readonly string _DBFunDiffMonthMethodName = nameof(DBFun.DiffMonth);
        private readonly string _DBFunDiffDayMethodName = nameof(DBFun.DiffDay);
        private readonly string _DBFunDiffHourMethodName = nameof(DBFun.DiffHour);
        private readonly string _DBFunDiffMinuteMethodName = nameof(DBFun.DiffMinute);
        private readonly string _DBFunDiffSecondMethodName = nameof(DBFun.DiffSecond);
        private readonly string _DBFunDiffMillisecondMethodName = nameof(DBFun.DiffMillisecond);
        private readonly string _DBFunAddYearMethodName = nameof(DBFun.AddYear);
        private readonly string _DBFunAddMonthMethodName = nameof(DBFun.AddMonth);
        private readonly string _DBFunAddDayMethodName = nameof(DBFun.AddDay);
        private readonly string _DBFunAddHourMethodName = nameof(DBFun.AddHour);
        private readonly string _DBFunAddMinuteMethodName = nameof(DBFun.AddMinute);
        private readonly string _DBFunAddSecondMethodName = nameof(DBFun.AddSecond);
        private readonly string _DBFunAddMillisecondMethodName = nameof(DBFun.AddMillisecond);
        private readonly Type _NullableType = typeof(int?).GetGenericTypeDefinition();
        private readonly Type _StringType = typeof(string);
        private readonly Type _BoolType = typeof(bool);
        private readonly Type _IntType = typeof(int);
        private readonly Type _UIntType = typeof(uint);
        private readonly Type _LongType = typeof(long);
        private readonly Type _ULongType = typeof(ulong);
        private readonly Type _DoubleType = typeof(double);
        private readonly Type _ShortType = typeof(short);
        private readonly Type _UShortType = typeof(ushort);
        private readonly Type _ByteType = typeof(byte);
        private readonly Type _SByteType = typeof(sbyte);
        private readonly Type _DecimalType = typeof(decimal);
        private readonly Type _FloatType = typeof(float);
        private readonly Type _GuidType = typeof(Guid);
        private readonly Type _DateTimeType = typeof(DateTime);
        private readonly Type _MathType = typeof(Math);
        private readonly Type _DBFunType = typeof(DBFun);

        /// <summary>
        /// 创建二元操作字段的基础表示 Sql 信息。
        /// </summary>
        /// <param name="parameterContext">创建表示 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="field">待创建表示 Sql 信息的二元操作字段。</param>
        /// <returns>该二元操作字段对应的基础 Sql 表示结果。</returns>
        public ExpressionInfo CreateBinaryBasicSql(IParameterContext parameterContext, BinaryField field)
        {
            if (field.IsBoolDataType)
            {
                return BoolToBasicExpression(field.DataContext, field.GetBoolExpression(parameterContext));
            }
            else
            {
                ExpressionInfo left = field.Left.GetBasicExpression(parameterContext);
                ExpressionInfo right = field.Right.GetBasicExpression(parameterContext);

                string leftSql = left.Type == OperationType.Subquery || (field.OperationType != OperationType.Coalesce && field.OperationType != OperationType.Power && Helper.CheckIsPriority(field.OperationType, left.Type, false)) ? $"({left.SQL})" : left.SQL;
                string rightSql = right.Type == OperationType.Subquery || (field.OperationType != OperationType.Coalesce && field.OperationType != OperationType.Power && Helper.CheckIsPriority(field.OperationType, right.Type, true)) ? $"({right.SQL})" : right.SQL;

                switch (field.OperationType)
                {
                    case OperationType.Add:
                        if (field.Left.DataType == _StringType || field.Right.DataType == _StringType)
                            return new ExpressionInfo($"{leftSql} || {rightSql}", field.OperationType);
                        else
                            return new ExpressionInfo($"{leftSql} + {rightSql}", field.OperationType);
                    case OperationType.And:
                        return new ExpressionInfo($"{leftSql} & {rightSql}", field.OperationType);
                    case OperationType.Divide:
                        return new ExpressionInfo($"{leftSql} / {rightSql}", field.OperationType);
                    case OperationType.Coalesce:
                        return new ExpressionInfo($"ifnull({leftSql}, {rightSql})", OperationType.Call);
                    case OperationType.ExclusiveOr:
                        throw new Exception("Sqlite 不支持按位异或操作");
                    case OperationType.LeftShift:
                        return new ExpressionInfo($"{leftSql} << {rightSql}", field.OperationType);
                    case OperationType.Modulo:
                        return new ExpressionInfo($"{leftSql} % {rightSql}", field.OperationType);
                    case OperationType.Multiply:
                        return new ExpressionInfo($"{leftSql} * {rightSql}", field.OperationType);
                    case OperationType.Or:
                        return new ExpressionInfo($"{leftSql} | {rightSql}", field.OperationType);
                    case OperationType.Power:
                        return new ExpressionInfo($"power({leftSql}, {rightSql})", OperationType.Call);
                    case OperationType.RightShift:
                        return new ExpressionInfo($"{leftSql} >> {rightSql}", field.OperationType);
                    case OperationType.Subtract:
                        return new ExpressionInfo($"{leftSql} - {rightSql}", field.OperationType);
                    default:
                        throw new Exception($"未能创建指定二元操作字段对应的基础表示信息，操作类型为：{field.OperationType}");
                }
            }
        }

        /// <summary>
        /// 创建二元操作字段的布尔表示 Sql 信息。
        /// </summary>
        /// <param name="parameterContext">创建表示 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="field">待创建表示 Sql 信息的二元操作字段。</param>
        /// <returns>该二元操作字段对应的布尔 Sql 表示结果。</returns>
        public ExpressionInfo CreateBinaryBoolSql(IParameterContext parameterContext, BinaryField field)
        {
            ExpressionInfo left;
            ExpressionInfo right;

            if (field.OperationType == OperationType.In || field.OperationType == OperationType.NotIn)
            {
                if (!field.Left.DataType.CheckIsBasicType())
                    throw new Exception("对用于子查询或 Where 条件的 Contains 方法，被调用对象 IEnumerable<T> 的成员（T）类型必须是基础数据类型");

                if (field.Right.Type == FieldType.Constant)
                {
                    left = field.Left.GetBasicExpression(parameterContext);

                    object constantRight = ((ConstantField)field.Right).Value;
                    string leftSql = Helper.CheckIsPriority(field.OperationType, left.Type, false) ? $"({left.SQL})" : left.SQL;
                    string containType = field.OperationType == OperationType.NotIn ? "not in" : "in";

                    if (field.Left.DataType == _StringType && !field.DataContext.IsCaseSensitive)
                        leftSql = $"{leftSql} {_CASE_SENSITIVE_MARK}";

                    //如果右侧集合是查询结果对象
                    if (constantRight is Queryable queryable)
                    {
                        return new ExpressionInfo($"{leftSql} {containType} ({queryable.GetSql(parameterContext)})", field.OperationType);
                    }
                    //否则判断右侧是否是常量数组或可枚举对象
                    else if ((field.Right.DataType.IsArray && field.Right.DataType.GetArrayRank() == 1) || field.Right.DataType.CheckIsEnumerable())
                    {
                        StringBuilder collection = new StringBuilder();

                        foreach (var item in (IEnumerable)constantRight)
                        {
                            if (collection.Length > 0)
                                collection.Append(", ");

                            collection.Append(parameterContext.Add(item));
                        }

                        return new ExpressionInfo($"{leftSql} {containType} ({collection})", field.OperationType);
                    }
                }
                else
                {
                    throw new Exception($"对用于子查询或 Where 条件的 Contains 方法，需要用于确认的集合对象必须是运行时常量或为 {nameof(Queryable)} 类型的查询对象");
                }
            }
            else if (field.OperationType == OperationType.Equal || field.OperationType == OperationType.NotEqual)
            {
                if (field.Left.Type == FieldType.Constant && ((ConstantField)field.Left).Value == null)
                {
                    if (field.Right.Type == FieldType.Constant && ((ConstantField)field.Right).Value == null)
                        return new ExpressionInfo("1 = 1", OperationType.Equal);

                    right = field.Right.GetBasicExpression(parameterContext);

                    string rightSql = right.Type == OperationType.Subquery ? $"({right.SQL})" : right.SQL;

                    return new ExpressionInfo($"{rightSql} is {(field.OperationType == OperationType.Equal ? "null" : "not null")}", field.OperationType);
                }
                else if (field.Right.Type == FieldType.Constant && ((ConstantField)field.Right).Value == null)
                {
                    left = field.Left.GetBasicExpression(parameterContext);

                    string leftSql = left.Type == OperationType.Subquery ? $"({left.SQL})" : left.SQL;

                    return new ExpressionInfo($"{leftSql} is {(field.OperationType == OperationType.Equal ? "null" : "not null")}", field.OperationType);
                }
                else
                {
                    left = field.Left.GetBasicExpression(parameterContext);
                    right = field.Right.GetBasicExpression(parameterContext);

                    string leftSql = Helper.CheckIsPriority(field.OperationType, left.Type, false) ? $"({left.SQL})" : left.SQL;
                    string rightSql = Helper.CheckIsPriority(field.OperationType, right.Type, true) ? $"({right.SQL})" : right.SQL;

                    if (!field.DataContext.IsCaseSensitive && field.Left.DataType == _StringType && field.Right.DataType == _StringType)
                        return new ExpressionInfo($"{leftSql} {_CASE_SENSITIVE_MARK} {(field.OperationType == OperationType.Equal ? "=" : "!=")} {rightSql}", field.OperationType);
                    else
                        return new ExpressionInfo($"{leftSql} {(field.OperationType == OperationType.Equal ? "=" : "!=")} {rightSql}", field.OperationType);
                }
            }
            else if (field.OperationType == OperationType.AndAlso || field.OperationType == OperationType.OrElse)
            {
                left = field.Left.GetBoolExpression(parameterContext);
                right = field.Right.GetBoolExpression(parameterContext);

                string leftSql = Helper.CheckIsPriority(field.OperationType, left.Type, false) ? $"({left.SQL})" : left.SQL;
                string rightSql = Helper.CheckIsPriority(field.OperationType, right.Type, true) ? $"({right.SQL})" : right.SQL;

                if (field.OperationType == OperationType.AndAlso)
                    return new ExpressionInfo($"{leftSql} and {rightSql}", OperationType.AndAlso);
                else
                    return new ExpressionInfo($"{leftSql} or {rightSql}", OperationType.OrElse);
            }
            else
            {
                left = field.Left.GetBasicExpression(parameterContext);
                right = field.Right.GetBasicExpression(parameterContext);

                string leftSql = Helper.CheckIsPriority(field.OperationType, left.Type, false) ? $"({left.SQL})" : left.SQL;
                string rightSql = Helper.CheckIsPriority(field.OperationType, right.Type, true) ? $"({right.SQL})" : right.SQL;

                switch (field.OperationType)
                {
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
                        if (!field.DataContext.IsCaseSensitive)
                            return new ExpressionInfo($"{leftSql} {_CASE_SENSITIVE_MARK} {(field.OperationType == OperationType.Like ? "like" : "not like")} {rightSql}", field.OperationType);
                        else
                            return new ExpressionInfo($"{leftSql} {(field.OperationType == OperationType.Like ? "like" : "not like")} {rightSql}", field.OperationType);
                }
            }

            throw new Exception($"未能创建指定二元操作字段对应布尔形态的表示信息，操作类型为：{field.OperationType}");
        }

        /// <summary>
        /// 创建三元操作字段的基础表示 Sql 信息。
        /// </summary>
        /// <param name="parameterContext">创建表示 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="field">待创建表示 Sql 信息的三元操作字段。</param>
        /// <returns>该三元操作字段对应的基础 Sql 表示结果。</returns>
        public ExpressionInfo CreateConditionalBasicSql(IParameterContext parameterContext, ConditionalField field)
        {
            var test = field.Test.GetBoolExpression(parameterContext);
            var ifTrue = field.IfTrue.GetBasicExpression(parameterContext);
            var ifFalse = field.IfFalse.GetBasicExpression(parameterContext);

            string testSql = test.Type == OperationType.Subquery ? $"({test.SQL})" : test.SQL;
            string ifTrueSql = ifTrue.Type == OperationType.Subquery ? $"({ifTrue.SQL})" : ifTrue.SQL;
            string ifFalseSql = ifFalse.Type == OperationType.Subquery ? $"({ifFalse.SQL})" : ifFalse.SQL;

            return new ExpressionInfo($"iif({testSql}, {ifTrueSql}, {ifFalseSql})", OperationType.Call);
        }

        /// <summary>
        /// 创建三元操作字段的布尔表示 Sql 信息。
        /// </summary>
        /// <param name="parameterContext">创建表示 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="field">待创建表示 Sql 信息的三元操作字段。</param>
        /// <returns>该三元操作字段对应的布尔 Sql 表示结果。</returns>
        public ExpressionInfo CreateConditionalBoolSql(IParameterContext parameterContext, ConditionalField field)
        {
            if (field.IfTrue.Type == FieldType.Constant && field.IfFalse.Type == FieldType.Constant)
            {
                bool ifTrue = (bool)((ConstantField)field.IfTrue).Value;
                bool ifFalse = (bool)((ConstantField)field.IfFalse).Value;

                if (ifTrue)
                {
                    if (ifFalse)
                        return new ExpressionInfo("1 = 1", OperationType.Equal);
                    else
                        return field.Test.GetBoolExpression(parameterContext);
                }
                else if (ifFalse)
                {
                    return new ExpressionInfo($"{field.Test.GetBasicExpression(parameterContext)} = 0", OperationType.Equal);
                }
                else
                {
                    return new ExpressionInfo("1 = 0", OperationType.Equal);
                }
            }
            else
            {
                return new ExpressionInfo($"{field.GetBasicExpression(parameterContext).SQL} = 1", OperationType.Equal);
            }
        }

        /// <summary>
        /// 创建常量字段的基础表示 Sql 信息。
        /// </summary>
        /// <param name="parameterContext">创建表示 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="field">待创建表示 Sql 信息的常量字段。</param>
        /// <returns>该常量字段对应的基础 Sql 表示结果。</returns>
        public ExpressionInfo CreateConstantBasicSql(IParameterContext parameterContext, ConstantField field)
        {
            //只有基础数据类型才能作为参数传入 Sqlite
            if (Helper.CheckIsBasicType(field.DataType))
                return new ExpressionInfo(parameterContext.Add(field.Value), OperationType.Default);
            else if (field.Value is Queryable)
                throw new Exception($"Sqlite 子查询不支持多行数据返回，若要使用子查询，可在 {nameof(Queryable)} 对象最后调用一次 First() 函数或 Last() 函数将其限定只返回一行数据");
            else if (field.Value is MultipleJoin)
                throw new Exception("子查询不支持直接返回多表关联查询对象（MultipleJoin），若要使用子查询，可在 MultipleJoin 对象上调用一次 Select 函数并再次调用 First 方法即可");
            else
                throw new Exception("Sqlite 只支持基础数据类型的常量作为参数传入");
        }

        /// <summary>
        /// 创建常量字段的布尔表示 Sql 信息。
        /// </summary>
        /// <param name="parameterContext">创建表示 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="field">待创建表示 Sql 信息的常量字段。</param>
        /// <returns>该常量字段对应的布尔 Sql 表示结果。</returns>
        public ExpressionInfo CreateConstantBoolSql(IParameterContext parameterContext, ConstantField field)
        {
            return new ExpressionInfo($"{field.GetBasicExpression(parameterContext).SQL} = 1", OperationType.Equal);
        }

        /// <summary>
        /// 创建成员调用字段的基础表示 Sql 信息。
        /// </summary>
        /// <param name="parameterContext">创建表示 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="field">待创建表示 Sql 信息的成员调用字段。</param>
        /// <returns>该成员调用字段对应的基础 Sql 表示结果。</returns>
        public ExpressionInfo CreateMemberBasicSql(IParameterContext parameterContext, MemberField field)
        {
            if (field.ObjectField != null && field.ObjectField is BasicField basicField)
            {
                //若调用实例为 string 类型
                if (field.ObjectField.DataType == _StringType)
                {
                    if (field.MemberInfo.Name == "Length" && field.MemberInfo.MemberType == MemberTypes.Property)
                    {
                        var obj = basicField.GetBasicExpression(parameterContext);

                        string objectSql = obj.Type == OperationType.Subquery ? $"({obj.SQL})" : obj.SQL;

                        return new ExpressionInfo($"length({objectSql})", OperationType.Call);
                    }
                }
                //若调用实例为 DateTime 类型且成员类型为属性
                else if (field.ObjectField.DataType == _DateTimeType && field.MemberInfo.MemberType == MemberTypes.Property)
                {
                    if (field.MemberInfo.Name == "Year"
                        || field.MemberInfo.Name == "Month"
                        || field.MemberInfo.Name == "Day"
                        || field.MemberInfo.Name == "Hour"
                        || field.MemberInfo.Name == "Minute"
                        || field.MemberInfo.Name == "Second"
                        || field.MemberInfo.Name == "Millisecond")
                    {
                        var obj = basicField.GetBasicExpression(parameterContext);

                        string objectSql = obj.Type == OperationType.Subquery ? $"({obj.SQL})" : obj.SQL;

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
                            return new ExpressionInfo($"cast(strftime('{format}', {objectSql}) as int)", OperationType.Call);
                        else
                            return new ExpressionInfo($"cast(substr(strftime('%f', {objectSql}), 4) as int)", OperationType.Call);
                    }
                    else if (field.MemberInfo.Name == "Date")
                    {
                        var obj = basicField.GetBasicExpression(parameterContext);

                        string objectSql = obj.Type == OperationType.Subquery ? $"({obj.SQL})" : obj.SQL;

                        return new ExpressionInfo($"date({objectSql})", OperationType.Call);
                    }
                }
            }

            throw new Exception($"未能创建指定成员调用字段对应的基础表示信息，类名：{field.MemberInfo.DeclaringType.FullName}，成员名：{field.MemberInfo.Name}");
        }

        /// <summary>
        /// 创建成员调用字段的布尔表示 Sql 信息。
        /// </summary>
        /// <param name="parameterContext">创建表示 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="field">待创建表示 Sql 信息的成员调用字段。</param>
        /// <returns>该成员调用字段对应的布尔 Sql 表示结果。</returns>
        public ExpressionInfo CreateMemberBoolSql(IParameterContext parameterContext, MemberField field)
        {
            throw new Exception($"未能创建指定成员调用字段对应布尔形态的表示信息，类名：{field.MemberInfo.DeclaringType.FullName}，成员名：{field.MemberInfo.Name}");
        }

        /// <summary>
        /// 创建方法调用字段的基础表示 Sql 信息。
        /// </summary>
        /// <param name="parameterContext">创建表示 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="field">待创建表示 Sql 信息的方法调用字段。</param>
        /// <returns>该方法调用字段对应的基础 Sql 表示结果。</returns>
        public ExpressionInfo CreateMethodBasicSql(IParameterContext parameterContext, MethodField field)
        {
            //实例方法
            if (field.ObjectField != null)
            {
                //调用实例字段是基础数据字段
                if (field.ObjectField is BasicField basicField)
                {
                    //若调用实例为 string 类型
                    if (field.ObjectField.DataType == _StringType)
                    {
                        //IndexOf 方法
                        if (field.MethodInfo.Name == "IndexOf" && field.Parameters != null && field.Parameters.Count == 1 && field.Parameters[0] is BasicField parameter)
                        {
                            var basic = basicField.GetBasicExpression(parameterContext);
                            var param = parameter.GetBasicExpression(parameterContext);

                            string objectSql = basic.Type == OperationType.Subquery ? $"({basic.SQL})" : basic.SQL;
                            string searchString = param.Type == OperationType.Subquery ? $"({param.SQL})" : param.SQL;

                            if (!field.DataContext.IsCaseSensitive)
                            {
                                objectSql = $"lower({objectSql})";
                                searchString = $"lower({searchString})";
                            }

                            //Sqlite 中的 charIndex 是从 1 开始，结果得手动减去 1，startIndex 得手动加上 1
                            return new ExpressionInfo($"instr({objectSql}, {searchString}) - 1", OperationType.Subtract);
                        }
                        //Substring 方法
                        else if (field.MethodInfo.Name == "Substring" && field.Parameters != null && (field.Parameters.Count == 1 || field.Parameters.Count == 2))
                        {
                            var basic = basicField.GetBasicExpression(parameterContext);
                            var start = ((BasicField)field.Parameters[0]).GetBasicExpression(parameterContext);
                            ExpressionInfo length = field.Parameters.Count == 1 ? null : ((BasicField)field.Parameters[1]).GetBasicExpression(parameterContext);

                            string startSql = Helper.CheckIsPriority(OperationType.Add, start.Type, false) ? $"({start.SQL})" : start.SQL;
                            string stringSql = basic.Type == OperationType.Subquery ? $"({basic.SQL})" : basic.SQL;
                            string lengthSql;

                            if (field.Parameters.Count == 1)
                                return new ExpressionInfo($"substr({stringSql}, {start} + 1)", OperationType.Call);
                            else
                                lengthSql = length.Type == OperationType.Subquery ? $"({length.SQL})" : length.SQL;

                            return new ExpressionInfo($"substr({stringSql}, {startSql} + 1, {lengthSql})", OperationType.Call);
                        }
                        //无参的 Trim、TrimStart、TrimEnd、ToUpper 或 ToLower 方法
                        else if ((field.MethodInfo.Name == "Trim" || field.MethodInfo.Name == "TrimStart" || field.MethodInfo.Name == "TrimEnd" || field.MethodInfo.Name == "ToUpper" || field.MethodInfo.Name == "ToLower") && (field.Parameters == null || field.Parameters.Count < 1))
                        {
                            var basic = basicField.GetBasicExpression(parameterContext);

                            string objectSql = basic.Type == OperationType.Subquery ? $"({basic.SQL})" : basic.SQL;

                            string methodName = field.MethodInfo.Name == "Trim" ? "trim" : field.MethodInfo.Name == "TrimStart" ? "ltrim" : field.MethodInfo.Name == "TrimEnd" ? "rtrim" : field.MethodInfo.Name == "ToLower" ? "lower" : "upper";

                            return new ExpressionInfo($"{methodName}({objectSql})", OperationType.Call);
                        }
                        //StartsWith、EndsWith 或 Contains 方法，不支持带参数的 StartsWith、EndsWith 方法
                        else if ((field.MethodInfo.Name == "StartsWith" || field.MethodInfo.Name == "EndsWith" || field.MethodInfo.Name == "Contains") && field.Parameters != null && field.Parameters.Count == 1)
                        {
                            return BoolToBasicExpression(field.DataContext, field.GetBoolExpression(parameterContext));
                        }
                        //Insert 或 Replace 方法
                        else if ((field.MethodInfo.Name == "Insert" || field.MethodInfo.Name == "Replace") && field.Parameters != null && field.Parameters.Count == 2 && field.Parameters[0] is BasicField && field.Parameters[1] is BasicField)
                        {
                            var basic = basicField.GetBasicExpression(parameterContext);
                            var param1 = ((BasicField)field.Parameters[0]).GetBasicExpression(parameterContext);
                            var param2 = ((BasicField)field.Parameters[1]).GetBasicExpression(parameterContext);

                            string objectSql = basic.Type == OperationType.Subquery ? $"({basic.SQL})" : basic.SQL;
                            string param1Sql = param1.Type == OperationType.Subquery ? $"({param1.SQL})" : param1.SQL;
                            string param2Sql = param2.Type == OperationType.Subquery ? $"({param2.SQL})" : param2.SQL;

                            if (field.MethodInfo.Name == "Replace")
                                return new ExpressionInfo($"replace({objectSql}, {param1Sql}, {param2Sql})", OperationType.Call);
                            else
                                return new ExpressionInfo($"substr({objectSql}, 1, {param1Sql}) || {param2Sql} || substr({objectSql}, {(Helper.CheckIsPriority(OperationType.Add, param1.Type, false) ? $"({param1Sql})" : param1Sql)} + 1)", OperationType.Add);
                        }
                    }
                    //若调用实例为 DateTime 类型
                    else if (field.ObjectField.DataType == _DateTimeType)
                    {
                        //ToString 方法
                        if (field.MethodInfo.Name == "ToString")
                        {
                            string format = null;

                            if (field.Parameters != null && field.Parameters.Count > 0)
                            {
                                if (field.Parameters.Count == 1 && field.Parameters[0] is BasicField formatField)
                                {
                                    if (formatField.DataType != _StringType)
                                        throw new Exception("Sqlite 日期格式化只支持字符串格式的参数");

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
                                        var formatExpression = formatField.GetBasicExpression(parameterContext);

                                        format = formatExpression.Type == OperationType.Subquery ? $"({formatExpression.SQL})" : formatExpression.SQL;

                                        format = $"replace(replace(replace(replace(replace(replace(replace({format}, 'ss.fff', '%f'), 'ss', '%S'), 'mm', '%M'), 'hh', '%H'), 'yyyy', '%Y'), 'MM', '%m'), 'dd', '%d')";
                                    }
                                }
                                else
                                {
                                    throw new Exception("Sqlite 不支持多参数的日期格式化函数");
                                }
                            }

                            var basic = basicField.GetBasicExpression(parameterContext);

                            string dateTimeSql = basic.Type == OperationType.Subquery ? $"({basic.SQL})" : basic.SQL;

                            return new ExpressionInfo($"strftime({format}, {dateTimeSql})", OperationType.Call);
                        }
                    }
                    //无参 ToString 方法
                    else if (field.MethodInfo.Name == "ToString" && (field.Parameters == null || field.Parameters.Count < 1))
                    {
                        var basic = basicField.GetBasicExpression(parameterContext);

                        string objectSql = basic.Type == OperationType.Subquery ? $"({basic.SQL})" : basic.SQL;

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
                    ExpressionInfo basic;
                    string parameterSql;

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
                            basic = basicField.GetBasicExpression(parameterContext);

                            parameterSql = basic.Type == OperationType.Subquery ? $"({basic.SQL})" : basic.SQL;

                            return new ExpressionInfo($"cast({parameterSql} as {field.DataContext.NetTypeToDBType(field.DataType)})", OperationType.Call);
                        case "ToDateTime":
                            basic = basicField.GetBasicExpression(parameterContext);

                            parameterSql = basic.Type == OperationType.Subquery ? $"({basic.SQL})" : basic.SQL;

                            return new ExpressionInfo($"datetime({parameterSql})", OperationType.Call);
                    }
                }
            }
            //如果是 Math 数学函数类
            else if (field.MethodInfo.ReflectedType == _MathType)
            {
                //Math.Round 函数
                if (field.MethodInfo.Name == "Round" && field.Parameters != null && (field.Parameters.Count == 1 || (field.Parameters.Count == 2 && field.Parameters[1].DataType == _IntType)))
                {
                    var numeric = ((BasicField)field.Parameters[0]).GetBasicExpression(parameterContext);

                    string numericSql = numeric.Type == OperationType.Subquery ? $"({numeric.SQL})" : numeric.SQL;

                    if (field.Parameters.Count == 1)
                    {
                        return new ExpressionInfo($"round({numericSql})", OperationType.Call);
                    }
                    else
                    {
                        var length = ((BasicField)field.Parameters[1]).GetBasicExpression(parameterContext);

                        string lengthSql = length.Type == OperationType.Subquery ? $"({length.SQL})" : length.SQL;

                        return new ExpressionInfo($"round({numericSql}, {lengthSql})", OperationType.Call);
                    }
                }
                //Math.Floor、Math.Ceiling 或 Math.Abs 函数
                else if ((field.MethodInfo.Name == "Floor" || field.MethodInfo.Name == "Ceiling" || field.MethodInfo.Name == "Abs") && field.Parameters != null && field.Parameters.Count == 1)
                {
                    var numeric = ((BasicField)field.Parameters[0]).GetBasicExpression(parameterContext);

                    string numericSql = numeric.Type == OperationType.Subquery ? $"({numeric.SQL})" : numeric.SQL;

                    return new ExpressionInfo($"{field.MethodInfo.Name.ToLower()}({numericSql})", OperationType.Call);
                }
                //Math.Pow 函数
                else if (field.MethodInfo.Name == "Pow" && field.Parameters != null && field.Parameters.Count == 2)
                {
                    var power = ((BasicField)field.Parameters[1]).GetBasicExpression(parameterContext);
                    var numeric = ((BasicField)field.Parameters[0]).GetBasicExpression(parameterContext);

                    string numericSql = numeric.Type == OperationType.Subquery ? $"({numeric.SQL})" : numeric.SQL;
                    string powerSql = power.Type == OperationType.Subquery ? $"({power.SQL})" : power.SQL;

                    return new ExpressionInfo($"power({numericSql}, {powerSql})", OperationType.Call);
                }
            }
            //如果是 DBFun 类的函数
            else if (field.MethodInfo.ReflectedType == _DBFunType)
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
                            var param = basicField.GetBasicExpression(parameterContext);

                            var paramSql = param.Type == OperationType.Subquery ? $"({param.SQL})" : param.SQL;

                            return new ExpressionInfo($"count({paramSql})", OperationType.Call);
                        }
                    }
                }
                //其他聚合方法（Max、Min、Sum、Avg）
                else if (field.Parameters != null && field.Parameters.Count == 1 && (field.MethodInfo.Name == _DBFunMaxMethodName || field.MethodInfo.Name == _DBFunMinMethodName || field.MethodInfo.Name == _DBFunSumMethodName || field.MethodInfo.Name == _DBFunAvgMethodName) && field.Parameters[0] is BasicField basicField)
                {
                    var param = basicField.GetBasicExpression(parameterContext);

                    var paramSql = param.Type == OperationType.Subquery ? $"({param.SQL})" : param.SQL;

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

                    return new ExpressionInfo($"{methodName}({paramSql})", OperationType.Call);
                }
                //DBFun.NowTime 函数
                else if ((field.Parameters == null || field.Parameters.Count < 1) && field.MethodInfo.Name == _DBFunNowTimeMethodName)
                {
                    return new ExpressionInfo("datetime('now', 'localtime')", OperationType.Call);
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
                        var min = ((BasicField)field.Parameters[0]).GetBasicExpression(parameterContext);
                        var max = ((BasicField)field.Parameters[1]).GetBasicExpression(parameterContext);

                        string minSql = Helper.CheckIsPriority(OperationType.Add, min.Type, false) ? $"({min.SQL})" : min.SQL;
                        string maxSql = Helper.CheckIsPriority(OperationType.Subtract, max.Type, false) ? $"({max.SQL})" : max.SQL;

                        maxSql = Helper.CheckIsPriority(OperationType.Subtract, min.Type, true) ? $"{maxSql} - ({min.SQL})" : $"{maxSql} - ({min.SQL})";

                        return new ExpressionInfo($"{minSql} + round(abs(random() / 9223372036854775807.0) * ({maxSql} - 1))", OperationType.Add);
                    }
                }
                //DBFun.NewLong 函数
                else if ((field.Parameters == null || field.Parameters.Count < 1) && field.MethodInfo.Name == _DBFunNewLongMethodName)
                {
                    return new ExpressionInfo("random()", OperationType.Call);
                }
                //DBFun 的各种日期差值计算函数
                else if (field.Parameters != null && field.MethodInfo.ReturnType == _IntType && field.Parameters.Count == 2
                    && (field.Parameters[0].DataType == _DateTimeType || (field.Parameters[0].DataType.IsGenericType && field.Parameters[0].DataType.GetGenericTypeDefinition() == _NullableType && field.Parameters[0].DataType.GetGenericArguments()[0] == _DateTimeType))
                    && (field.Parameters[1].DataType == _DateTimeType || (field.Parameters[1].DataType.IsGenericType && field.Parameters[1].DataType.GetGenericTypeDefinition() == _NullableType && field.Parameters[1].DataType.GetGenericArguments()[0] == _DateTimeType)) &&
                    && field.Parameters[0].DataType == _DateTimeType && field.Parameters[1].DataType == _DateTimeType &&
                    (field.MethodInfo.Name == _DBFunDiffYearMethodName
                    || field.MethodInfo.Name == _DBFunDiffMonthMethodName
                    || field.MethodInfo.Name == _DBFunDiffDayMethodName
                    || field.MethodInfo.Name == _DBFunDiffHourMethodName
                    || field.MethodInfo.Name == _DBFunDiffMinuteMethodName
                    || field.MethodInfo.Name == _DBFunDiffSecondMethodName
                    || field.MethodInfo.Name == _DBFunDiffMillisecondMethodName))
                {
                    var obj = ((BasicField)field.Parameters[0]).GetBasicExpression(parameterContext);
                    var compare = ((BasicField)field.Parameters[1]).GetBasicExpression(parameterContext);

                    string objectSql = obj.Type == OperationType.Subquery ? $"({obj.SQL})" : obj.SQL;
                    string compareSql = compare.Type == OperationType.Subquery ? $"({compare.SQL})" : compare.SQL;

                    if (field.MethodInfo.Name == _DBFunDiffYearMethodName)
                        return new ExpressionInfo($"strftime('%Y', {compareSql}) - strftime('%Y', {objectSql})", OperationType.Subtract);
                    else if (field.MethodInfo.Name == _DBFunDiffMonthMethodName)
                        return new ExpressionInfo($"(strftime('%Y', {compareSql}) - strftime('%Y', {objectSql})) * 12 + (strftime('%m', {compareSql}) - strftime('%m', {objectSql}))", OperationType.Add);
                    else if (field.MethodInfo.Name == _DBFunDiffDayMethodName)
                        return new ExpressionInfo($"(strftime('%s',date({compareSql})) - strftime('%s',date({objectSql}))) / 86400", OperationType.Divide);
                    else if (field.MethodInfo.Name == _DBFunDiffHourMethodName)
                        return new ExpressionInfo($"(strftime('%s',strftime('%Y-%m-%d %H:00:00', {compareSql})) - strftime('%s',strftime('%Y-%m-%d %H:00:00', {objectSql}))) / 3600", OperationType.Divide);
                    else if (field.MethodInfo.Name == _DBFunDiffMinuteMethodName)
                        return new ExpressionInfo($"(strftime('%s',strftime('%Y-%m-%d %H:%M:00', {compareSql})) - strftime('%s',strftime('%Y-%m-%d %H:%M:00', {objectSql}))) / 60", OperationType.Divide);
                    else if (field.MethodInfo.Name == _DBFunDiffSecondMethodName)
                        return new ExpressionInfo($"strftime('%s',{compareSql}) - strftime('%s',{objectSql})", OperationType.Subtract);
                    else
                        return new ExpressionInfo($"cast(strftime('%s', {compareSql}) || substr(strftime('%f', {compareSql}), 4) as long) - cast(strftime('%s', {objectSql}) || substr(strftime('%f', {objectSql}), 4) as long)", OperationType.Subtract);
                }
                //DNFun 的各种日期添加函数
                else if (field.Parameters != null && field.MethodInfo.ReturnType == _DateTimeType && field.Parameters.Count == 2
                    && (field.Parameters[0].DataType == _DateTimeType || (field.Parameters[0].DataType.IsGenericType && field.Parameters[0].DataType.GetGenericTypeDefinition() == _NullableType && field.Parameters[0].DataType.GetGenericArguments()[0] == _DateTimeType))
                    && field.Parameters[1].DataType == _IntType &&
                    (
                    field.MethodInfo.Name == _DBFunAddYearMethodName
                    || field.MethodInfo.Name == _DBFunAddMonthMethodName
                    || field.MethodInfo.Name == _DBFunAddDayMethodName
                    || field.MethodInfo.Name == _DBFunAddHourMethodName
                    || field.MethodInfo.Name == _DBFunAddMinuteMethodName
                    || field.MethodInfo.Name == _DBFunAddSecondMethodName
                    || field.MethodInfo.Name == _DBFunAddMillisecondMethodName))
                {
                    var obj = ((BasicField)field.Parameters[0]).GetBasicExpression(parameterContext);
                    var value = ((BasicField)field.Parameters[1]).GetBasicExpression(parameterContext);

                    string objectSql = obj.Type == OperationType.Subquery ? $"({obj.SQL})" : obj.SQL;
                    string valueSql = value.Type == OperationType.Subquery ? $"({value.SQL})" : value.SQL;

                    if (field.MethodInfo.Name == _DBFunAddMillisecondMethodName)
                    {
                        objectSql = $"cast(strftime('%s', {objectSql}) || substr(strftime('%f', {objectSql}), 4) as long)";

                        objectSql = $"{objectSql} + {valueSql}";

                        return new ExpressionInfo($"datetime(({objectSql}) / 1000, 'unixepoch')", OperationType.Call);
                    }
                    else
                    {
                        string type;

                        if (field.MethodInfo.Name == _DBFunAddYearMethodName)
                            type = "year";
                        else if (field.MethodInfo.Name == _DBFunAddMonthMethodName)
                            type = "month";
                        else if (field.MethodInfo.Name == _DBFunAddDayMethodName)
                            type = "day";
                        else if (field.MethodInfo.Name == _DBFunAddHourMethodName)
                            type = "hour";
                        else if (field.MethodInfo.Name == _DBFunAddMinuteMethodName)
                            type = "minute";
                        else
                            type = "second";

                        return new ExpressionInfo($"datetime({objectSql}, {valueSql} || ' {type}')", OperationType.Call);
                    }
                }
            }
            //各种 Parse 方法
            else if (field.MethodInfo.Name == "Parse" && field.Parameters != null && field.Parameters.Count == 1 && field.Parameters[0].DataType == _StringType && field.Parameters[0] is BasicField parameterField)
            {
                //DateTime.Parse 方法
                if (field.DataType == _DateTimeType && field.MethodInfo.ReflectedType == _DateTimeType)
                {
                    var param = parameterField.GetBasicExpression(parameterContext);

                    string parameterSql = param.Type == OperationType.Subquery ? $"({param.SQL})" : param.SQL;

                    return new ExpressionInfo($"datetime({parameterSql})", OperationType.Call);
                }
                //int.Parse 方法
                else if (field.DataType == _IntType && field.MethodInfo.ReflectedType == _IntType)
                {
                    var param = parameterField.GetBasicExpression(parameterContext);

                    string parameterSql = param.Type == OperationType.Subquery ? $"({param.SQL})" : param.SQL;

                    return new ExpressionInfo($"cast({parameterSql} as int32)", OperationType.Call);
                }
                //double.Parse 方法
                else if (field.DataType == _DoubleType && field.MethodInfo.ReflectedType == _DoubleType)
                {
                    var param = parameterField.GetBasicExpression(parameterContext);

                    string parameterSql = param.Type == OperationType.Subquery ? $"({param.SQL})" : param.SQL;

                    return new ExpressionInfo($"cast({parameterSql} as double)", OperationType.Call);
                }
                //long.Parse 方法
                else if (field.DataType == _LongType && field.MethodInfo.ReflectedType == _LongType)
                {
                    var param = parameterField.GetBasicExpression(parameterContext);

                    string parameterSql = param.Type == OperationType.Subquery ? $"({param.SQL})" : param.SQL;

                    return new ExpressionInfo($"cast({parameterSql} as long)", OperationType.Call);
                }
                //short.Parse 方法
                else if (field.DataType == _ShortType && field.MethodInfo.ReflectedType == _ShortType)
                {
                    var param = parameterField.GetBasicExpression(parameterContext);

                    string parameterSql = param.Type == OperationType.Subquery ? $"({param.SQL})" : param.SQL;

                    return new ExpressionInfo($"cast({parameterSql} as int16)", OperationType.Call);
                }
                //byte.Parse 方法
                else if (field.DataType == _ByteType && field.MethodInfo.ReflectedType == _ByteType)
                {
                    var param = parameterField.GetBasicExpression(parameterContext);

                    string parameterSql = param.Type == OperationType.Subquery ? $"({param.SQL})" : param.SQL;

                    return new ExpressionInfo($"cast({parameterSql} as int8)", OperationType.Call);
                }
                //decimal.Parse 方法
                else if (field.DataType == _DecimalType && field.MethodInfo.ReflectedType == _DecimalType)
                {
                    var param = parameterField.GetBasicExpression(parameterContext);

                    string parameterSql = param.Type == OperationType.Subquery ? $"({param.SQL})" : param.SQL;

                    return new ExpressionInfo($"cast({parameterSql} as double)", OperationType.Call);
                }
                //bool.Parse 方法
                else if (field.DataType == _BoolType && field.MethodInfo.ReflectedType == _BoolType)
                {
                    var param = parameterField.GetBasicExpression(parameterContext);

                    string parameterSql = param.Type == OperationType.Subquery ? $"({param.SQL})" : param.SQL;

                    return new ExpressionInfo($"cast({parameterSql} as boolean)", OperationType.Call);
                }
                //Guid.Parse 方法
                else if (field.DataType == _GuidType && field.MethodInfo.ReflectedType == _GuidType)
                {
                    return parameterField.GetBasicExpression(parameterContext);
                }
            }
            //字符串静态 IsNullOrEmpty 或 IsNullOrWhiteSpace 方法
            else if (field.Parameters != null && field.Parameters.Count == 1 && field.Parameters[0].DataType == _StringType && field.Parameters[0] is BasicField basicField && (field.MethodInfo.Name == "IsNullOrEmpty" || field.MethodInfo.Name == "IsNullOrWhiteSpace") && field.MethodInfo.ReflectedType == _StringType)
            {
                return BoolToBasicExpression(field.DataContext, field.GetBoolExpression(parameterContext));
            }

            throw new Exception($"未能创建指定方法调用字段对应的基础表示信息，类名：{field.MethodInfo.DeclaringType.FullName}，方法名：{field.MethodInfo.Name}");
        }

        /// <summary>
        /// 创建方法调用字段的布尔表示 Sql 信息。
        /// </summary>
        /// <param name="parameterContext">创建表示 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="field">待创建表示 Sql 信息的方法调用字段。</param>
        /// <returns>该方法调用字段对应的布尔 Sql 表示结果。</returns>
        public ExpressionInfo CreateMethodBoolSql(IParameterContext parameterContext, MethodField field)
        {
            //StartsWith、EndsWith 或 Contains 方法，不支持带参数的 StartsWith、EndsWith 方法
            if (field.ObjectField != null && field.MethodInfo.ReflectedType == _StringType && field.ObjectField is BasicField basicField && (field.MethodInfo.Name == "StartsWith" || field.MethodInfo.Name == "EndsWith" || field.MethodInfo.Name == "Contains") && field.Parameters != null && field.Parameters.Count == 1 && field.Parameters[0] is BasicField parameter)
            {
                ExpressionInfo basic = basicField.GetBasicExpression(parameterContext);
                ExpressionInfo param = parameter.GetBasicExpression(parameterContext);

                string objectSql = basic.Type == OperationType.Subquery ? $"({basic})" : basic.SQL;
                string searchString = param.Type == OperationType.Subquery ? $"({param.SQL})" : param.SQL;

                if (field.MethodInfo.Name == "EndsWith")
                {
                    if (!field.DataContext.IsCaseSensitive)
                        return new ExpressionInfo($"instr(lower({objectSql}), lower({searchString})) == length({objectSql}) - length({searchString}) + 1", OperationType.Equal);
                    else
                        return new ExpressionInfo($"instr({objectSql}, {searchString}) == length({objectSql}) - length({searchString}) + 1", OperationType.Equal);
                }
                else
                {
                    string checkString = field.MethodInfo.Name == "StartsWith" ? "=" : ">=";

                    if (!field.DataContext.IsCaseSensitive)
                        return new ExpressionInfo($"instr(lower({objectSql}), lower({searchString})) {checkString} 1", field.MethodInfo.Name == "StartsWith" ? OperationType.Equal : OperationType.GreaterThanOrEqual);
                    else
                        return new ExpressionInfo($"instr({objectSql}, {searchString}) {checkString} 1", field.MethodInfo.Name == "StartsWith" ? OperationType.Equal : OperationType.GreaterThanOrEqual);
                }
            }
            //string 静态的 IsNullOrEmpty 或 IsNullOrWhiteSpace 方法
            else if (field.ObjectField == null && field.MethodInfo.ReflectedType == _StringType && (field.MethodInfo.Name == "IsNullOrEmpty" || field.MethodInfo.Name == "IsNullOrWhiteSpace") && field.Parameters != null && field.Parameters.Count == 1 && field.Parameters[0].DataType == _StringType && field.Parameters[0] is BasicField)
            {
                var param = ((BasicField)field.Parameters[0]).GetBasicExpression(parameterContext);

                string parameterSql = param.Type == OperationType.Subquery ? $"({param.SQL})" : param.SQL;

                if (field.MethodInfo.Name == "IsNullOrEmpty")
                    return new ExpressionInfo($"{parameterSql} is null or {parameterSql} = ''", OperationType.OrElse);
                else
                    return new ExpressionInfo($"{parameterSql} is null or trim({parameterSql}) = ''", OperationType.OrElse);
            }

            throw new Exception($"未能创建指定方法调用字段对应布尔形态的表示信息，类名：{field.MethodInfo.DeclaringType.FullName}，方法名：{field.MethodInfo.Name}");
        }

        /// <summary>
        /// 创建原始数据库表或视图字段的基础表示 Sql 信息。
        /// </summary>
        /// <param name="parameterContext">创建表示 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="field">待创建表示 Sql 信息的原始字段。</param>
        /// <returns>该原始数据库表或视图字段对应的基础 Sql 表示结果。</returns>
        public ExpressionInfo CreateOriginalBasicSql(IParameterContext parameterContext, OriginalField field)
        {
            if (string.IsNullOrWhiteSpace(field.DataSourceAlias))
                return new ExpressionInfo(field.FieldName, OperationType.Default);
            else
                return new ExpressionInfo($"{field.DataSourceAlias}.{field.FieldName}", OperationType.Default);
        }

        /// <summary>
        /// 创建原始数据库表或视图字段的布尔表示 Sql 信息。
        /// </summary>
        /// <param name="parameterContext">创建表示 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="field">待创建表示 Sql 信息的原始字段。</param>
        /// <returns>该原始数据库表或视图字段对应的布尔 Sql 表示结果。</returns>
        public ExpressionInfo CreateOriginalBoolSql(IParameterContext parameterContext, OriginalField field)
        {
            return new ExpressionInfo($"{field.GetBasicExpression(parameterContext).SQL} = 1", OperationType.Equal);
        }

        /// <summary>
        /// 创建引用字段的基础表示 Sql 信息。
        /// </summary>
        /// <param name="parameterContext">创建表示 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="field">待创建表示 Sql 信息的引用字段。</param>
        /// <returns>该引用字段对应的基础 Sql 表示结果。</returns>
        public ExpressionInfo CreateQuoteBasicSql(IParameterContext parameterContext, QuoteField field)
        {
            if (string.IsNullOrWhiteSpace(field.QuoteDataSourceAlias))
                return new ExpressionInfo(field.QuoteFieldName, OperationType.Default);
            else
                return new ExpressionInfo($"{field.QuoteDataSourceAlias}.{field.QuoteFieldName}", OperationType.Default);
        }

        /// <summary>
        /// 创建引用字段的布尔表示 Sql 信息。
        /// </summary>
        /// <param name="parameterContext">创建表示 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="field">待创建表示 Sql 信息的引用字段。</param>
        /// <returns>该引用字段对应的布尔 Sql 表示结果。</returns>
        public ExpressionInfo CreateQuoteBoolSql(IParameterContext parameterContext, QuoteField field)
        {
            return new ExpressionInfo($"{field.GetBasicExpression(parameterContext).SQL} = 1", OperationType.Equal);
        }

        /// <summary>
        /// 创建子查询字段的基础表示 Sql 信息。
        /// </summary>
        /// <param name="parameterContext">创建表示 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="field">待创建表示 Sql 信息的子查询字段。</param>
        /// <returns>该子查询字段对应的基础 Sql 表示结果。</returns>
        public ExpressionInfo CreateSubqueryBasicSql(IParameterContext parameterContext, SubqueryField field)
        {
            return new ExpressionInfo(GenerateSelectSql(parameterContext, field.SelectField, field.BelongDataSource, false), OperationType.Subquery);
        }

        /// <summary>
        /// 创建子查询字段的布尔表示 Sql 信息。
        /// </summary>
        /// <param name="parameterContext">创建表示 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="field">待创建表示 Sql 信息的子查询字段。</param>
        /// <returns>该子查询字段对应的布尔 Sql 表示结果。</returns>
        public ExpressionInfo CreateSubqueryBoolSql(IParameterContext parameterContext, SubqueryField field)
        {
            return new ExpressionInfo($"({field.GetBasicExpression(parameterContext).SQL}) = 1", OperationType.Equal);
        }

        /// <summary>
        /// 创建 Switch 分支字段的基础表示 Sql 信息。
        /// </summary>
        /// <param name="parameterContext">创建表示 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="field">待创建表示 Sql 信息的 Switch 分支字段。</param>
        /// <returns>该 Switch 分支字段对应的基础 Sql 表示结果。</returns>
        public ExpressionInfo CreateSwitchBasicSql(IParameterContext parameterContext, SwitchField field)
        {
            var switchValue = field.SwitchValue.GetBasicExpression(parameterContext);
            ExpressionInfo defaultBody = field.DefaultBody == null ? null : field.DefaultBody.GetBasicExpression(parameterContext);

            string switchValueSql = Helper.CheckIsPriority(OperationType.Equal, switchValue.Type, false) ? $"({switchValue.SQL})" : switchValue.SQL;

            if (field.Cases != null && field.Cases.Count > 0)
            {
                StringBuilder sql = new StringBuilder();

                sql.Append($"case");

                foreach (var item in field.Cases)
                {
                    sql.Append(" when ");

                    int index = 0;

                    foreach (var testValueField in item.TestValues)
                    {
                        var testValue = testValueField.GetBasicExpression(parameterContext);

                        string testValueSql = Helper.CheckIsPriority(OperationType.Equal, testValue.Type, true) ? $"({testValue.SQL})" : testValue.SQL;

                        if (index > 0)
                            sql.Append(" or ");

                        sql.Append($"{switchValueSql} = {testValueSql}");

                        index++;
                    }

                    sql.Append(" then ");

                    var testBody = item.Body.GetBasicExpression(parameterContext);

                    if (item.Body.Type == FieldType.Subquery)
                        sql.Append($"({testBody.SQL})");
                    else
                        sql.Append(testBody.SQL);
                }

                if (field.DefaultBody != null)
                {
                    sql.Append(" else ");

                    if (defaultBody.Type == OperationType.Subtract)
                        sql.Append($"({defaultBody.SQL})");
                    else
                        sql.Append(defaultBody.SQL);
                }

                sql.Append(" end");

                return new ExpressionInfo(sql.ToString(), OperationType.Default);
            }
            else
            {
                return defaultBody;
            }
        }

        /// <summary>
        /// 创建 Switch 分支字段的布尔表示 Sql 信息。
        /// </summary>
        /// <param name="parameterContext">创建表示 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="field">待创建表示 Sql 信息的 Switch 分支字段。</param>
        /// <returns>该 Switch 分支字段对应的布尔 Sql 表示结果。</returns>
        public ExpressionInfo CreateSwitchBoolSql(IParameterContext parameterContext, SwitchField field)
        {
            return new ExpressionInfo($"({field.GetBasicExpression(parameterContext).SQL}) = 1", OperationType.Equal);
        }

        /// <summary>
        /// 创建一元操作字段的基础表示 Sql 信息。
        /// </summary>
        /// <param name="parameterContext">创建表示 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="field">待创建表示 Sql 信息的一元操作字段。</param>
        /// <returns>该一元操作字段对应的基础 Sql 表示结果。</returns>
        public ExpressionInfo CreateUnaryBasicSql(IParameterContext parameterContext, UnaryField field)
        {
            if (field.OperationType == OperationType.Not)
            {
                if (field.IsBoolDataType)
                {
                    return BoolToBasicExpression(field.DataContext, field.GetBoolExpression(parameterContext));
                }
                else
                {
                    ExpressionInfo operand = field.Operand.GetBasicExpression(parameterContext);

                    string operandSql = Helper.CheckIsPriority(OperationType.Not, operand.Type, true) ? $"({operand.SQL})" : operand.SQL;

                    return new ExpressionInfo($"~{operandSql}", OperationType.Not);
                }
            }
            else if (field.OperationType == OperationType.Negate)
            {
                ExpressionInfo operand = field.Operand.GetBasicExpression(parameterContext);

                string operandSql = Helper.CheckIsPriority(OperationType.Negate, operand.Type, true) ? $"({operand.SQL})" : operand.SQL;

                return new ExpressionInfo($"-{operandSql}", OperationType.Negate);
            }
            else if (field.OperationType == OperationType.Convert)
            {
                ExpressionInfo operand = field.Operand.GetBasicExpression(parameterContext);

                string operandSql = operand.Type == OperationType.Subquery ? $"({operand.SQL})" : operand.SQL;

                if (field.DataType == _DateTimeType)
                    return new ExpressionInfo($"datetime({operandSql})", OperationType.Call);
                else
                    return new ExpressionInfo($"cast({operandSql} as {field.DataContext.NetTypeToDBType(field.DataType)})", OperationType.Call);
            }

            throw new Exception($"未能创建指定一元操作字段对应的基础表示信息，操作类型为：{field.OperationType}");
        }

        /// <summary>
        /// 创建一元操作字段的布尔表示 Sql 信息。
        /// </summary>
        /// <param name="parameterContext">创建表示 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="field">待创建表示 Sql 信息的一元操作字段。</param>
        /// <returns>该一元操作字段对应的布尔 Sql 表示结果。</returns>
        public ExpressionInfo CreateUnaryBoolSql(IParameterContext parameterContext, UnaryField field)
        {
            if (field.OperationType == OperationType.Not && field.IsBoolDataType)
            {
                if (field.Operand.Type == FieldType.Method)
                {
                    MethodField operandMethodField = (MethodField)field.Operand;

                    if (operandMethodField.MethodInfo.ReflectedType == _StringType && operandMethodField.Parameters != null && operandMethodField.Parameters.Count == 1 && operandMethodField.Parameters[0].DataType == _StringType && operandMethodField.Parameters[0] is BasicField parameter)
                    {
                        //对 string 静态的 IsNullOrEmpty 或 IsNullOrWhiteSpace 方法取反时做特殊操作
                        if ((operandMethodField.MethodInfo.Name == "IsNullOrEmpty" || operandMethodField.MethodInfo.Name == "IsNullOrWhiteSpace") && operandMethodField.ObjectField == null)
                        {
                            ExpressionInfo param = parameter.GetBasicExpression(parameterContext);

                            string parameterString = param.Type == OperationType.Subquery ? $"({param.SQL})" : param.SQL;

                            if (operandMethodField.MethodInfo.Name == "IsNullOrEmpty")
                                return new ExpressionInfo($"{parameterString} is not null and {parameterString} != ''", OperationType.AndAlso);
                            else
                                return new ExpressionInfo($"{parameterString} is not null and trim({parameterString}) != ''", OperationType.AndAlso);
                        }
                        //对 string  类型的 StartsWith 或 Contains 方法取反时做特殊操作
                        else if (operandMethodField.ObjectField != null && operandMethodField.ObjectField is BasicField basicField && (operandMethodField.MethodInfo.Name == "StartsWith" || operandMethodField.MethodInfo.Name == "Contains"))
                        {
                            ExpressionInfo basic = basicField.GetBasicExpression(parameterContext);
                            ExpressionInfo param = parameter.GetBasicExpression(parameterContext);

                            string objectSql = basic.Type == OperationType.Subquery ? $"({basic})" : basic.SQL;
                            string searchString = param.Type == OperationType.Subquery ? $"({param.SQL})" : param.SQL;
                            string checkString = operandMethodField.MethodInfo.Name == "StartsWith" ? "!=" : "<";

                            if (!field.DataContext.IsCaseSensitive)
                                return new ExpressionInfo($"instr(lower({objectSql}), lower({searchString})) {checkString} 1", operandMethodField.MethodInfo.Name == "StartsWith" ? OperationType.NotEqual : OperationType.LessThan);
                            else
                                return new ExpressionInfo($"instr({objectSql}, {searchString}) {checkString} 1", operandMethodField.MethodInfo.Name == "StartsWith" ? OperationType.NotEqual : OperationType.LessThan);
                        }
                        //对 string  类型的 EndsWith 方法取反时做特殊操作
                        else if (operandMethodField.ObjectField != null && operandMethodField.ObjectField is BasicField stringField && operandMethodField.MethodInfo.Name == "EndsWith")
                        {
                            ExpressionInfo basic = stringField.GetBasicExpression(parameterContext);
                            ExpressionInfo param = parameter.GetBasicExpression(parameterContext);

                            string objectSql = basic.Type == OperationType.Subquery ? $"({basic})" : basic.SQL;
                            string searchString = param.Type == OperationType.Subquery ? $"({param.SQL})" : param.SQL;

                            if (field.DataContext.IsCaseSensitive)
                                return new ExpressionInfo($"instr(lower({objectSql}), lower({searchString})) != length({objectSql}) - length({searchString}) + 1", OperationType.NotEqual);
                            else
                                return new ExpressionInfo($"instr({objectSql}, {searchString}) != length({objectSql}) - length({searchString}) + 1", OperationType.NotEqual);
                        }
                    }
                }
                else
                {
                    ExpressionInfo operand = field.Operand.GetBasicExpression(parameterContext);

                    string operandSql = Helper.CheckIsPriority(OperationType.Equal, operand.Type, false) ? $"({operand.SQL})" : operand.SQL;

                    return new ExpressionInfo($"{operandSql} = 0", OperationType.Equal);
                }
            }

            throw new Exception($"未能创建指定一元操作字段对应布尔形态的表示信息，操作类型为：{field.OperationType}");
        }

        /// <summary>
        /// 对指定的原始字段名进行编码。
        /// </summary>
        /// <param name="name">需要编码的原始字段名。</param>
        /// <returns>编码后的名称。</returns>
        public string EncodeFieldName(string name)
        {
            return $"`{name}`";
        }

        /// <summary>
        /// 对指定的原始表名进行编码。
        /// </summary>
        /// <param name="name">需要编码的原始表名。</param>
        /// <returns>编码后的名称。</returns>
        public string EncodeTableName(string name)
        {
            return $"`{name}`";
        }

        /// <summary>
        /// 对指定的原始视图名进行编码。
        /// </summary>
        /// <param name="name">需要编码的原始视图名。</param>
        /// <returns>编码后的名称。</returns>
        public string EncodeViewName(string name)
        {
            return $"`{name}`";
        }

        /// <summary>
        /// 使用指定的别名下标生成一个数据源别名（必须保证不同下标生成的别名不同，相同下标生成的别名相同，且所生成的别名不得是数据库中的关键字）。
        /// </summary>
        /// <param name="aliasIndex">生成别名所使用的下标。</param>
        /// <returns>使用指定别名下标生成好的别名。</returns>
        public string GenerateDataSourceAlias(int aliasIndex)
        {
            return $"T{aliasIndex}";
        }

        /// <summary>
        /// 生成指定数据源对应的删除 Sql 语句。
        /// </summary>
        /// <param name="parameterContext">创建表示 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="deletingDataSources">需要删除数据行的源数组。</param>
        /// <param name="fromDataSource">待删除数据行的目标来源。</param>
        /// <param name="where">删除数据行时的条件字段。</param>
        /// <returns>生成好的删除 Sql 语句。</returns>
        public string GenerateDeleteSql(IParameterContext parameterContext, TableDataSource[] deletingDataSources, DataSource.DataSource fromDataSource, BasicField where)
        {
            if (deletingDataSources.Length == 1)
            {
                if (deletingDataSources[0] == fromDataSource)
                {
                    Dictionary<object, DbParameter> pars = new Dictionary<object, DbParameter>();

                    string sql;

                    if (where == null)
                        sql = $"delete from {deletingDataSources[0].Name}";
                    else
                        sql = $"delete from {deletingDataSources[0].Name} as {deletingDataSources[0].Alias} where {where.GetBoolExpression(parameterContext).SQL}";

                    return sql;
                }
                else
                {
                    throw new Exception("Sqlite 不支持 delete x from T 格式的关联删除语法");
                }
            }
            else
            {
                throw new Exception("Sqlite 不支持一次性删除多表数据");
            }
        }

        /// <summary>
        /// 使用指定的别名下标生成一个字段别名（必须保证不同下标生成的别名不同，相同下标生成的别名相同，且所生成的别名不得是数据库中的关键字）。
        /// </summary>
        /// <param name="aliasIndex">生成别名所使用的下标。</param>
        /// <returns>使用指定别名下标生成好的别名。</returns>
        public string GenerateFieldAlias(int aliasIndex)
        {
            return $"F{aliasIndex}";
        }

        /// <summary>
        /// 生成指定数据源对应的插入 Sql 语句。
        /// </summary>
        /// <param name="parameterContext">生成 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="dataSource">待生成语句的数据源对象。</param>
        /// <param name="fields">需要插入的字段集合。</param>
        /// <param name="args">需要插入字段对应的参数集合。</param>
        /// <returns>生成好的插入 Sql 语句。</returns>
        public string GenerateInsertSql(IParameterContext parameterContext, TableDataSource dataSource, ReadOnlyList<OriginalField> fields, ReadOnlyList<BasicField> args)
        {
            StringBuilder insertFields = new StringBuilder();
            StringBuilder insertValues = new StringBuilder();

            for (int i = 0; i < fields.Count; i++)
            {
                OriginalField field = fields[i];
                BasicField valueField = args[i];

                var value = valueField.GetBasicExpression(parameterContext);

                if (i > 0)
                {
                    insertFields.Append(",");
                    insertValues.Append(",");
                }

                insertFields.Append(field.FieldName);

                if (value.Type == OperationType.Subquery)
                    insertValues.Append($"({value.SQL})");
                else
                    insertValues.Append(value.SQL);
            }

            return $"insert into {dataSource.Name}({insertFields}) values ({insertValues})";
        }

        /// <summary>
        /// 生成指定数据源对应的插入 Sql 语句。
        /// </summary>
        /// <param name="parameterContext">生成 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="dataSource">待生成语句的数据源对象。</param>
        /// <param name="insertDataSource">需要插入到数据表的数据源。</param>
        /// <returns>生成好的插入 Sql 语句。</returns>
        public string GenerateInsertSql(IParameterContext parameterContext, TableDataSource dataSource, BasicDataSource insertDataSource)
        {
            StringBuilder insertFields = new StringBuilder();

            ObjectField insertField = (ObjectField)(insertDataSource.SelectField ?? insertDataSource.RootField);

            if (insertField.ConstructorInfo != null && insertField.ConstructorInfo.Parameters != null && insertField.ConstructorInfo.Parameters.Count > 0)
            {
                throw new Exception($"生成 {insertField.DataType.FullName} 类所映射数据库表的查询插入 Sql 语句时，查询对象的实例不能有构造参数");
            }
            else
            {
                ObjectField rootField = (ObjectField)dataSource.RootField;

                foreach (var item in insertField.Members)
                {
                    if (rootField.Members.TryGetValue(item.Key, out Core.MemberInfo memberInfo))
                    {
                        if (memberInfo.Field.Type == FieldType.Original)
                        {
                            if (insertFields.Length > 0)
                                insertFields.Append(",");

                            insertFields.Append(((OriginalField)memberInfo.Field).FieldName);
                        }
                        else
                        {
                            throw new Exception($"生成 {rootField.DataType.FullName} 类所映射数据库表的插入语句时，{item.Key} 成员并非是原始数据字段");
                        }
                    }
                    else
                    {
                        throw new Exception($"生成 {rootField.DataType.FullName} 类所映射数据库表的插入语句时，{item.Key} 成员未找到");
                    }
                }

                string querySql = GenerateSelectSql(parameterContext, insertField, insertDataSource, false);

                if (insertDataSource.Type == SourceType.Union)
                    querySql = $"({querySql})";

                return $"insert into { dataSource.Name }({ insertFields }) { querySql }";
            }
        }

        /// <summary>
        /// 生成指定数据源对应的查询 Sql 语句。
        /// </summary>
        /// <param name="parameterContext">生成 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="dataSource">待生成语句的数据源对象。</param>
        /// <returns>生成好的查询 Sql 语句。</returns>
        public string GenerateSelectSql(IParameterContext parameterContext, BasicDataSource dataSource)
        {
            return GenerateSelectSql(parameterContext, dataSource.SelectField ?? dataSource.RootField, dataSource, true);
        }

        /// <summary>
        /// 生成指定数据源对应的更新 Sql 语句。
        /// </summary>
        /// <param name="parameterContext">生成 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="updateGroups">所有待更新的数据组。</param>
        /// <param name="fromDataSource">待更新数据的目标来源。</param>
        /// <param name="where">更新数据行时的条件字段。</param>
        /// <returns>生成好的更新 Sql 语句。</returns>
        public string GenerateUpdateSql(IParameterContext parameterContext, ReadOnlyList<UpdateGroup> updateGroups, DataSource.DataSource fromDataSource, BasicField where)
        {
            if (updateGroups.Count > 1)
            {
                throw new Exception("Sqlite 不支持一次性修改多个数据表的数据");
            }
            else if (updateGroups[0].UpdateSource != fromDataSource)
            {
                throw new Exception("Sqlite 不支持关联修改操作");
            }
            else
            {
                StringBuilder setSql = new StringBuilder();

                foreach (var item in updateGroups[0].UpdateFields)
                {
                    if (setSql.Length > 0)
                        setSql.Append(",");

                    var value = item.Value.GetBasicExpression(parameterContext);

                    setSql.Append(item.Field.FieldName);

                    setSql.Append(" = ");

                    if (value.Type == OperationType.Subquery)
                        setSql.Append($"({value.SQL})");
                    else
                        setSql.Append(value.SQL);
                }

                string result;

                if (where == null)
                    result = $"update {updateGroups[0].UpdateSource.Name} as {updateGroups[0].UpdateSource.Alias} set {setSql}";
                else
                    result = $"update {updateGroups[0].UpdateSource.Name} as {updateGroups[0].UpdateSource.Alias} set {setSql} where {where.GetBoolExpression(parameterContext).SQL}";

                return result;
            }
        }

        /// <summary>
        /// 生成校验某个数据库表是否存在的 Sql 语句。
        /// </summary>
        /// <param name="parameterContext">生成 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="tableName">需要创建校验 Sql 语句的数据库表名称。</param>
        /// <returns>生成好的校验 Sql 语句。</returns>
        internal string GenerateExistsTableSql(IParameterContext parameterContext, string tableName)
        {
            return $"select count(1) from sqlite_master where type = 'table' and name = {parameterContext.Add(tableName)}";
        }

        /// <summary>
        /// 生成校验某个数据库视图是否存在的 Sql 语句。
        /// </summary>
        /// <param name="parameterContext">生成 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="viewName">需要创建校验 Sql 语句的数据库视图名称。</param>
        /// <returns>生成好的校验 Sql 语句。</returns>
        internal string GenerateExistsViewSql(IParameterContext parameterContext, string viewName)
        {
            return $"SELECT count(*) FROM sqlite_master WHERE type = 'view' AND name = {parameterContext.Add(viewName)}";
        }

        /// <summary>
        /// 生成创建数据库表的 Sql 语句。
        /// </summary>
        /// <param name="parameterContext">生成 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="tableDataSource">需要生成创建数据库表 Sql 语句的数据源。</param>
        /// <returns>生成好的创建 Sql 语句。</returns>
        internal string GenerateCreateTableSql(IParameterContext parameterContext, TableDataSource tableDataSource)
        {
            StringBuilder fields = new StringBuilder();
            StringBuilder indices = new StringBuilder();

            foreach (var item in ((ObjectField)tableDataSource.RootField).Members)
            {
                OriginalField field = (OriginalField)item.Value.Field;

                if (fields.Length > 0)
                    fields.Append($",{Environment.NewLine}");

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

                    if (field.DataType == _IntType
                        || field.DataType == _UIntType
                        || field.DataType == _LongType
                        || field.DataType == _ULongType
                        || field.DataType == _ShortType
                        || field.DataType == _UShortType
                        || field.DataType == _ByteType
                        || field.DataType == _SByteType
                        || field.DataType == _FloatType
                        || field.DataType == _DoubleType
                        || field.DataType == _DecimalType
                        || field.DataType == _BoolType
                        || field.DataType.IsEnum
                        || (new Regex(@"^\s*(?:(?:date)|(?:datetime))\s*\(\S|\s*\)\s*$", RegexOptions.IgnoreCase).IsMatch(field.DefaultValue.ToString()) && field.DataType == _DateTimeType))
                    {
                        fields.Append($" default ({field.DefaultValue})");
                    }
                    else
                    {
                        fields.Append($" default ('{field.DefaultValue}')");
                    }
                }

                if (!string.IsNullOrWhiteSpace(field.CheckConstraint))
                    fields.Append($" check({field.CheckConstraint})");
            }

            if (tableDataSource.Indices != null && tableDataSource.Indices.Count > 0)
            {
                foreach (var item in tableDataSource.Indices)
                {
                    if (indices.Length > 0)
                        indices.Append(Environment.NewLine);

                    if (item.Type == IndexType.Unique)
                        indices.Append("create unique index ");
                    else
                        indices.Append("create index ");

                    indices.Append($"{item.Name} on {tableDataSource.Name}(");

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

            createSql.Append(tableDataSource.Name);

            createSql.AppendLine("(");

            createSql.Append(fields);

            createSql.Append($"{Environment.NewLine});{Environment.NewLine}");

            if (indices.Length > 0)
                createSql.Append($"{Environment.NewLine}{indices}");

            return createSql.ToString();
        }

        /// <summary>
        /// 生成创建数据库视图的 Sql 语句。
        /// </summary>
        /// <param name="parameterContext">生成 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="viewDataSource">需要生成创建数据库视图 Sql 语句的数据源。</param>
        /// <returns>生成好的创建 Sql 语句。</returns>
        internal string GenerateCreateViewSql(IParameterContext parameterContext, ViewDataSource viewDataSource)
        {
            return $"create view {viewDataSource.Name}{Environment.NewLine}as{Environment.NewLine}{viewDataSource.CreateSQL}";
        }

        /// <summary>
        /// 生成删除数据库表的 Sql 语句。
        /// </summary>
        /// <param name="parameterContext">生成 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="tableDataSource">需要生成删除数据库表 Sql 语句的数据源。</param>
        /// <returns>生成好的删除 Sql 语句。</returns>
        internal string GenerateDeleteTableSql(IParameterContext parameterContext, TableDataSource tableDataSource)
        {
            return $"drop table {tableDataSource.Name}";
        }

        /// <summary>
        /// 生成删除数据库视图的 Sql 语句。
        /// </summary>
        /// <param name="parameterContext">生成 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="viewSource">需要生成删除数据库视图 Sql 语句的数据源。</param>
        /// <returns>生成好的删除 Sql 语句。</returns>
        internal string GenerateDeleteViewSql(IParameterContext parameterContext, ViewDataSource viewSource)
        {
            return $"drop view {viewSource.Name}";
        }

        /// <summary>
        /// 生成一个用于查询所有数据库表名的 Sql 语句。
        /// </summary>
        /// <param name="parameterContext">生成 Sql 时用于参数化操作的上下文对象。</param>
        /// <returns>生成好的查询 Sql 语句。</returns>
        internal string GenerateSelectAllTableNameSql(IParameterContext parameterContext)
        {
            return "select `name` from sqlite_master where type = 'table'";
        }

        /// <summary>
        /// 生成一个用于查询所有数据库视图名的 Sql 语句。
        /// </summary>
        /// <param name="parameterContext">生成 Sql 时用于参数化操作的上下文对象。</param>
        /// <returns>生成好的查询 Sql 语句。</returns>
        internal string GenerateSelectAllViewNameSql(IParameterContext parameterContext)
        {
            return "select `name` from sqlite_master where type = 'view'";
        }

        /// <summary>
        /// 将指定的布尔表示 Sql 语句转换成普通的查询表示语句。
        /// </summary>
        /// <param name="dataContext">转换表示 Sql 时所使用的数据操作上下文对象。</param>
        /// <param name="expressionInfo">待转换的布尔表示语句。</param>
        /// <returns>转换后的基础表示 Sql 信息。</returns>
        private ExpressionInfo BoolToBasicExpression(IDataContext dataContext, ExpressionInfo expressionInfo)
        {
            string sql = expressionInfo.SQL;

            if (expressionInfo.Type == OperationType.Subquery)
                sql = $"({sql})";

            return new ExpressionInfo($"cast({sql} as boolean)", OperationType.Call);
        }

        /// <summary>
        /// 生成指定数据源中某个字段的查询 Sql 语句。
        /// </summary>
        /// <param name="parameterContext">创建表示 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="selectField">待查询字段信息。</param>
        /// <param name="belongDataSource">待查询字段归属的数据源。</param>
        /// <param name="applyFieldAlias">是否应当为查询字段应用上别名。</param>
        /// <returns>生成好的查询 Sql 语句。</returns>
        private string GenerateSelectSql(IParameterContext parameterContext, Field.Field selectField, BasicDataSource belongDataSource, bool applyFieldAlias)
        {
            UnionDataSource unionDataSource = belongDataSource.Type == SourceType.Union ? (UnionDataSource)belongDataSource : null;

            if (unionDataSource != null && unionDataSource.SelectField == null && !unionDataSource.Limit.HasValue && !unionDataSource.IsDistinctly && (unionDataSource.GroupFields == null || unionDataSource.GroupFields.Count < 1) && (unionDataSource.SortItems == null || unionDataSource.SortItems.Count < 1) && unionDataSource.Where == null)
            {
                return GenerateUnionSql(parameterContext, (UnionDataSource)belongDataSource);
            }
            else
            {
                StringBuilder sqlFields = new StringBuilder();
                StringBuilder orderBy = new StringBuilder();
                StringBuilder groupBy = new StringBuilder();
                string sqlFrom = GenerateFromSql(parameterContext, belongDataSource, false);
                string sqlWhere = string.Empty;
                string distinct = belongDataSource.IsDistinctly ? "distinct " : string.Empty;
                string limit = string.Empty;

                AppendSelectField(parameterContext, selectField, sqlFields, new HashSet<Field.Field>(), applyFieldAlias);

                if (belongDataSource.SortItems != null && belongDataSource.SortItems.Count > 0)
                {
                    orderBy.Append(" order by ");

                    foreach (var item in belongDataSource.SortItems)
                    {
                        if (orderBy.Length > 10)
                            orderBy.Append(", ");

                        var by = item.Field.GetBasicExpression(parameterContext);

                        if (by.Type == OperationType.Subquery)
                            orderBy.Append($"({by.SQL})");
                        else
                            orderBy.Append(by.SQL);

                        if (item.Type == SortType.Descending)
                            orderBy.Append(" desc");
                    }
                }

                if (belongDataSource.GroupFields != null && belongDataSource.GroupFields.Count > 0)
                {
                    groupBy.Append(" group by ");

                    foreach (var item in belongDataSource.GroupFields)
                    {
                        if (groupBy.Length > 10)
                            groupBy.Append(", ");

                        var by = item.GetBasicExpression(parameterContext);

                        if (by.Type == OperationType.Subquery)
                            groupBy.Append($"({by.SQL})");
                        else
                            groupBy.Append(by.SQL);
                    }
                }

                if (belongDataSource.Where != null)
                    sqlWhere = $" where {belongDataSource.Where.GetBoolExpression(parameterContext).SQL}";

                if (belongDataSource.Limit != null && belongDataSource.Limit.HasValue)
                {
                    if (belongDataSource.Limit.Value.Start == 0)
                        limit = $" limit {belongDataSource.Limit.Value.Count}";
                    else
                        limit = $" limit {belongDataSource.Limit.Value.Count} offset {belongDataSource.Limit.Value.Start}";
                }

                return $"select {distinct}{sqlFields} from {sqlFrom}{sqlWhere}{groupBy}{orderBy}{limit}";
            }
        }

        /// <summary>
        /// 生成指定数据源在被用作 Select 查询中的 From 数据源时的 Sql。
        /// </summary>
        /// <param name="parameterContext">创建表示 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="dataSource">被用作查询 Sql 中的 From 数据源。</param>
        /// <param name="forceQuery">若是原始数据源是否强制启用查询。</param>
        /// <returns>生成好的 Sql 查询语句。</returns>
        private string GenerateFromSql(IParameterContext parameterContext, DataSource.DataSource dataSource, bool forceQuery)
        {
            if (dataSource.Type == SourceType.Table || dataSource.Type == SourceType.View)
            {
                OriginalDataSource originalDataSource = (OriginalDataSource)dataSource;

                if (forceQuery && originalDataSource.SelectField != null)
                    return $"({GenerateSelectSql(parameterContext, originalDataSource.SelectField, originalDataSource, true)}) as {originalDataSource.Alias}";
                else
                    return $"{originalDataSource.Name} as {originalDataSource.Alias}";
            }
            else if (dataSource.Type == SourceType.Select)
            {
                SelectDataSource selectDataSource = (SelectDataSource)dataSource;

                if (forceQuery)
                    return $"({GenerateSelectSql(parameterContext, selectDataSource.SelectField ?? selectDataSource.RootField, selectDataSource, true)}) as {selectDataSource.Alias}";
                else
                    return GenerateFromSql(parameterContext, selectDataSource.QueryDataSource, true);
            }
            else if (dataSource.Type == SourceType.Union)
            {
                UnionDataSource unionDataSource = (UnionDataSource)dataSource;

                if (forceQuery && unionDataSource.SelectField != null)
                    return $"({GenerateSelectSql(parameterContext, unionDataSource.SelectField, unionDataSource, true)}) as {unionDataSource.Alias}";
                else
                    return $"({GenerateUnionSql(parameterContext, unionDataSource)}) as {unionDataSource.Alias}";
            }
            else
            {
                JoinDataSource joinDataSource = (JoinDataSource)dataSource;

                string left = GenerateFromSql(parameterContext, joinDataSource.Left, true);
                string right = GenerateFromSql(parameterContext, joinDataSource.Right, true);

                if (joinDataSource.Type == SourceType.CrossJoin)
                {
                    return $"{left} cross join {right}";
                }
                else
                {
                    string joinType;

                    switch (joinDataSource.Type)
                    {
                        case SourceType.LeftJoin:
                            joinType = "left join";
                            break;
                        case SourceType.RightJoin:
                            string temp = left;

                            left = right;

                            right = temp;

                            joinType = "left join";
                            break;
                        case SourceType.FullJoin:
                            throw new Exception("Sqlite 不支持全外连接查询方式");
                        default:
                            joinType = "inner join";
                            break;
                    }

                    return $"{left} {joinType} {right} on {joinDataSource.On.GetBoolExpression(parameterContext).SQL}";
                }
            }
        }

        /// <summary>
        /// 生成指定数据合并数据源的合并 Sql 语句。
        /// </summary>
        /// <param name="parameterContext">创建表示 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="unionDataSource">待生成 Sql 语句的数据源。</param>
        /// <returns>生成好的合并 Sql 语句。</returns>
        private string GenerateUnionSql(IParameterContext parameterContext, UnionDataSource unionDataSource)
        {
            string unionType;

            switch (unionDataSource.UnionType)
            {
                case UnionType.Intersect:
                    unionType = "intersect";
                    break;
                case UnionType.Union:
                    unionType = "union";
                    break;
                case UnionType.UnionALL:
                    unionType = "union all";
                    break;
                case UnionType.Minus:
                    unionType = "except";
                    break;
                default:
                    throw new Exception($"Sqlite 不支持 {unionDataSource.UnionType} 的方式合并数据源");
            }

            return $"{GenerateSelectSql(parameterContext, unionDataSource.Main.SelectField ?? unionDataSource.Main.RootField, unionDataSource.Main, true)}{Environment.NewLine}{unionType}{Environment.NewLine}{GenerateSelectSql(parameterContext, unionDataSource.Affiliation.SelectField ?? unionDataSource.Affiliation.RootField, unionDataSource.Affiliation, true)}";
        }

        /// <summary>
        /// 追加待查询字段信息到字段列表中。
        /// </summary>
        /// <param name="parameterContext">创建表示 Sql 时用于参数化操作的上下文对象。</param>
        /// <param name="selectField">待追加的查询字段。</param>
        /// <param name="sqlFields">用于保存查询字段信息的字符串构造器。</param>
        /// <param name="appendedFields">已经追加过的字段集合。</param>
        /// <param name="applyFieldAlias">生成的查询字段是否应当应用上字段别名。</param>
        private void AppendSelectField(IParameterContext parameterContext, Field.Field selectField, StringBuilder sqlFields, HashSet<Field.Field> appendedFields, bool applyFieldAlias)
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
                            AppendSelectField(parameterContext, item, sqlFields, appendedFields, applyFieldAlias);
                        }
                    }

                    if (objectField.Members != null && objectField.Members.Count > 0)
                    {
                        foreach (var item in objectField.Members)
                        {
                            AppendSelectField(parameterContext, item.Value.Field, sqlFields, appendedFields, applyFieldAlias);
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
                            AppendSelectField(parameterContext, item, sqlFields, appendedFields, applyFieldAlias);
                        }
                    }

                    foreach (var item in collectionField)
                    {
                        AppendSelectField(parameterContext, item, sqlFields, appendedFields, applyFieldAlias);
                    }
                }
                else if (selectField is BasicField basicField)
                {
                    var basic = basicField.GetBasicExpression(parameterContext);

                    if (sqlFields.Length > 0)
                        sqlFields.Append(", ");

                    if (basic.Type == OperationType.Subquery)
                        sqlFields.Append($"({basic.SQL})");
                    else
                        sqlFields.Append(basic.SQL);

                    if (applyFieldAlias && !string.IsNullOrWhiteSpace(basicField.Alias))
                    {
                        string fieldInnerName = null;

                        if (basicField.Type == FieldType.Original)
                            fieldInnerName = ((OriginalField)basicField).FieldName;
                        else if (basicField.Type == FieldType.Quote)
                            fieldInnerName = ((QuoteField)basicField).QuoteFieldName;

                        if (basicField.Alias != fieldInnerName)
                            sqlFields.Append($" as {basicField.Alias}");
                    }
                }

                appendedFields.Add(selectField);
            }
        }
    }
}
