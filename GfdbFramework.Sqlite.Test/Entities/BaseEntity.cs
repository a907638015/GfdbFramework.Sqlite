using System;
using System.Collections.Generic;
using System.Text;
using GfdbFramework.Attribute;

namespace GfdbFramework.Sqlite.Test.Entities
{
    /// <summary>
    /// 所有实体类的基类。
    /// </summary>
    public class BaseEntity
    {
        /// <summary>
        /// 获取或设置该数据的主键值。
        /// </summary>
        [Field(IsPrimaryKey = true, IsAutoincrement = true, DataType = "integer")]
        public long ID { get; set; }

        /// <summary>
        /// 获取或设置该数据是否已被软删除。
        /// </summary>
        [Field(DefaultValue = 0, IsNullable = false, SimpleIndex = Enum.SortType.Ascending)]
        public bool IsDeleted { get; set; }

        /// <summary>
        /// 获取或设置该数据行创建的时间。
        /// </summary>
        [Field(DefaultValue = "datetime('now','localtime')", IsNullable = false, SimpleIndex = Enum.SortType.Descending)]
        public DateTime CreateTime { get; set; }

        /// <summary>
        /// 获取或设置创建该数据的用户主键值。
        /// </summary>
        [Field(IsInsertForDefault = true, DefaultValue = 0, IsNullable = false, SimpleIndex = Enum.SortType.Ascending)]
        public long CreateUID { get; set; }
    }
}
