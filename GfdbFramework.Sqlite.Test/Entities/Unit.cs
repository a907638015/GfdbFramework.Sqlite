using System;
using System.Collections.Generic;
using System.Text;
using GfdbFramework.Attribute;

namespace GfdbFramework.Sqlite.Test.Entities
{
    /// <summary>
    /// 商品单位实体类。
    /// </summary>
    [Mapping("Units")]
    public class Unit : BaseEntity
    {
        /// <summary>
        /// 获取或设置该商品单位的名称。
        /// </summary>
        [Field(IsNullable = Enum.FieldNullableMode.NotNullable, SimpleIndex = Enum.SortType.Ascending)]
        public string Name { get; set; }
    }
}
