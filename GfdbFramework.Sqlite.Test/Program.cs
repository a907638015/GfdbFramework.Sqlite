﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using GfdbFramework.Attribute;
using GfdbFramework.Core;

namespace GfdbFramework.Sqlite.Test
{
    class Program
    {
        static readonly Random rd = new Random();
        static readonly string[] userNames = new string[] { "张三", "李四", "王五", "赵六", "孙八", "周九" };
        static readonly string[] brandNames = new string[] { "白沙", "利群", "芙蓉王", "黄金叶", "中华", "红塔山", "红双喜" };
        static readonly string[] classifyNames = new string[] { "日用品", "水产品", "蔬菜", "坚果", "化妆品", "零食", "医疗用品", "其他" };
        static readonly string[] unitNames = new string[] { "克", "千克", "斤", "两", "吨", "方", "包", "箱", "台", "件" };
        static readonly string[] commodityNames = new string[] { "精品白沙（二代）", "利群（白色硬壳包装）", "芙蓉王（黄涩硬壳包装）", "波力海苔", "人参果", "东北大米", "帝王蟹" };

        static void Main(string[] args)
        {
            DataContext dataContext = new DataContext();

            if (!dataContext.ExistsDatabase(new DatabaseInfo("TestDB")))
            {
                Console.WriteLine($"{Environment.NewLine}----- 初始化数据库 ----{Environment.NewLine}");

                //创建数据库
                dataContext.CreateDatabase(new DatabaseInfo("TestDB"));

                //创建各种表
                dataContext.CreateTable(dataContext.Users);
                dataContext.CreateTable(dataContext.Commodities);
                dataContext.CreateTable(dataContext.Classifies);
                dataContext.CreateTable(dataContext.Brands);
                dataContext.CreateTable(dataContext.Units);

                Console.WriteLine($"{Environment.NewLine}----- 随机添加测试数据 ----{Environment.NewLine}");

                //添加用户信息
                for (int i = 0; i < userNames.Length; i++)
                {
                    Entities.User user = new Entities.User()
                    {
                        Account = GetRandomString(16),
                        Password = "123456",
                        Name = userNames[i],
                        CreateTime = DateTime.Now,
                        JobNumber = GetRandomString(5)
                    };

                    dataContext.Users.Insert(user);

                    Console.WriteLine($"新增用户 ID 为：{user.ID}");
                }

                //添加分类信息
                for (int i = 0; i < classifyNames.Length; i++)
                {
                    //此处会生成两个插入 SQL，第一条 SQL 先去随机获得一个用户 ID，然后再将这个 ID 传入插入 SQL 作为变量使用
                    dataContext.Classifies.Insert(new Entities.Classify()
                    {
                        Code = GetRandomString(6),
                        Name = classifyNames[i],
                        CreateTime = DateTime.Now,
                        CreateUID = dataContext.Users.Select(user => user.ID).Ascending(user => DBFun.NewLong()).First()  //随机获取一个用户 ID 作为创建者
                    });
                }

                //添加品牌信息
                for (int i = 0; i < brandNames.Length; i++)
                {
                    //此处只会生成一个插入 SQL， CreateUID 的值会改成子查询
                    dataContext.Brands.Insert(() => new Entities.Brand()
                    {
                        Code = GetRandomString(6),
                        Name = brandNames[i],
                        CreateTime = DateTime.Now,
                        CreateUID = dataContext.Users.Select(user => user.ID).Ascending(user => DBFun.NewLong()).First()  //随机获取一个用户 ID 作为创建者
                    });
                }

                //添加单位信息
                for (int i = 0; i < unitNames.Length; i++)
                {
                    dataContext.Units.Insert(() => new Entities.Unit()
                    {
                        Name = unitNames[i],
                        CreateTime = DateTime.Now,
                        CreateUID = dataContext.Users.Select(user => user.ID).Ascending(user => DBFun.NewLong()).First()  //随机获取一个用户 ID 作为创建者
                    });
                }

                //添加商品信息
                for (int i = 0; i < commodityNames.Length; i++)
                {
                    dataContext.Commodities.Insert(() => new Entities.Commodity()
                    {
                        Name = commodityNames[i],
                        CreateTime = DateTime.Now,
                        CreateUID = dataContext.Users.Select(user => user.ID).Ascending(user => DBFun.NewLong()).First(),  //随机获取一个用户 ID 作为创建者
                        PackageUnitID = dataContext.Units.Select(unit => unit.ID).Ascending(unit => DBFun.NewLong()).First(),
                        MiddleUnitID = dataContext.Units.Select(unit => unit.ID).Ascending(unit => DBFun.NewLong()).First(),
                        MiddleQuantity = 50,
                        MinimumUnitID = dataContext.Units.Select(unit => unit.ID).Ascending(unit => DBFun.NewLong()).First(),
                        MinimumQuantity = 10,
                        BrandID = dataContext.Brands.Select(brand => brand.ID).Ascending(brand => DBFun.NewLong()).First(),
                        ClassifyID = dataContext.Brands.Select(classify => classify.ID).Ascending(classify => DBFun.NewLong()).First(),
                        Code = GetRandomString(6),
                        CostPrice = rd.Next(200, 6000),
                        MiddleNorms = GetRandomString(10),
                        PackageNorms = GetRandomString(10),
                        MinimumNorms = GetRandomString(10),
                        SellingPrice = rd.Next(300, 10000),
                        WarrantyUnit = 1,
                        WarrantyPeriod = rd.Next(10),
                        TaxRate = rd.Next(10)
                    });
                }
            }

            Console.WriteLine($"{Environment.NewLine}----- 获取 Users 表中所有数据 ----{Environment.NewLine}");

            //获取 Users 表中所有数据（直接循环该表对应的对象即可，会在首次尝试读取数据时查询表中所有数据）
            foreach (var item in dataContext.Users)
            {
                Console.WriteLine($"用户名称：{item.Name}");
            }

            Console.WriteLine($"{Environment.NewLine}----- 查询创建 波力海苔 以及 人参果 两个商品的用户信息（条件 in） ----{Environment.NewLine}");

            //条件 in （查询创建 波力海苔 以及 人参果 两个商品的用户信息）
            var users = dataContext.Users.Where(user => dataContext.Commodities.Select(commodity => commodity.ID).Where(commodity => commodity.Name == "波力海苔" || commodity.Name == "人参果").Contains(user.ID));

            foreach (var item in users)
            {
                Console.WriteLine($"创建 波力海苔 或 人参果 的用户名称：{item.Name}");
            }

            Console.WriteLine($"{Environment.NewLine}----- 查询商品名称包含 硬壳 的商品信息（条件 like） ----{Environment.NewLine}");

            //条件 like （查询商品名称包含 硬壳 的商品信息）
            var commodities = dataContext.Commodities.Where(commodity => commodity.Name.Like("%硬壳%"));

            foreach (var item in commodities)
            {
                Console.WriteLine($"包含 硬壳 的商品名称：{item.Name}");
            }

            Console.WriteLine($"{Environment.NewLine}----- 将 东北大米 商品名称改成 泰国香米（普通修改） ----{Environment.NewLine}");

            //普通修改（将 东北大米 商品名称改成 泰国香米）
            int updateCount = dataContext.Commodities.Update(new Entities.Commodity()
            {
                Name = "泰国香米"
            }, commodity => commodity.Name == "东北大米");

            Console.WriteLine($"修改成功的商品数量：{updateCount}");

            Console.WriteLine($"{Environment.NewLine}----- 将 ID 值为 100 的用户删除（主键删除） ----{Environment.NewLine}");

            //主键删除（将 ID 值为 100 的用户删除）
            Console.WriteLine(dataContext.Users.Delete(100) ? "删除成功" : "删除失败");

            Console.WriteLine($"{Environment.NewLine}----- 查询所有商品名称以及创建该商品的用户名（内连接） ----{Environment.NewLine}");

            //内连接（查询所有商品名称以及创建该商品的用户名）
            var data = dataContext.Commodities.InnerJoin(dataContext.Users, (commodity, user) => new
            {
                UserName = user.Name,
                CommodityName = commodity.Name
            }, (commodity, user) => commodity.CreateUID == user.ID);

            foreach (var item in data)
            {
                Console.WriteLine($"商品 【{item.CommodityName}】 由 【{item.UserName}】 创建");
            }

            Console.WriteLine($"{Environment.NewLine}----- 查询所有商品名称以及创建该商品的用户主键 ID（左外连接） ----{Environment.NewLine}");

            //左外连接（查询所有商品名称以及创建该商品的用户主键 ID）
            var data1 = dataContext.Commodities.LeftJoin(dataContext.Users, (commodity, user) => new
            {
                UserID = user.ID.ToNull(),
                CommodityName = commodity.Name
            }, (commodity, user) => commodity.CreateUID == user.ID);

            foreach (var item in data1)
            {
                Console.WriteLine($"商品 【{item.CommodityName}】 由用户 ID 为 【{(item.UserID.HasValue ? item.UserID.Value : 0)}】 的用户创建");
            }

            Console.WriteLine($"{Environment.NewLine}-----  新增一条父级分类为化妆品的子分类（查询插入） ----{Environment.NewLine}");

            //查询插入（新增一条父级分类为化妆品的子分类）
            if (dataContext.Classifies.Where(classify => classify.Name == "口红").Count < 1)
            {
                int insertCount = dataContext.Classifies.Insert(dataContext.Classifies.Select(classify => new Entities.Classify()
                {
                    Name = "口红",
                    ParentID = classify.ID,
                    Code = GetRandomString(6),
                    CreateTime = DateTime.Now,
                    CreateUID = dataContext.Users.Select(user => user.ID).Ascending(user => DBFun.NewLong()).First()
                }).Where(classify => classify.Name == "化妆品"));

                Console.WriteLine(insertCount > 0 ? "口红分类插入成功" : "口红分类插入失败");
            }
            else
            {
                Console.WriteLine("口红分类已存在");
            }

            //Console.WriteLine($"{Environment.NewLine}-----  将张三创建的商品名称改成 张三的登录账号 + 原有商品名称（关联修改） ----{Environment.NewLine}");

            ////关联修改（将张三创建的商品名称改成 张三的登录账号 + 原有商品名称）
            //updateCount = dataContext.Commodities.InnerJoin(dataContext.Users, (commodity, user) => commodity.CreateUID == user.ID).Update(source => new Entities.Commodity()
            //{
            //    Name = source.Right.Account + source.Left.Name
            //}, source => source.Right.Name == "张三");

            //Console.WriteLine($"成功修改张三创建商品名称的数据条数：{updateCount}");

            //Console.WriteLine($"{Environment.NewLine}-----  将 李四 用户创建的商品删除（关联删除） ----{Environment.NewLine}");

            ////关联删除（将 李四 用户创建的商品删除）
            //int deleteCount = dataContext.Commodities.InnerJoin(dataContext.Users, (left, right) => left.CreateUID == right.ID).Delete(source => source.Right.Name == "李四");

            //Console.WriteLine($"成功删除李四户创建的商品数量：{deleteCount}");

            Console.WriteLine($"{Environment.NewLine}-----  查询商品名称、最大包装单位、中包包装单位、最小单位、分类名称、品牌名称（多表关联） ----{Environment.NewLine}");

            //多表关联（查询商品名称、最大包装单位、中包包装单位、最小单位、分类名称、品牌名称）
            var data2 = dataContext.Commodities
                .InnerJoin(dataContext.Classifies, (left, right) => left.ClassifyID == right.ID)
                .InnerJoin(dataContext.Units, (left, right) => left.Left.PackageUnitID == right.ID)
                .InnerJoin(dataContext.Units, (left, right) => left.Left.Left.MiddleUnitID == right.ID)
                .InnerJoin(dataContext.Units, (left, right) => left.Left.Left.Left.MinimumUnitID == right.ID)
                .LeftJoin(dataContext.Brands, (left, right) => left.Left.Left.Left.Left.BrandID == right.ID)
                .Select(source => new
                {
                    Name = source.Left.Left.Left.Left.Left.Name,
                    Classify = source.Left.Left.Left.Left.Right.Name,
                    PackageUnit = source.Left.Left.Left.Right.Name,
                    MiddleUnit = source.Left.Left.Right.Name,
                    MinimumUnit = source.Left.Right.Name,
                    Brand = source.Right.Name
                });

            foreach (var item in data2)
            {
                Console.WriteLine($"商品名称：{item.Name}，分类：{item.Classify}，最大包装单位：{item.PackageUnit}，中包包装单位：{item.MiddleUnit}，零售包装单位：{ item.MinimumUnit}，品牌：{ item.Brand}");
            }

            //数据合并 union all（查询 ID 大于 1 的用户名以及 ID 大于 1 的商品名）
            var data3 = dataContext.Users.Select(item => new
            {
                item.ID,
                item.Name,
                Type = "User"
            }).Where(item => item.ID > 1).UnionAll(dataContext.Commodities.Select(item => new
            {
                item.ID,
                item.Name,
                Type = "Commodity"
            }).Where(item => item.ID > 1));

            foreach (var item in data3)
            {
                Console.WriteLine($"类型：{(item.Type == "User" ? "用户：" : "商品：")}{item.Name} ID:{item.ID}");
            }
        }

        static string GetRandomString(int len)
        {
            string str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_@";
            StringBuilder result = new StringBuilder();

            for (int i = 0; i < len; i++)
            {
                result.Append(str[rd.Next(str.Length)]);
            }

            return result.ToString();
        }
    }
}
