using FlowBufferEnvironment;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TSDBConnector;

namespace TSDBConnectorTest
{
    [TestClass]
    public class RowsAPITests
    {
        TsdbCredentials credentials = new TsdbCredentials("127.0.0.1", 7777, "admin", "admin");

        [TestMethod]
        public async Task TestAddRec()
        {
            var baseName = "";
            using var client = new TsdbClient(credentials);
            try
            {
                await client.Init();
                Assert.IsTrue(client.IsConnected);

                var seriesEx = await TestHelper.PrepareSeries(client);
                Assert.IsNotNull(seriesEx);
                
                baseName = seriesEx.ParentBaseName;
                await client.OpenBase(baseName);

                var nanosec = Now();
                await client.DataAddRow(baseName, seriesEx.Id, seriesEx.DataClass, nanosec, 0, 16);

            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
            finally
            {
                await client.CloseBase(baseName);
            }
        }

        // testcase if base is not open
        // testcase if no data
        [TestMethod]
        public async Task TestGetLastValue()
        {
            var baseName = "";
            using var client = new TsdbClient(credentials);
            try
            {
                await client.Init();
                Assert.IsTrue(client.IsConnected);

                var seriesEx = await TestHelper.PrepareSeries(client);
                Assert.IsNotNull(seriesEx);
                
                baseName = seriesEx.ParentBaseName;
                await client.OpenBase(baseName);

                var now = Now();
                var value = 123123;
                uint quality = 252;

                await client.DataAddRow(baseName, seriesEx.Id, seriesEx.DataClass, now, quality, value);

                var lastValue = await client.DataGetLastValue(baseName, seriesEx.Id, seriesEx.DataClass);

                if (lastValue == null) throw new ArgumentNullException();

                Assert.AreEqual(lastValue.T,  now);
                CollectionAssert.AreEqual(lastValue.Value,  ByteConverter.Int64ToBytes(value));
                CollectionAssert.AreEqual(lastValue.Q,  ByteConverter.Int32ToBytes((int)quality));

            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
            finally
            {
                await client.CloseBase(baseName);
            }
        }

        // testcase: if response time mark is unequal with requested, should be a nearest point
        [TestMethod]
        public async Task TestGetValueAtTime()
        {
            var baseName = "";
            using var client = new TsdbClient(credentials);
            try
            {
                await client.Init();
                Assert.IsTrue(client.IsConnected);

                var seriesEx = await TestHelper.PrepareSeries(client);
                Assert.IsNotNull(seriesEx);
                
                baseName = seriesEx.ParentBaseName;
                await client.OpenBase(baseName);

                var origin = Now();
                var reqTime = origin;
                var value = 0;
                uint quality = 0;

                for (int i = 0; i < 5; i ++)
                {
                    await client.DataAddRow(baseName, seriesEx.Id, seriesEx.DataClass, origin, quality, value);
                    origin += 100000000;
                    value += 100;
                    quality += 10;
                }

                var atTime = await client.DataGetValueAtTime(baseName, seriesEx.Id, seriesEx.DataClass, reqTime);

                if (atTime == null) throw new ArgumentNullException();

                Assert.AreEqual(atTime.T, reqTime);
                CollectionAssert.AreEqual(atTime.Value,  ByteConverter.Int64ToBytes(0));
                CollectionAssert.AreEqual(atTime.Q,  ByteConverter.Int32ToBytes(0));

            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
            finally
            {
                await client.CloseBase(baseName);
            }
        }


        [TestMethod]
        public async Task TestGetCP()
        {
            var baseName = "";
            using var client = new TsdbClient(credentials);
            try
            {
                await client.Init();
                Assert.IsTrue(client.IsConnected);

                var seriesEx = await TestHelper.PrepareSeries(client);
                Assert.IsNotNull(seriesEx);
                
                baseName = seriesEx.ParentBaseName;
                await client.OpenBase(baseName);

                var now = Now();
                var value = 0;
                uint quality = 0;

                await client.DataAddRow(baseName, seriesEx.Id, seriesEx.DataClass, now, quality, value);

                var cp = await client.DataGetCP(baseName, seriesEx.Id, now);

                Assert.IsNotNull(cp);
                Assert.AreNotEqual(cp, string.Empty);
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
            finally
            {
                await client.CloseBase(baseName);
            }
        }

        [TestMethod]
        public async Task TestGetRange()
        {
            var baseName = "";
            using var client = new TsdbClient(credentials);
            try
            {
                await client.Init();
                Assert.IsTrue(client.IsConnected);

                var seriesEx = await TestHelper.PrepareSeries(client);
                Assert.IsNotNull(seriesEx);
                
                baseName = seriesEx.ParentBaseName;
                await client.OpenBase(baseName);

                var origin = Now();
                var reqTime = origin;
                var value = 0;
                uint quality = 0;

                for (int i = 0; i < 5; i ++)
                {
                    await client.DataAddRow(baseName, seriesEx.Id, seriesEx.DataClass, origin, quality, value);
                    origin += 100000000;
                    value += 100;
                    quality += 10;
                }
                 
                var cpRec = await client.DataGetRange(baseName, seriesEx.Id, 0, SeekDirection.ToMax, 100, reqTime, origin, 0);
                Assert.IsNotNull(cpRec);
                var cpRecDpi = await client.DataGetRange(baseName, seriesEx.Id, 0, SeekDirection.ToMax, 100, reqTime, origin, 10);                
                Assert.IsNotNull(cpRecDpi);
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
            finally
            {
                await client.CloseBase(baseName);
            }
        }

        // testcase: other seek direction
        [TestMethod]
        public async Task TestGetFromCP()
        {
            var baseName = "";
            using var client = new TsdbClient(credentials);
            try
            {
                await client.Init();
                Assert.IsTrue(client.IsConnected);

                var seriesEx = await TestHelper.PrepareSeries(client);
                Assert.IsNotNull(seriesEx);
                
                baseName = seriesEx.ParentBaseName;
                await client.OpenBase(baseName);

                var origin = Now();
                var reqTime = origin;
                var value = 0;
                uint quality = 0;

                for (int i = 0; i < 5; i ++)
                {
                    await client.DataAddRow(baseName, seriesEx.Id, seriesEx.DataClass, origin, quality, value);
                    origin += 100000000;
                    value += 100;
                    quality += 10;
                }
                var cp = await client.DataGetCP(baseName, seriesEx.Id, reqTime);
                Assert.IsNotNull(cp);
                Assert.AreNotEqual(cp, string.Empty);
                
                var recsCp = await client.DataGetFromCP(baseName, 0, cp, SeekDirection.ToMax, 100);

                Assert.IsNotNull(recsCp);

                for (int i = recsCp.Recs.Length - 1; i > 0; i--)
                {
                    origin -= 100000000;
                    value -= 100;
                    quality -= 10;
                    Assert.AreEqual(recsCp.Recs[i].T, origin);
                    CollectionAssert.AreEqual(recsCp.Recs[i].Value,  ByteConverter.Int64ToBytes(value));
                    CollectionAssert.AreEqual(recsCp.Recs[i].Q,  ByteConverter.Int32ToBytes((int)quality));
                }
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
            finally
            {
                await client.CloseBase(baseName);
            }
        }

        // testcase: other direction
        [TestMethod]
        public async Task TestRangeGetFromCP()
        {
            var baseName = "";
            using var client = new TsdbClient(credentials);
            try
            {
                await client.Init();
                Assert.IsTrue(client.IsConnected);

                var seriesEx = await TestHelper.PrepareSeries(client);
                Assert.IsNotNull(seriesEx);
                
                baseName = seriesEx.ParentBaseName;
                await client.OpenBase(baseName);

                var origin = Now();
                var reqTime = origin;
                var value = 0;
                uint quality = 0;

                for (int i = 0; i < 5; i ++)
                {
                    await client.DataAddRow(baseName, seriesEx.Id, seriesEx.DataClass, origin, quality, value);
                    origin += 100000000;
                    value += 100;
                    quality += 10;
                }
                var cp = await client.DataGetCP(baseName, seriesEx.Id, reqTime);
                Assert.IsNotNull(cp);
                Assert.AreNotEqual(cp, string.Empty);

                var recsCp = await client.DataGetRangeFromCP(baseName, 0, cp, SeekDirection.ToMax, 100, reqTime, origin, 100);

                Assert.IsNotNull(recsCp);
                value = 0; quality = 0; origin = reqTime;
                for (int i = 0; i < recsCp.Recs.Length - 1; i++)
                {
                    Assert.AreEqual(recsCp.Recs[i].T, origin);
                    CollectionAssert.AreEqual(recsCp.Recs[i].Value,  ByteConverter.Int64ToBytes(value));
                    CollectionAssert.AreEqual(recsCp.Recs[i].Q,  ByteConverter.Int32ToBytes((int)quality));
                    origin += 100000000;
                    value += 100;
                    quality += 10;
                }
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
            finally
            {
                await client.CloseBase(baseName);
            }
        }

        [TestMethod]
        public async Task TestDeleteRow()
        {
            var baseName = "";
            using var client = new TsdbClient(credentials);
            try
            {
                await client.Init();
                Assert.IsTrue(client.IsConnected);

                var seriesEx = await TestHelper.PrepareSeries(client);
                Assert.IsNotNull(seriesEx);
                
                baseName = seriesEx.ParentBaseName;
                await client.OpenBase(baseName);

                var now = Now();

                await client.DataAddRow(baseName, seriesEx.Id, seriesEx.DataClass, now - 10000000, 20, 20);

                await client.DataAddRow(baseName, seriesEx.Id, seriesEx.DataClass, now, 10, 10);
                var atTime = await client.DataGetValueAtTime(baseName, seriesEx.Id, seriesEx.DataClass, now);

                Assert.IsNotNull(atTime);
                Assert.AreEqual(atTime.T, now);
                CollectionAssert.AreEqual(atTime.Value,  ByteConverter.Int64ToBytes(10));
                CollectionAssert.AreEqual(atTime.Q,  ByteConverter.Int32ToBytes(10));

                await client.DataDeleteRow(baseName, seriesEx.Id, now);

                var afterDel = await client.DataGetValueAtTime(baseName, seriesEx.Id, seriesEx.DataClass, now);

                Assert.IsNotNull(afterDel);
                Assert.AreEqual(afterDel.T, now - 10000000);
                CollectionAssert.AreEqual(afterDel.Value,  ByteConverter.Int64ToBytes(20));
                CollectionAssert.AreEqual(afterDel.Q,  ByteConverter.Int32ToBytes(20));               
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
            finally
            {
                await client.CloseBase(baseName);
            }
        }


        [TestMethod]
        public async Task TestDeleteRows()
        {
            var baseName = "";
            using var client = new TsdbClient(credentials);
            try
            {
                await client.Init();
                Assert.IsTrue(client.IsConnected);

                var seriesEx = await TestHelper.PrepareSeries(client);
                Assert.IsNotNull(seriesEx);
                
                baseName = seriesEx.ParentBaseName;
                await client.OpenBase(baseName);

                var now = Now();
                var reqTime = now;
                var value = 0;
                uint quality = 0;
                for (int i = 0; i < 5; i ++)
                {
                    await client.DataAddRow(baseName, seriesEx.Id, seriesEx.DataClass, now, quality, value);
                    now += 100000000;
                    value += 100;
                    quality += 10;
                }

                await client.DataDeleteRows(baseName, seriesEx.Id, reqTime, now);

                var recAtTime = await client.DataGetValueAtTime(baseName, seriesEx.Id, seriesEx.DataClass, now);
                if (recAtTime != null)
                {
                    Assert.AreNotEqual(recAtTime.T, now);
                }
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
            finally
            {
                await client.CloseBase(baseName);
            }
        }

        [TestMethod]
        public async Task TestGetBoundary()
        {
            var baseName = "";
            using var client = new TsdbClient(credentials);
            try
            {
                await client.Init();
                Assert.IsTrue(client.IsConnected);

                var seriesEx = await TestHelper.PrepareSeries(client);
                Assert.IsNotNull(seriesEx);
                
                baseName = seriesEx.ParentBaseName;
                await client.OpenBase(baseName);

                var now = Now();
                var reqTime = now;
                var value = 0;
                uint quality = 0;
                for (int i = 0; i < 5; i ++)
                {
                    await client.DataAddRow(baseName, seriesEx.Id, seriesEx.DataClass, now, quality, value);
                    now += 100000000;
                    value += 100;
                    quality += 10;
                }
                var bounds = await client.DataGetBoundary(baseName, seriesEx.Id);
                Assert.IsNotNull(bounds);
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
            finally
            {
                await client.CloseBase(baseName);
            }
        }

        [TestMethod]
        public async Task TestAddRows()
        {
            var baseName = "";
            using var client = new TsdbClient(credentials);
            try
            {
                await client.Init();
                Assert.IsTrue(client.IsConnected);

                var seriesEx = await TestHelper.PrepareSeries(client);
                Assert.IsNotNull(seriesEx);
                
                baseName = seriesEx.ParentBaseName;
                await client.OpenBase(baseName);
                
                var boundsBefore = await client.DataGetBoundary(baseName, seriesEx.Id);

                var count = 10;
                var cache = CreateRowsCache(seriesEx.Id, seriesEx.DataClass, count);
               
                await client.DataAddRows(baseName, cache);

                var boundsAfter = await client.DataGetBoundary(baseName, seriesEx.Id);
                Assert.IsNotNull(boundsAfter);
                Assert.AreEqual(boundsBefore?.RowCount ?? 0, boundsAfter.RowCount - count);
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
            finally
            {
                await client.CloseBase(baseName);
            }
        }

        [TestMethod]
        public async Task TestAddRowsCache()
        {
            var baseName = "";
            using var client = new TsdbClient(credentials);
            try
            {
                await client.Init();
                Assert.IsTrue(client.IsConnected);

                var seriesEx = await TestHelper.PrepareSeries(client);
                Assert.IsNotNull(seriesEx);
                
                baseName = seriesEx.ParentBaseName;
                await client.OpenBase(baseName);

                var boundsBefore = await client.DataGetBoundary(baseName, seriesEx.Id);

                var count = 10;

                var cache = CreateRowsCache(seriesEx.Id, seriesEx.DataClass, count);
               
                var cacheCount = await client.DataAddRowsCache(baseName, cache);

                var boundsAfter = await client.DataGetBoundary(baseName, seriesEx.Id);
                Assert.IsNotNull(boundsAfter);
                Assert.AreEqual((boundsBefore?.RowCount ?? 0) + count, boundsAfter.RowCount);
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
            finally
            {
                await client.CloseBase(baseName);
            }
        }

        private RowsCacheT CreateRowsCache(long seriesId, DataClass dataClass, int count)
        {
            var cache = new RowsCacheT();
            var reqTime = Now();
            var now = reqTime;
            var value = 0;
            uint quality = 0;
            for (int i = 0; i < count; i++)
            {
                cache.AddRec(seriesId, dataClass, reqTime, quality, value);
                reqTime += 100000000;
                value += 100;
                quality += 10;
            }
            return cache;
        }


        //[TestMethod]
        public async Task TestGeneration()
        {
            var baseName = "";
            using var client = new TsdbClient(credentials);
            try
            {
                await client.Init();
                Assert.IsTrue(client.IsConnected);

                var seriesEx = await TestHelper.PrepareSeries(client);
                Assert.IsNotNull(seriesEx);
                
                baseName = seriesEx.ParentBaseName;
                await client.OpenBase(baseName);

                var value = 0;
                uint quality = 42;

                for (int i = 0; i < 1000; i ++)
                {
                    if (value > 100) value = 0;
                    var now = Now();
                    await client.DataAddRow(baseName, seriesEx.Id, seriesEx.DataClass, now, quality, value);
                    value++;

                    await Task.Delay(100);
                }
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
            finally
            {
                await client.CloseBase(baseName);
            }
        }

        private long Now()
        {
            return System.DateTimeOffset.Now.ToUnixTimeMilliseconds() * 1000000;
        }
    }
}