using FlowBufferEnvironment;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TSDBConnector;

namespace TSDBConnectorTest
{
    [TestClass]
    public class DataIntegrationTests
    {
        string host = "127.0.0.1";
        int port = 7777;
        string login = "admin";
        string pass = "admin";

        // TODO: create [class initialize] method, mounting environment
        // and [clean up] on dispose    
        
        [TestMethod]
        public async Task TestAddRec()
        {
            var baseName = "TEST_Series";
            using var client = new TsdbClient();
            try
            {
                await client.CreateConnection(host, port, login, pass);
                Assert.IsTrue(client.isConnected);

                SeriesT? seriesEx = null;
                if (await client.GetBase(baseName) == null)
                {
                    var tuple = await IntegrationTests.PrepareSeries(client);
                    baseName = tuple.Item2;
                    seriesEx = tuple.Item1;
                }
                if (seriesEx == null)
                {
                    seriesEx = await client.GetSeries(baseName, "zero");
                }

                if (seriesEx == null) throw new ArgumentNullException();

                await client.OpenBase(baseName);

                var nanosec = System.DateTimeOffset.Now.ToUnixTimeMilliseconds() * 1000000;
                await client.DataAddRow(baseName, seriesEx.Id, seriesEx.Class, nanosec, 0, 16);

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
        public async Task TestGetLastValue()
        {
            var baseName = "TEST_Series";
            using var client = new TsdbClient();
            try
            {
                await client.CreateConnection(host, port, login, pass);
                Assert.IsTrue(client.isConnected);

                SeriesT? seriesEx = null;
                if (await client.GetBase(baseName) == null)
                {
                    var tuple = await IntegrationTests.PrepareSeries(client);
                    baseName = tuple.Item2;
                    seriesEx = tuple.Item1;
                }
                if (seriesEx == null)
                {
                    seriesEx = await client.GetSeries(baseName, "zero");
                }

                if (seriesEx == null) throw new ArgumentNullException();

                await client.OpenBase(baseName);

                var now = System.DateTimeOffset.Now.ToUnixTimeMilliseconds() * 1000000;
                var value = 123123;
                uint quality = 252;

                await client.DataAddRow(baseName, seriesEx.Id, seriesEx.Class, now, quality, value);

                var lastValue = await client.DataGetLastValue(baseName, seriesEx.Id, seriesEx.Class);

                if (lastValue == null) throw new ArgumentNullException();

                // TODO: testcase if base is not open
                // TODO: testcase if no data
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

        [TestMethod]
        public async Task TestGetValueAtTime()
        {
            var baseName = "TEST_Series";
            using var client = new TsdbClient();
            try
            {
                await client.CreateConnection(host, port, login, pass);
                Assert.IsTrue(client.isConnected);

                SeriesT? seriesEx = null;
                if (await client.GetBase(baseName) == null)
                {
                    var tuple = await IntegrationTests.PrepareSeries(client);
                    baseName = tuple.Item2;
                    seriesEx = tuple.Item1;
                }
                if (seriesEx == null)
                {
                    seriesEx = await client.GetSeries(baseName, "zero");
                }

                if (seriesEx == null) throw new ArgumentNullException();

                await client.OpenBase(baseName);

                var origin = System.DateTimeOffset.Now.ToUnixTimeMilliseconds() * 1000000;
                var reqTime = origin;
                var value = 0;
                uint quality = 0;

                for (int i = 0; i < 5; i ++)
                {
                    await client.DataAddRow(baseName, seriesEx.Id, seriesEx.Class, origin, quality, value);
                    origin += 100000000;
                    value += 100;
                    quality += 10;
                }

                // TODO: testcase if base is not open
                // TODO: testcase if no data
                // TODO: testcase if response time mark is unequal with requested, should be a nearest point
                var atTime = await client.DataGetValueAtTime(baseName, seriesEx.Id, seriesEx.Class, reqTime);

                if (atTime == null) throw new ArgumentNullException();

                // TODO: shorthand method for rec assert
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
            var baseName = "TEST_Series";
            using var client = new TsdbClient();
            try
            {
                await client.CreateConnection(host, port, login, pass);
                Assert.IsTrue(client.isConnected);

                SeriesT? seriesEx = null;
                if (await client.GetBase(baseName) == null)
                {
                    var tuple = await IntegrationTests.PrepareSeries(client);
                    baseName = tuple.Item2;
                    seriesEx = tuple.Item1;
                }
                if (seriesEx == null)
                {
                    seriesEx = await client.GetSeries(baseName, "zero");
                }

                if (seriesEx == null) throw new ArgumentNullException();

                await client.OpenBase(baseName);

                var now = System.DateTimeOffset.Now.ToUnixTimeMilliseconds() * 1000000;
                var value = 0;
                uint quality = 0;

                await client.DataAddRow(baseName, seriesEx.Id, seriesEx.Class, now, quality, value);

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
            var baseName = "TEST_Series";
            using var client = new TsdbClient();
            try
            {
                await client.CreateConnection(host, port, login, pass);
                Assert.IsTrue(client.isConnected);

                SeriesT? seriesEx = null;
                if (await client.GetBase(baseName) == null)
                {
                    var tuple = await IntegrationTests.PrepareSeries(client);
                    baseName = tuple.Item2;
                    seriesEx = tuple.Item1;
                }
                if (seriesEx == null)
                {
                    seriesEx = await client.GetSeries(baseName, "zero");
                }

                if (seriesEx == null) throw new ArgumentNullException();

                await client.OpenBase(baseName);

                // TODO: refactor duplicates
                var origin = System.DateTimeOffset.Now.ToUnixTimeMilliseconds() * 1000000;
                var reqTime = origin;
                var value = 0;
                uint quality = 0;

                for (int i = 0; i < 5; i ++)
                {
                    await client.DataAddRow(baseName, seriesEx.Id, seriesEx.Class, origin, quality, value);
                    origin += 100000000;
                    value += 100;
                    quality += 10;
                }
                // TODO: test over various cases of arguments

                // TODO: error: cannot be used with type any
                // it depens of 'convert' argument, from tcp it always nil
                 
                var cpRec = await client.DataGetRange(baseName, seriesEx.Id, 1, 100, reqTime, origin, 1000);

                Assert.IsNotNull(cpRec);
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
        public async Task TestGetFromCP()
        {
            var baseName = "TEST_Series";
            using var client = new TsdbClient();
            try
            {
                await client.CreateConnection(host, port, login, pass);
                Assert.IsTrue(client.isConnected);

                SeriesT? seriesEx = null;
                if (await client.GetBase(baseName) == null)
                {
                    var tuple = await IntegrationTests.PrepareSeries(client);
                    baseName = tuple.Item2;
                    seriesEx = tuple.Item1;
                }
                if (seriesEx == null)
                {
                    seriesEx = await client.GetSeries(baseName, "zero");
                }

                if (seriesEx == null) throw new ArgumentNullException();

                await client.OpenBase(baseName);

                var origin = System.DateTimeOffset.Now.ToUnixTimeMilliseconds() * 1000000;
                var reqTime = origin;
                var value = 0;
                uint quality = 0;

                for (int i = 0; i < 5; i ++)
                {
                    await client.DataAddRow(baseName, seriesEx.Id, seriesEx.Class, origin, quality, value);
                    origin += 100000000;
                    value += 100;
                    quality += 10;
                }
                var cp = await client.DataGetCP(baseName, seriesEx.Id, reqTime);
                Assert.IsNotNull(cp);
                Assert.AreNotEqual(cp, string.Empty);
                // TODO: testcase other direction
                var recsCp = await client.DataGetFromCP(baseName, cp, 1, 100);

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

        [TestMethod]
        public async Task TestRangeGetFromCP()
        {
            var baseName = "TEST_Series";
            using var client = new TsdbClient();
            try
            {
                await client.CreateConnection(host, port, login, pass);
                Assert.IsTrue(client.isConnected);

                SeriesT? seriesEx = null;
                if (await client.GetBase(baseName) == null)
                {
                    var tuple = await IntegrationTests.PrepareSeries(client);
                    baseName = tuple.Item2;
                    seriesEx = tuple.Item1;
                }
                if (seriesEx == null)
                {
                    seriesEx = await client.GetSeries(baseName, "zero");
                }

                if (seriesEx == null) throw new ArgumentNullException();

                await client.OpenBase(baseName);

                var origin = System.DateTimeOffset.Now.ToUnixTimeMilliseconds() * 1000000;
                var reqTime = origin;
                var value = 0;
                uint quality = 0;

                for (int i = 0; i < 5; i ++)
                {
                    await client.DataAddRow(baseName, seriesEx.Id, seriesEx.Class, origin, quality, value);
                    origin += 100000000;
                    value += 100;
                    quality += 10;
                }
                var cp = await client.DataGetCP(baseName, seriesEx.Id, reqTime);
                Assert.IsNotNull(cp);
                Assert.AreNotEqual(cp, string.Empty);
                // TODO: testcase other direction
                var recsCp = await client.DataGetRangeFromCP(baseName, cp, 1, 100, reqTime, origin, 1000);

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
            var baseName = "TEST_Series";
            using var client = new TsdbClient();
            try
            {
                await client.CreateConnection(host, port, login, pass);
                Assert.IsTrue(client.isConnected);

                SeriesT? seriesEx = null;
                if (await client.GetBase(baseName) == null)
                {
                    var tuple = await IntegrationTests.PrepareSeries(client);
                    baseName = tuple.Item2;
                    seriesEx = tuple.Item1;
                }
                if (seriesEx == null)
                {
                    seriesEx = await client.GetSeries(baseName, "zero");
                }

                if (seriesEx == null) throw new ArgumentNullException();

                await client.OpenBase(baseName);

                var now = System.DateTimeOffset.Now.ToUnixTimeMilliseconds() * 1000000;

                await client.DataAddRow(baseName, seriesEx.Id, seriesEx.Class, now - 10000000, 20, 20);

                await client.DataAddRow(baseName, seriesEx.Id, seriesEx.Class, now, 10, 10);
                var atTime = await client.DataGetValueAtTime(baseName, seriesEx.Id, seriesEx.Class, now);

                Assert.IsNotNull(atTime);
                Assert.AreEqual(atTime.T, now);
                CollectionAssert.AreEqual(atTime.Value,  ByteConverter.Int64ToBytes(10));
                CollectionAssert.AreEqual(atTime.Q,  ByteConverter.Int32ToBytes(10));

                await client.DataDeleteRow(baseName, seriesEx.Id, now);

                var afterDel = await client.DataGetValueAtTime(baseName, seriesEx.Id, seriesEx.Class, now);

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
            var baseName = "TEST_Series";
            using var client = new TsdbClient();
            try
            {
                await client.CreateConnection(host, port, login, pass);
                Assert.IsTrue(client.isConnected);

                SeriesT? seriesEx = null;
                if (await client.GetBase(baseName) == null)
                {
                    var tuple = await IntegrationTests.PrepareSeries(client);
                    baseName = tuple.Item2;
                    seriesEx = tuple.Item1;
                }
                if (seriesEx == null)
                {
                    seriesEx = await client.GetSeries(baseName, "zero");
                }

                if (seriesEx == null) throw new ArgumentNullException();

                await client.OpenBase(baseName);

                var now = System.DateTimeOffset.Now.ToUnixTimeMilliseconds() * 1000000;
                var reqTime = now;
                var value = 0;
                uint quality = 0;
                for (int i = 0; i < 5; i ++)
                {
                    await client.DataAddRow(baseName, seriesEx.Id, seriesEx.Class, now, quality, value);
                    now += 100000000;
                    value += 100;
                    quality += 10;
                }

                await client.DataDeleteRows(baseName, seriesEx.Id, reqTime, now);

                var recAtTime = await client.DataGetValueAtTime(baseName, seriesEx.Id, seriesEx.Class, now);
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
            var baseName = "TEST_Series";
            using var client = new TsdbClient();
            try
            {
                await client.CreateConnection(host, port, login, pass);
                Assert.IsTrue(client.isConnected);

                SeriesT? seriesEx = null;
                if (await client.GetBase(baseName) == null)
                {
                    var tuple = await IntegrationTests.PrepareSeries(client);
                    baseName = tuple.Item2;
                    seriesEx = tuple.Item1;
                }
                if (seriesEx == null)
                {
                    seriesEx = await client.GetSeries(baseName, "zero");
                }

                if (seriesEx == null) throw new ArgumentNullException();

                await client.OpenBase(baseName);

                var now = System.DateTimeOffset.Now.ToUnixTimeMilliseconds() * 1000000;
                var reqTime = now;
                var value = 0;
                uint quality = 0;
                for (int i = 0; i < 5; i ++)
                {
                    await client.DataAddRow(baseName, seriesEx.Id, seriesEx.Class, now, quality, value);
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
    }
}