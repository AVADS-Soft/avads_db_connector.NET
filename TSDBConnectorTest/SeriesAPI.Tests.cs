using Microsoft.VisualStudio.TestTools.UnitTesting;
using TSDBConnector;

namespace TSDBConnectorTest
{
    [TestClass]
    public class SeriesAPITests
    {
        
        [TestMethod]
        public async Task TestGetSeriesList()
        {
            using var client = new TsdbClient(TestHelper.Credentials);
            try
            {
                await client.Init();
                Assert.IsTrue(client.IsConnected);
                var baseEx = await TestHelper.PrepareBase(client);
                Assert.IsNotNull(baseEx);
                var namesList = await TestHelper.GenerateSeries(client, baseEx.Name, 8);
                
                var serList = await client.GetSeriesList(baseEx.Name);
                Assert.IsNotNull(serList);

                Assert.AreEqual(serList.Count, 8);
                CollectionAssert.AreEquivalent(serList.Select(x => x.Name).ToArray(), namesList.ToArray());
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
            finally
            {
                await TestHelper.RemoveAllBases(client);
            }
        }

        [TestMethod]
        public async Task TestGetSeries()
        {
            using var client = new TsdbClient(TestHelper.Credentials);
            try
            {
                await client.Init();
                Assert.IsTrue(client.IsConnected);

                var seriesEx = await TestHelper.PrepareSeries(client);
                Assert.IsNotNull(seriesEx);

                var idSer = await client.GetSeriesById(seriesEx.ParentBaseName, seriesEx.Id);
                Assert.IsNotNull(idSer);
                var nameSer = await client.GetSeries(seriesEx.ParentBaseName, seriesEx.Name);
                Assert.IsNotNull(nameSer);

                Assert.AreEqual(idSer.Name, nameSer.Name);
                Assert.AreEqual(idSer.Id, nameSer.Id);
                Assert.AreEqual(idSer.ParentBaseName, nameSer.ParentBaseName);
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
            finally
            {
                await TestHelper.RemoveAllBases(client);
            }
        }

        [TestMethod]
        public async Task TestAddSeries()
        {
            using var client = new TsdbClient(TestHelper.Credentials);
            try
            {
                await client.Init();
                Assert.IsTrue(client.IsConnected);
                var baseEx = await TestHelper.PrepareBase(client);
                Assert.IsNotNull(baseEx);
                var serNames = await TestHelper.GenerateSeries(client, baseEx.Name, 8);
                for (int i = 0; i < 8; i++)
                {
                    var serName = serNames[i];
                    var series = await client.GetSeries(baseEx.Name, serName);
                    Assert.IsNotNull(series);
                }
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
            finally
            {
                await TestHelper.RemoveAllBases(client);
            }
        }

        [TestMethod]
        public async Task TestRemoveSeries()
        {
            using var client = new TsdbClient(TestHelper.Credentials);
            try
            {
                await client.Init();
                Assert.IsTrue(client.IsConnected);

                var baseEx = await TestHelper.PrepareBase(client);
                Assert.IsNotNull(baseEx);
                await TestHelper.GenerateSeries(client, baseEx.Name, 8);
                var serList = await client.GetSeriesList(baseEx.Name);
                for (int i = 0; i < 8; i++)
                {
                    var serId = serList[i].Id;
                    await client.RemoveSeries(baseEx.Name, serId);
                    var del = await client.GetSeriesById(baseEx.Name, serId);
                    Assert.IsNull(del);
                }                
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
            finally
            {
                await TestHelper.RemoveAllBases(client);
            }
        }

        [TestMethod]
        public async Task TestUpdateSeries()
        {
            using var client = new TsdbClient(TestHelper.Credentials);
            try
            {
                await client.Init();
                Assert.IsTrue(client.IsConnected);

                var seriesEx = await TestHelper.PrepareSeries(client);
                Assert.IsNotNull(seriesEx);

                var seriesUpdate = new SeriesT("UPDATED", seriesEx.Id, 27, "UPDATE_SUCCS");

                await client.UpdateSeries(seriesEx.ParentBaseName, seriesUpdate);
                var upd = await client.GetSeriesById(seriesEx.ParentBaseName, seriesEx.Id);
                Assert.IsNotNull(upd);

                Assert.AreEqual(seriesUpdate.Name, upd.Name);
                Assert.AreEqual(seriesUpdate.Id, upd.Id);
                Assert.AreEqual(seriesUpdate.Type, upd.Type);
                Assert.AreEqual(seriesUpdate.Comment, upd.Comment);
                
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
            finally
            {
                await TestHelper.RemoveAllBases(client);
            }
        }

    }
}