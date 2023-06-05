using System;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TSDBConnector;

namespace TSDBConnectorTest
{
    [TestClass]
    public class IntegrationTests
    {
        string host = "127.0.0.1";
        int port = 7777;
        string login = "admin";
        string pass = "admin";

        [TestMethod]
        public async Task TestCreateConnection()
        {
            using var client = new TsdbClient();
            try
            {
                await client.CreateConnection(host, port, login, pass);
                Assert.IsTrue(client.isConnected);
                Assert.IsFalse(String.IsNullOrEmpty(client.sessionKey));
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
        }

        [TestMethod]
        public async Task TestAddBase()
        {
            using var client = new TsdbClient();
            try
            {
                await client.CreateConnection(host, port, login, pass);
                // , "По русски",, 0, FsTypes.FS_MEMORY, , new LoopingT(1, "2m"), false, false, "", ""
                var baseT = new BaseT("first base111", "./db/test", "10mb");

                await client.CreateBase(baseT);
                Assert.IsTrue(client.isConnected);
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
        }

        [TestMethod]
        public async Task TestGetAllBases()
        {
            using var client = new TsdbClient();
            try
            {
                await client.CreateConnection(host, port, login, pass);
                Assert.IsTrue(client.isConnected);
                var list = await client.GetBasesList();
                Assert.IsNotNull(list);
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
        }

        [TestMethod]
        public async Task TestGetBase()
        {
            using var client = new TsdbClient();
            try
            {
                await client.CreateConnection(host, port, login, pass);
                Assert.IsTrue(client.isConnected);
                var list = await client.GetBasesList();
                var random = new Random();
                int index = random.Next(list.Count);
                var baseInst = list[index];

                var singleBase = await client.GetBase(baseInst.Name);
                Assert.IsNotNull(baseInst);

                Assert.AreEqual(baseInst.Name, singleBase?.Name);
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
        }

        [TestMethod]
        public async Task TestRemoveBase()
        {
            using var client = new TsdbClient();
            try
            {
                await client.CreateConnection(host, port, login, pass);
                Assert.IsTrue(client.isConnected);
                var list = await client.GetBasesList();
                var random = new Random();
                int index = random.Next(list.Count);
                var baseToDelete = list[index];

                await client.RemoveBase(baseToDelete.Name);

                var removed = await client.GetBase(baseToDelete.Name);
                Assert.IsNull(removed);
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
        }

        [TestMethod]
        public async Task TestRemoveAllBases()
        {
            using var client = new TsdbClient();
            try
            {
                await client.CreateConnection(host, port, login, pass);
                Assert.IsTrue(client.isConnected);
                var list = await client.GetBasesList();

                for (int i = 0; i < list.Count; i ++)
                {
                    var name = list[i].Name;
                    await client.RemoveBase(name);

                }

                var emptyList = await client.GetBasesList();
                Assert.AreEqual(emptyList.Count, 0);
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
        }

        [TestMethod]
        public async Task TestUpdateBase()
        {
            using var client = new TsdbClient();
            try
            {
                await client.CreateConnection(host, port, login, pass);
                Assert.IsTrue(client.isConnected);

                var baseToUpdate = new BaseT("test update base", "./db/test", "10mb");
                var baseEx = await client.GetBase(baseToUpdate.Name);
                if (baseEx == null)
                {
                    await client.CreateBase(baseToUpdate);
                    baseEx = await client.GetBase(baseToUpdate.Name);
                }
                var newName = baseToUpdate.Name + "_upd";
                var updateInfo = new BaseT(newName, baseEx!.Path, baseToUpdate.DbSize, comment: "UPDATE SUCCESS");
                
                // @fixme: base is corrupted after update (open via web client)
                await client.UpdateBase(baseToUpdate.Name, updateInfo);

                var updated = await client.GetBase(updateInfo.Name);
                Assert.IsNotNull(updated);
                // was update
                Assert.AreEqual(updated.Name, updateInfo.Name);
                Assert.AreEqual(updated.DbSize, updateInfo.DbSize);
                Assert.AreEqual(updated.FsType, updateInfo.FsType);
                Assert.AreEqual(updated.Comment, updateInfo.Comment);
                // was not updated
                Assert.AreEqual(updated.Path, updateInfo.Path);
                Assert.AreEqual(updated.AutoSave, updateInfo.AutoSave);
                Assert.AreEqual(updated.AutoSaveDuration, updateInfo.AutoSaveDuration);
                Assert.AreEqual(updated.AutoSaveInterval, updateInfo.AutoSaveInterval);

                await client.RemoveBase(updated.Name);
                var del = await client.GetBase(updated.Name);
                Assert.IsNull(del);
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
        }

        [TestMethod]
        public async Task TestOpenBase()
        {
            using var client = new TsdbClient();
            try
            {
                await client.CreateConnection(host, port, login, pass);
                Assert.IsTrue(client.isConnected);
                var baseT = new BaseT("first base", "./db/test", "10mb");
                var baseEx = await client.GetBase(baseT.Name);
                if (baseEx == null)
                {
                    await client.CreateBase(baseT);
                }

                await client.OpenBase(baseT.Name);
                var opened = await client.GetBase(baseT.Name);
                Assert.IsNotNull(opened);
                // TODO: check status value
                //Assert.AreEqual(opened.Status, 3);
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
        }

        [TestMethod]
        public async Task TestCloseBase()
        {
            using var client = new TsdbClient();
            try
            {
                await client.CreateConnection(host, port, login, pass);
                Assert.IsTrue(client.isConnected);
                // TODO: generate unique test baseName
                var baseT = new BaseT("first base", "./db/test", "10mb");
                var baseEx = await client.GetBase(baseT.Name);
                if (baseEx == null)
                {
                    await client.CreateBase(baseT);
                }

                await client.OpenBase(baseT.Name);

                await client.CloseBase(baseT.Name);
                var opened = await client.GetBase(baseT.Name);
                Assert.IsNotNull(opened);

                Assert.AreEqual(opened.Status, 0);
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
        }

        private async Task<BaseT?> PrepareBase(TsdbClient client)
        {
            var baseName = "TEST_Series";
            
            var baseEx = await client.GetBase(baseName);
            if (baseEx != null)
            {
                await client.RemoveBase(baseName);
                baseEx = await client.GetBase(baseName);
            }
            if (baseEx == null)
            {
                var baseForSeries = new BaseT(baseName, "./db", "10mb");
                await client.CreateBase(baseForSeries);
                baseEx = await client.GetBase(baseName);
            }
            return baseEx;
        }


        private async Task<Tuple<SeriesT?, string>> PrepareSeries(TsdbClient client)
        {
            var baseEx = await PrepareBase(client);

            if (baseEx != null)
            {               

                var seriesName = "zero";
                var series = await client.GetSeries(baseEx.Name, seriesName);
                if (series == null)
                {
                    var newSeries = new SeriesT(seriesName, 12, 1);
                    await client.AddSeries(baseEx.Name, newSeries);
                    series = await client.GetSeries(baseEx.Name, seriesName);
                }
                return new Tuple<SeriesT?, string>(series, baseEx.Name);
            }
            return new Tuple<SeriesT?, string>(null, "");;
        }
        


        [TestMethod]
        public async Task TestGetSeriesList()
        {
            using var client = new TsdbClient();
            try
            {
                await client.CreateConnection(host, port, login, pass);
                Assert.IsTrue(client.isConnected);
                // TODO: fix tests
                var baseEx = await PrepareBase(client);
                Assert.IsNotNull(baseEx);

                var serList = await client.GetSeriesList(baseEx.Name);

                Assert.IsNotNull(serList);
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
        }

        [TestMethod]
        public async Task TestGetSeries()
        {
            using var client = new TsdbClient();
            try
            {
                await client.CreateConnection(host, port, login, pass);
                Assert.IsTrue(client.isConnected);

                // TODO: fix tests
                var seriesT = await PrepareSeries(client);

                Assert.IsNotNull(seriesT);
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
        }

        [TestMethod]
        public async Task TestAddSeries()
        {
            using var client = new TsdbClient();
            try
            {
                await client.CreateConnection(host, port, login, pass);
                Assert.IsTrue(client.isConnected);
                // TODO: fix tests
                var baseEx = await PrepareBase(client);
                Assert.IsNotNull(baseEx);

                var newSeries = new SeriesT("zero", 12, 1);
                await client.AddSeries(baseEx.Name, newSeries);
                var series = await client.GetSeries(baseEx.Name, newSeries.Name);
                Assert.IsNotNull(series);
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
        }

        [TestMethod]
        public async Task TestRemoveSeries()
        {
            using var client = new TsdbClient();
            try
            {
                await client.CreateConnection(host, port, login, pass);
                Assert.IsTrue(client.isConnected);

                // TODO: add baseName to SeriesT?
                // del tuples, refactor tests
                var tuple = await PrepareSeries(client);
                var seriesEx = tuple.Item1;
                var baseName = tuple.Item2;
                Assert.IsNotNull(seriesEx);

                await client.RemoveSeries(baseName, seriesEx.Id);

                var series = await client.GetSeries(baseName, seriesEx.Name);
                Assert.IsNull(series);
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
        }

        [TestMethod]
        public async Task TestUpdateSeries()
        {
            using var client = new TsdbClient();
            try
            {
                await client.CreateConnection(host, port, login, pass);
                Assert.IsTrue(client.isConnected);

                var tuple = await PrepareSeries(client);
                var seriesEx = tuple.Item1;
                var baseName = tuple.Item2;
                Assert.IsNotNull(seriesEx);

                var seriesUpdate = new SeriesT("UPDATED", seriesEx.Id, 27, "UPDATE_SUCCS");

                await client.UpdateSeries(baseName, seriesUpdate);

                var upd = await client.GetSeriesById(baseName, seriesEx.Id);
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
        }


    }
}

// TODO: return instance of api on CreateConnection?
// TODO: improve tests for prevent locks (del all bases before/after each)
// TODO: web client broadcast events bug 