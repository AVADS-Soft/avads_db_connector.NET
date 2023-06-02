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
                var baseT = new BaseT("first base2", "./db/test", "10mb");

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
        public async Task TestUpdateBase()
        {
            using var client = new TsdbClient();
            try
            {
                await client.CreateConnection(host, port, login, pass);
                Assert.IsTrue(client.isConnected);

                var baseToUpdate = new BaseT("first base", "./db/test", "10mb");
                var baseEx = await client.GetBase(baseToUpdate.Name);
                if (baseEx == null)
                {
                    await client.CreateBase(baseToUpdate);
                }
                var newName = baseToUpdate.Name + "_upd";
                var updateInfo = new BaseT(newName, baseToUpdate.Path, baseToUpdate.DbSize, comment: "UPDATE SUCCESS");
                
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


    }
}

// TODO: return instance of api on CreateConnection?
// TODO: improve tests for prevent locks (del all bases before/after each)