using System;
using TSDBConnector;

namespace TSDBConnectorTest;

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
        using(var client = new TsdbClient())
        {
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
    }

    [TestMethod]
    public async Task TestAddBase()
    {
        using(var client = new TsdbClient())
        {
            try
            {
                await client.CreateConnection(host, port, login, pass);
                // , "По русски",, 0, FsTypes.FS_MEMORY, , new LoopingT(1, "2m"), false, false, "", ""
                var baseT = new BaseT("first base2", "./db/test", "10mb");

                await client.CreateBase(baseT);
                Assert.IsTrue(client.isConnected);
            }
            catch(Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
        }
    }

    [TestMethod]
    public async Task TestGetAllBases()
    {
        using(var client = new TsdbClient())
        {
            try
            {
                await client.CreateConnection(host, port, login, pass);
                Assert.IsTrue(client.isConnected);
                var list = await client.GetBasesList();
                Assert.IsNotNull(list);
            }
            catch(Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
        }
    }

    [TestMethod]
    public async Task TestGetBase()
    {
        using(var client = new TsdbClient())
        {
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
            catch(Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
        }
    }

    [TestMethod]
    public async Task TestRemoveBase()
    {
        using(var client = new TsdbClient())
        {
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
            catch(Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
        }
    }

    [TestMethod]
    public async Task TestUpdateBase()
    {
        using(var client = new TsdbClient())
        {
            try
            {
                await client.CreateConnection(host, port, login, pass);
                Assert.IsTrue(client.isConnected);
                var list = await client.GetBasesList();
                var random = new Random();
                int index = random.Next(list.Count);
                var baseToUpdate = list[index];
                var updateInfo = new BaseT(baseToUpdate.Name + "_upd", "upd", "10mb", FsTypes.FS_FS, "UPDATE SUCCESS");
                await client.UpdateBase(baseToUpdate.Name, updateInfo);

                var updated = await client.GetBase(updateInfo.Name);
                Assert.IsNotNull(updated);
                // was update
                Assert.AreEqual(updated.Name, updateInfo.Name);
                Assert.AreEqual(updated.Path, updateInfo.Path);
                Assert.AreEqual(updated.DbSize, updateInfo.DbSize);
                Assert.AreEqual(updated.FsType, updateInfo.FsType);
                Assert.AreEqual(updated.Comment, updateInfo.Comment);
                // was not updated
                Assert.AreEqual(baseToUpdate.AutoSave, updateInfo.AutoSave);
                Assert.AreEqual(updated.AutoSaveDuration, updateInfo.AutoSaveDuration);
                Assert.AreEqual(updated.AutoSaveInterval, updateInfo.AutoSaveInterval);
            }
            catch(Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
        }
    }

    
}