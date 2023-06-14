using Microsoft.VisualStudio.TestTools.UnitTesting;
using TSDBConnector;

namespace TSDBConnectorTest
{
    [TestClass]
    public class BaseAPITests
    {
        private static Random random = new Random();
        TsdbCredentials credentials = new TsdbCredentials("127.0.0.1", 7777, "admin", "admin");

        // test case: base is exist
        [TestMethod]
        public async Task TestAddBase()
        {
            
            using var client = new TsdbClient(credentials);
            try
            {
                await client.Init();
                Assert.IsTrue(client.IsConnected);
                var baseName = TestHelper.RandomString(8);
                var baseT = new BaseT(baseName, "db/test", "10mb");
                await client.CreateBase(baseT);

                var baseEx = await client.GetBase(baseName);
                Assert.IsNotNull(baseEx);
                Assert.AreEqual(baseEx.Name, baseName);
                Assert.AreEqual(baseEx.Path, "db/test");
                Assert.AreEqual(baseEx.DbSize, "10mb");
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
        }

        [TestMethod]
        public async Task TestGetAllBases()
        {
            using var client = new TsdbClient(credentials);
            try
            {
                await client.Init();
                Assert.IsTrue(client.IsConnected);
                var count = 8;
                await TestHelper.RemoveAllBases(client);
                var genNames = await TestHelper.GenerateBases(client, count);
                var list = await client.GetBasesList();
                Assert.IsNotNull(list);
                Assert.AreEqual(list.Count, count);
                CollectionAssert.AreEquivalent(list.Select(x => x.Name).ToArray(), genNames.ToArray());
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
        public async Task TestGetBase()
        {
            using var client = new TsdbClient(credentials);
            try
            {
                await client.Init();
                Assert.IsTrue(client.IsConnected);

                var baseInst = await TestHelper.PrepareBase(client);

                Assert.IsNotNull(baseInst);
                var singleBase = await client.GetBase(baseInst.Name);
                Assert.IsNotNull(singleBase);
                Assert.AreEqual(baseInst.Name, singleBase.Name);
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

        // test case: base is opened
        [TestMethod]
        public async Task TestRemoveBase()
        {
            using var client = new TsdbClient(credentials);
            try
            {
                await client.Init();
                Assert.IsTrue(client.IsConnected);

               
                var baseToDelete = await TestHelper.PrepareBase(client);
                Assert.IsNotNull(baseToDelete);
                await client.RemoveBase(baseToDelete.Name);

                var removed = await client.GetBase(baseToDelete.Name);
                Assert.IsNull(removed);
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
        public async Task TestUpdateBase()
        {
            using var client = new TsdbClient(credentials);
            try
            {
                await client.Init();
                Assert.IsTrue(client.IsConnected);

                var baseToUpdate = await TestHelper.PrepareBase(client);
                Assert.IsNotNull(baseToUpdate);

                var newName = baseToUpdate.Name + "_upd";
                var updateInfo = new BaseT(newName, baseToUpdate.Path, baseToUpdate.DbSize, comment: "UPDATE SUCCESS");
                await client.UpdateBase(baseToUpdate.Name, updateInfo);

                var updated = await client.GetBase(updateInfo.Name);
                Assert.IsNotNull(updated);
                Assert.AreEqual(updated.Name, updateInfo.Name);
                Assert.AreEqual(updated.DbSize, updateInfo.DbSize);
                Assert.AreEqual(updated.FsType, updateInfo.FsType);
                Assert.AreEqual(updated.Comment, updateInfo.Comment);
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
            finally
            {
                await TestHelper.RemoveAllBases(client);
            }
        }
        
        [TestMethod]
        public async Task TestOpenBase()
        {
            var baseName = "";
            using var client = new TsdbClient(credentials);
            try
            {
                await client.Init();
                Assert.IsTrue(client.IsConnected);
                
                var baseEx = await TestHelper.PrepareBase(client);
                Assert.IsNotNull(baseEx);
                baseName = baseEx.Name;

                await client.OpenBase(baseName);
                var opened = await client.GetBase(baseName);
                Assert.IsNotNull(opened);
                Assert.AreNotEqual(opened.Status, 0);
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
            finally
            {
                await client.CloseBase(baseName);
                await TestHelper.RemoveAllBases(client);
            }
        }

        [TestMethod]
        public async Task TestCloseBase()
        {
            using var client = new TsdbClient(credentials);
            try
            {
                await client.Init();
                Assert.IsTrue(client.IsConnected);
                var baseEx = await TestHelper.PrepareBase(client);
                Assert.IsNotNull(baseEx);

                await client.OpenBase(baseEx.Name);
                var opened = await client.GetBase(baseEx.Name);
                Assert.IsNotNull(opened);
                Assert.AreNotEqual(opened.Status, 0);

                await client.CloseBase(baseEx.Name);
                var closed = await client.GetBase(baseEx.Name);
                Assert.IsNotNull(closed);
                Assert.AreEqual(closed.Status, 0);
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