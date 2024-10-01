using Microsoft.VisualStudio.TestTools.UnitTesting;
using TSDBConnector;

namespace TSDBConnectorTest
{
    [TestClass]
    public class PerfomanceTests
    {
        TsdbCredentials credentials = new TsdbCredentials("127.0.0.1", 7777, "admin", "admin");
    

        [TestMethod]
        public async Task TestAddManyBases()
        {
            
            using var client = new TsdbClient(credentials);
            try
            {
                await client.Init();
                Assert.IsTrue(client.IsConnected);
                await TestHelper.GenerateBases(client, 2000);
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
        }

        [TestMethod]
        public async Task TestAddManySeries()
        {
            
            using var client = new TsdbClient(credentials);
            try
            {
                await client.Init();
                Assert.IsTrue(client.IsConnected);
                var baseInst = await TestHelper.PrepareBase(client);
                Assert.IsNotNull(baseInst);
                await TestHelper.GenerateSeries(client, baseInst.Name, 10_000);
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
        }

        [TestMethod]
        public async Task RemoveAllBases()
        {
            using var client = new TsdbClient(credentials);
            try
            {
                await client.Init();
                Assert.IsTrue(client.IsConnected);
                await TestHelper.RemoveAllBases(client);
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
        }
    }

}