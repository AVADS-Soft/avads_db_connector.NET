using Microsoft.VisualStudio.TestTools.UnitTesting;
using TSDBConnector;

namespace TSDBConnectorTest
{
    [TestClass]
    public class ProtocolTests
    {
        [TestMethod]
        public async Task TestCreateConnection()
        {
            using var client = new TsdbClient(TestHelper.Credentials);
            try
            {
                await client.Init();
                Assert.IsTrue(client.IsConnected);
                Assert.IsFalse(String.IsNullOrEmpty(client.SessionKey));
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
        }
        
        [TestMethod]
        public async Task TestRestoreSession()
        {
            using var client = new TsdbClient(TestHelper.Credentials);
            try
            {
                await client.Init();
                var startSession = client.SessionKey;
                Assert.IsTrue(client.IsConnected);
                Assert.IsFalse(String.IsNullOrEmpty(client.SessionKey));
                
                var doTest = 0;
                var flag = true;
                while (flag)
                {
                    await client.GetBasesList();
                    doTest++;

                    if (doTest == 2)
                    {
                        client.CloseConnection();
                    }

                    if (doTest == 3)
                    {
                        flag = false;
                        Assert.AreEqual(startSession, client.SessionKey);
                        // assert opened bases list
                    }
                }
            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
        }
    }
}
