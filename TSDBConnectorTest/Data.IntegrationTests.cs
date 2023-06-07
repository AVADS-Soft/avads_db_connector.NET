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

        private async Task PrepareSeries()
        {
            
        }


        [TestMethod]
        public async Task Test()
        {
            using var client = new TsdbClient();
            try
            {
                await client.CreateConnection(host, port, login, pass);
                Assert.IsTrue(client.isConnected);


            }
            catch (Exception e)
            {
                throw new AssertFailedException(e.Message, e);
            }
        }


    }
}