using TSDBConnector;

namespace TSDBConnectorTest
{
    public static class TestHelper
    {
        private static Random random = new Random();
        public static TsdbCredentials Credentials = new TsdbCredentials("127.0.0.1", 7777, "admin", "admin");
        public static async Task<BaseT?> PrepareBase(TsdbClient client, string baseName = "")
        {
            if (String.IsNullOrEmpty(baseName))
            {
                baseName = RandomString(8);
            }
            var baseEx = await client.GetBase(baseName);
            if (baseEx == null)
            {
                var newBase = new BaseT(baseName, "db/test", "10mb");
                await client.CreateBase(newBase);
                baseEx = await client.GetBase(baseName);
            }
            return baseEx;
        }

        public static async Task<SeriesT?> PrepareSeries(TsdbClient client, string baseName = "", string seriesName = "")
        {
            if (String.IsNullOrEmpty(baseName))
            {
                var baseEx = await PrepareBase(client, baseName);
                baseName = baseEx?.Name ?? "";
            }
            
            if  (String.IsNullOrEmpty(seriesName))
            {
                seriesName = RandomString(8);
            }
            var seriesEx = await client.GetSeries(baseName, seriesName);
            if (seriesEx == null)
            {
                var newSeries = new SeriesT(seriesName, 0, (long)MecTypes.LINT, baseName);
                await client.CreateSeries(baseName, newSeries);
                seriesEx = await client.GetSeries(baseName, seriesName);
            }
            return seriesEx;
        }

        public static async Task<List<string>> GenerateBases(TsdbClient client, int count)
        {
            var list = new List<string>();
            for (int i = 0; i < count; i++)
            {
                var baseName = RandomString(8);
                var baseT = new BaseT(baseName, "./db", "15mb");
                try
                {
                    await client.CreateBase(baseT);
                    list.Add(baseName);
                }
                catch { continue; }
            }
            return list;
        }

        public static async Task<List<string>> GenerateSeries(TsdbClient client, string baseName, int count)
        {
            await client.OpenBase(baseName);
            var list = new List<string>();
            for (int i = 0; i < count; i++)
            {
                var seriesName = RandomString(8);
                var seriesT = new SeriesT(seriesName, i, (byte)MecTypes.ULINT, baseName);
                try
                {
                    await client.CreateSeries(baseName, seriesT);
                    list.Add(seriesName);
                }
                catch { continue; }
                finally {
                    await client.CloseBase(baseName);
                }
            }
            return list;
        }

        public static async Task RemoveAllBases(TsdbClient client)
        {
            var list = await client.GetBasesList();
            for (int i = 0; i < list.Count; i ++)
            {
                var name = list[i].Name;
                await client.RemoveBase(name);
            }
        }

        public static async Task RemoveAllSeries(TsdbClient client, string baseName)
        {
            var list = await client.GetSeriesList(baseName);
            for (int i = 0; i < list.Count; i ++)
            {
                var id = list[i].Id;
                await client.RemoveSeries(baseName, id);
            }
        }

        public static string RandomString(int length)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
                .Select(s => s[random.Next(s.Length)]).ToArray());
        }
    }
}