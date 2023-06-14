using FlowBufferEnvironment;

namespace TSDBConnector
{
    public static class TsdbSeriesExtension
    {
        public static async Task<List<SeriesT>> GetSeriesList(this TsdbClient client, string baseName)
        {
            var reqPack = 
                new FlowBuffer((byte)CmdType.SeriesGetAll)
                .AddString(baseName)
                .GetPayloadPack();

            var readBuffer = await client.Fetch(reqPack);
            var count = readBuffer.GetInt64(); 
            var result = new List<SeriesT>();
            for (int i = 0; i < count; i++)
            {
                var serT = ExtractSeries(ref readBuffer, baseName);
                result.Add(serT);
            }
            return result;
        }

        public static async Task<SeriesT?> GetSeries(this TsdbClient client, string baseName, string seriesName)
        {
            try
            {
                var reqPack = 
                    new FlowBuffer((byte)CmdType.SeriesGetInfo)
                    .AddString(baseName)
                    .AddString(seriesName)
                    .GetPayloadPack();

                var buffer = await client.Fetch(reqPack);
                var seriesT = ExtractSeries(ref buffer, baseName);
                return seriesT;
            }
            catch (Exception e)
            {
                if (e.Message.StartsWith("#16001") || e.Message.StartsWith("#16002") || e.Message.StartsWith("#16006"))
                {
                    return null;
                }
                else throw;
            }
        }

        public static async Task<SeriesT?> GetSeriesById(this TsdbClient client, string baseName, long seriesId)
        {
            try
            {
                var reqPack = 
                    new FlowBuffer((byte)CmdType.GetSeriesById)
                    .AddString(baseName)
                    .AddInt64(seriesId)
                    .GetPayloadPack();

                var buffer = await client.Fetch(reqPack);
                var seriesT = ExtractSeries(ref buffer, baseName);
                return seriesT;
            }
            catch (Exception e)
            {
                if (e.Message.StartsWith("#16001") || e.Message.StartsWith("#16002") || e.Message.StartsWith("#16006"))
                {
                    return null;
                }
                else throw;
            }
        }

        public static async Task CreateSeries(this TsdbClient client, string baseName, SeriesT series)
        {
            var reqPack = 
                new FlowBuffer((byte)CmdType.SeriesCreate)
                .AddString(baseName)
                .AddInt64(series.Id)
                .AddString(series.Name)
                .AddInt64(series.Type)
                .AddByte((byte)series.ViewTimeMod)
                .AddString(series.Comment)
                .AddByte(series.Looping.Type)
                .AddString(series.Looping.Lt)
                .GetPayloadPack();

            await client.Fetch(reqPack, ResponseType.State);
        }

        public static async Task RemoveSeries(this TsdbClient client, string baseName, long seriesId)
        {
            var reqPack = 
                new FlowBuffer((byte)CmdType.SeriesRemove)
                .AddString(baseName)
                .AddInt64(seriesId)
                .GetPayloadPack();

            await client.Fetch(reqPack, ResponseType.State);
        }

        public static async Task UpdateSeries(this TsdbClient client, string baseName, SeriesT seriesUpd)
        {
            var reqBuffer = 
                new FlowBuffer((byte)CmdType.SeriesUpdate)
                .AddString(baseName)
                .AddInt64(seriesUpd.Id)
                .AddString(seriesUpd.Name)
                .AddInt64(seriesUpd.Type)
                .AddByte((byte)seriesUpd.ViewTimeMod)
                .AddString(seriesUpd.Comment)
                .AddByte(seriesUpd.Looping.Type)
                .AddString(seriesUpd.Looping.Lt)
                .GetPayloadPack();

            await client.Fetch(reqBuffer, ResponseType.State);
        }

        private static SeriesT ExtractSeries(ref ReadBuffer buffer, string baseName)
        {
            var id = buffer.GetInt64();
            var name = buffer.GetString();
            var dataClass = buffer.GetByte();
            var type = buffer.GetInt64();
            var vtm = (int)buffer.GetByte();
            var comment = buffer.GetString();
            var loopType = buffer.GetByte();
            var loopTime = buffer.GetString();
            var loopT = new LoopingT(loopType, loopTime);
            var serT = new SeriesT(name, id, type, baseName, comment, vtm, loopT, (DataClass)dataClass);
            return serT;
        }
    }
}