using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using FlowBufferEnvironment;

namespace TSDBConnector
{
    public static class TsdbSeriesExtension
    {
        public static async Task<List<SeriesT>> GetSeriesList(this TsdbClient api, string baseName)
        {
            var reqBuffer = new FlowBuffer(CmdType.SeriesGetAll);
            reqBuffer.AddString(baseName);

            await api.wrap.SendRequest(reqBuffer.GetPackWithPayload());

            var response = await api.wrap.GetResponse();

            var readBuffer = new ReadBuffer(response);
            var count = readBuffer.GetInt64();
 
            var result = new List<SeriesT>();
            for (int i = 0; i < count; i++)
            {
                try
                {
                    var serT = ExtractSeries(ref readBuffer);
                    result.Add(serT);
                }
                catch(Exception e)
                {
                    Console.WriteLine("buffer extracting error" + e.Message);
                }
            }
            return result;
        }


        public static async Task<SeriesT?> GetSeries(this TsdbClient api, string baseName, string seriesName)
        {
            try
            {
                var reqBuffer = new FlowBuffer(CmdType.SeriesGetInfo);
                reqBuffer.AddString(baseName);
                reqBuffer.AddString(seriesName);
                await api.wrap.SendRequest(reqBuffer.GetPackWithPayload());
                var response = await api.wrap.GetResponse();
                var buffer = new ReadBuffer(response);
                var seriesT = ExtractSeries(ref buffer);
                return seriesT;
            }
            catch (Exception e)
            {
                // TODO: create special Exception, and add err number in base not found exc
                if (e.Message.StartsWith("#16006"))
                {
                    return null;
                }
                else throw;
            }
        }

        public static async Task<SeriesT?> GetSeriesById(this TsdbClient api, string baseName, long seriesId)
        {
            try
            {
                var reqBuffer = new FlowBuffer(CmdType.GetSeriesById);
                reqBuffer.AddString(baseName);
                reqBuffer.AddInt64(seriesId);
                await api.wrap.SendRequest(reqBuffer.GetPackWithPayload());
                var response = await api.wrap.GetResponse();
                var buffer = new ReadBuffer(response);
                var seriesT = ExtractSeries(ref buffer);
                return seriesT;
            }
            catch (Exception e)
            {
                // TODO: create special Exception, and add err number in base not found exc
                if (e.Message.StartsWith("#16006"))
                {
                    return null;
                }
                else throw;
            }
        }

        public static async Task AddSeries(this TsdbClient api, string baseName, SeriesT series)
        {
            var reqBuffer = new FlowBuffer(CmdType.SeriesCreate);
            reqBuffer.AddString(baseName);
            reqBuffer.AddInt64(series.Id);
            reqBuffer.AddString(series.Name);
            reqBuffer.AddInt64(series.Type);
            reqBuffer.AddByte((byte)series.ViewTimeMod);
            reqBuffer.AddString(series.Comment);
            reqBuffer.AddByte(series.Looping.Type);
            reqBuffer.AddString(series.Looping.Lt);

            await api.wrap.SendRequest(reqBuffer.GetPackWithPayload());

            // TODO:  naming: GetResponseState?
            await api.wrap.CheckResponseState();
        }

        public static async Task RemoveSeries(this TsdbClient api, string baseName, long seriesId)
        {
            var reqBuffer = new FlowBuffer(CmdType.SeriesRemove);
            reqBuffer.AddString(baseName);
            // TODO: fix error in ldb connector, it requires seriesName
            reqBuffer.AddInt64(seriesId);

            await api.wrap.SendRequest(reqBuffer.GetPackWithPayload());

            // TODO: rework naming? GetResponseState?
            await api.wrap.CheckResponseState();
        }

        public static async Task UpdateSeries(this TsdbClient api, string baseName, SeriesT seriesUpd)
        {
            var reqBuffer = new FlowBuffer(CmdType.SeriesUpdate);
            reqBuffer.AddString(baseName);
            reqBuffer.AddInt64(seriesUpd.Id);
            reqBuffer.AddString(seriesUpd.Name);
            reqBuffer.AddInt64(seriesUpd.Type);
            reqBuffer.AddByte((byte)seriesUpd.ViewTimeMod);
            reqBuffer.AddString(seriesUpd.Comment);
            reqBuffer.AddByte(seriesUpd.Looping.Type);
            reqBuffer.AddString(seriesUpd.Looping.Lt);

            await api.wrap.SendRequest(reqBuffer.GetPackWithPayload());

            // TODO:  naming: GetResponseState?
            await api.wrap.CheckResponseState();
        }


        private static SeriesT ExtractSeries(ref ReadBuffer buffer)
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
            var serT = new SeriesT(name, id, type, comment, vtm, loopT, dataClass);
            return serT;
        }
    }
}