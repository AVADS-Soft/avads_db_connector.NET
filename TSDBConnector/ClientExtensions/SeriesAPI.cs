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
            var reqPack = 
                new FlowBuffer(CmdType.SeriesGetAll)
                .AddString(baseName)
                .GetPayloadPack();

            var readBuffer = await api.Fetch(reqPack);
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
                var reqPack = 
                    new FlowBuffer(CmdType.SeriesGetInfo)
                    .AddString(baseName)
                    .AddString(seriesName)
                    .GetPayloadPack();

                var buffer = await api.Fetch(reqPack);
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
                var reqPack = 
                    new FlowBuffer(CmdType.GetSeriesById)
                    .AddString(baseName)
                    .AddInt64(seriesId)
                    .GetPayloadPack();

                var buffer = await api.Fetch(reqPack);
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
            var reqPack = 
                new FlowBuffer(CmdType.SeriesCreate)
                .AddString(baseName)
                .AddInt64(series.Id)
                .AddString(series.Name)
                .AddInt64(series.Type)
                .AddByte((byte)series.ViewTimeMod)
                .AddString(series.Comment)
                .AddByte(series.Looping.Type)
                .AddString(series.Looping.Lt)
                .GetPayloadPack();

            await api.Fetch(reqPack, ResponseType.State);
        }

        public static async Task RemoveSeries(this TsdbClient api, string baseName, long seriesId)
        {
            var reqPack = 
                new FlowBuffer(CmdType.SeriesRemove)
                .AddString(baseName)
                .AddInt64(seriesId)
                .GetPayloadPack();

            await api.Fetch(reqPack, ResponseType.State);
        }

        public static async Task UpdateSeries(this TsdbClient api, string baseName, SeriesT seriesUpd)
        {
            var reqBuffer = 
                new FlowBuffer(CmdType.SeriesUpdate)
                .AddString(baseName)
                .AddInt64(seriesUpd.Id)
                .AddString(seriesUpd.Name)
                .AddInt64(seriesUpd.Type)
                .AddByte((byte)seriesUpd.ViewTimeMod)
                .AddString(seriesUpd.Comment)
                .AddByte(seriesUpd.Looping.Type)
                .AddString(seriesUpd.Looping.Lt)
                .GetPayloadPack();

            await api.Fetch(reqBuffer, ResponseType.State);
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