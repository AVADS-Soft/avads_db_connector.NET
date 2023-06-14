using FlowBufferEnvironment;

namespace TSDBConnector
{
    public static class TsdbRowsExtension
    {
        public static FlowBuffer AddRec(this FlowBuffer buffer, long seriesId, byte dataClass, long time, UInt32 quality, object value)
        {
            buffer.AddInt64(seriesId).AddByte(dataClass).AddInt64(time);
            if (dataClass == 0)
            {
                byte[] bytes = ByteConverter.AtomicToBytes(value);
                buffer.AddBytes(bytes);
            }
            else if (dataClass == 1)
            {
                byte[] bytes = ByteConverter.BlobToBytes(value);
                buffer.AddInt32(bytes.Length).AddBytes(bytes);
            }
            buffer.AddInt32((Int32)quality);
            return buffer;
        }
        public static RowT GetRec(this ReadBuffer buffer, byte dataClass)
        {
            var value = new byte[0];
            var time = buffer.GetInt64();
            if (dataClass == 0)
            {
                value = buffer.GetBytes(8);
            }
            else if (dataClass == 1)
            {
                value = buffer.GetBlob();
            }
            var q = buffer.GetBytes(4);
            var rec = new RowT(time, value, q);
            return rec;
        }

        public static async Task DataAddRows(this TsdbClient api, string baseName, RowsCacheT rows)
        {
            var baseId = api.GetTempBaseId(baseName);
            if (baseId != -1)
            {
                var reqPack = 
                    new FlowBuffer(CmdType.DataAddRows)
                    .AddInt64(baseId)
                    .AddBytes(rows.Cache)
                    .GetPayloadPack();

                await api.Fetch(reqPack, ResponseType.State);
            }
        }

        public static async Task<long> DataAddRowsCache(this TsdbClient api, string baseName, RowsCacheT rows)
        {
            var baseId = api.GetTempBaseId(baseName);
            if (baseId != -1)
            {
                var reqPack = 
                    new FlowBuffer(CmdType.DataAddRowCache)
                    .AddInt64(baseId)
                    .AddBytes(rows.Cache)
                    .GetPayloadPack();

                var readBuffer = await api.Fetch(reqPack);
                var count = readBuffer.GetInt64();
                return count;
            }
            // TODO: throw exception everywhere on failed get base
            return 0;
        }

        // TODO: create enum for dataClass
        public static async Task DataAddRow(this TsdbClient api, string baseName, long seriesId, byte dataClass, long time, UInt32 quality, object value)
        {
            var baseId = api.GetTempBaseId(baseName);
            if (baseId != -1)
            {
                var reqPack = 
                    new FlowBuffer(CmdType.DataAddRow)
                    .AddInt64(baseId)
                    .AddRec(seriesId, dataClass, time, quality, value)
                    .GetPayloadPack();

                await api.Fetch(reqPack, ResponseType.State);
            }
            else throw new Exception("no available bases, possibly base is not opened");
        }


        public static async Task<RowT?> DataGetLastValue(this TsdbClient api, string baseName, long seriesId, byte dataClass)
        {
            var baseId = api.GetTempBaseId(baseName);
            if (baseId != -1)
            {
                var reqPack = 
                    new FlowBuffer(CmdType.DataGetLastValue)
                    .AddInt64(baseId)
                    .AddInt64(seriesId)
                    .GetPayloadPack();

                var readBuffer = await api.Fetch(reqPack);
                var rec = readBuffer.GetRec(dataClass);
                return rec;
            }
            return null;
        }

        public static async Task<RowT?> DataGetValueAtTime(this TsdbClient api, string baseName, long seriesId, byte dataClass, long t)
        {
            var baseId = api.GetTempBaseId(baseName);
            if (baseId != -1)
            {
                var reqPack = 
                    new FlowBuffer(CmdType.DataGetValueAtTime)
                    .AddInt64(baseId)
                    .AddInt64(seriesId)
                    .AddInt64(t)
                    .GetPayloadPack();

                var readBuffer = await api.Fetch(reqPack);
                var rec = readBuffer.GetRec(dataClass);
                return rec;
            }
            return null;
        }

        public static async Task<string> DataGetCP(this TsdbClient api, string baseName, long seriesId, long t)
        {
            var baseId = api.GetTempBaseId(baseName);
            if (baseId != -1)
            {

                var reqPack = 
                    new FlowBuffer(CmdType.DataGetCP)
                    .AddInt64(baseId)
                    .AddInt64(seriesId)
                    .AddInt64(t)
                    .GetPayloadPack();

                var readBuffer = await api.Fetch(reqPack);
                var result = readBuffer.GetString();
                return result;
            }
            return String.Empty;
        }
        
        // TODO: add direction enum (tomax = 1, tomin = 2)
        public static async Task<RecsWithCP?> DataGetRange(this TsdbClient api, string baseName, long seriesId, byte dataClass, byte direct, long limit, long min, long max, short dpi)
        {
            var baseId = api.GetTempBaseId(baseName);
            if (baseId != -1)
            {
                var reqPack = 
                    new FlowBuffer(CmdType.DateGetRangeDirection)
                    .AddInt64(baseId)
                    .AddInt64(seriesId)
                    .AddByte(direct)
                    .AddInt64(limit)
                    .AddInt64(min)
                    .AddInt64(max)
                    .AddInt16(dpi)
                    .GetPayloadPack();

                var readBuffer = await api.Fetch(reqPack);
                var result = ExtractRecs(readBuffer, dataClass);
                return result;
            }
            return null;
        }
        
        public static async Task<RecsWithCP?> DataGetFromCP(this TsdbClient api, string baseName, byte dataClass, string cp, byte direct, long limit)
        {
            var baseId = api.GetTempBaseId(baseName);
            if (baseId != -1)
            {
                var reqPack = 
                    new FlowBuffer(CmdType.DataGetFromCP)
                    .AddInt64(baseId)
                    .AddString(cp)
                    .AddByte(direct)
                    .AddInt64(limit)
                    .GetPayloadPack();

                var readBuffer = await api.Fetch(reqPack);
                var result = ExtractRecs(readBuffer, dataClass);
                return result;
            }
            return null;
        }

        // TODO: create arguments default values
        public static async Task<RecsWithCP?> DataGetRangeFromCP(this TsdbClient api, string baseName, byte dataClass, string cp, byte direct, long limit, long min, long max, short dpi)
        {
            var baseId = api.GetTempBaseId(baseName);
            if (baseId != -1)
            {
                var reqPack = 
                    new FlowBuffer(CmdType.DateGetRangeFromCP)
                    .AddInt64(baseId)
                    .AddString(cp)
                    .AddByte(direct)
                    .AddInt64(limit)
                    .AddInt64(min)
                    .AddInt64(max)
                    .AddInt16(dpi)
                    .GetPayloadPack();

                var readBuffer = await api.Fetch(reqPack);
                var result = ExtractRecs(readBuffer, dataClass);
                return result;
            }
            return null;
        }
        
        public static async Task DataDeleteRow(this TsdbClient api, string baseName, long seriesId, long t)
        {
            var baseId = api.GetTempBaseId(baseName);
            if (baseId != -1)
            {
                var reqPack = 
                    new FlowBuffer(CmdType.DataDeleteRow)
                    .AddInt64(baseId)
                    .AddInt64(seriesId)
                    .AddInt64(t)
                    .GetPayloadPack();

                await api.Fetch(reqPack, ResponseType.State);
            }
        }
        
        public static async Task<long> DataDeleteRows(this TsdbClient api, string baseName, long seriesId, long timeStart, long timeEnd)
        {
            var baseId = api.GetTempBaseId(baseName);
            if (baseId != -1)
            {
                var reqBuffer = 
                    new FlowBuffer(CmdType.DataDeleteRows)
                    .AddInt64(baseId)
                    .AddInt64(seriesId)
                    .AddInt64(timeStart)
                    .AddInt64(timeEnd)
                    .GetPayloadPack();

                var readBuffer = await api.Fetch(reqBuffer);
                var count = readBuffer.GetInt64();
                return count;
            }
            return 0;
        }

        public static async Task<BoundaryT?> DataGetBoundary(this TsdbClient api, string baseName, long seriesId)
        {
            var baseId = api.GetTempBaseId(baseName);
            if (baseId != -1)
            {
                var reqBuffer = 
                    new FlowBuffer(CmdType.DataGetBoundary)
                    .AddInt64(baseId)
                    .AddInt64(seriesId)
                    .GetPayloadPack();

                var readBuffer = await api.Fetch(reqBuffer);
                var min = readBuffer.GetInt64();
                var max = readBuffer.GetInt64();
                var count = readBuffer.GetInt64();
                var startCp = readBuffer.GetString();
                var endCp = readBuffer.GetString();

                return new BoundaryT(min, max, count, startCp, endCp);
            }
            return null;
        }

        private static RecsWithCP ExtractRecs(ReadBuffer readBuffer, byte dataClass)
        {
            var startCp = readBuffer.GetString();
            var endCp = readBuffer.GetString();
            var hasCont = readBuffer.GetBool();
            var recsCount = readBuffer.GetInt64();
            var recs = new RowT[recsCount];
            for (long i = 0; i < recsCount; i++)
            {
                recs[i] = readBuffer.GetRec(dataClass);
            }
            var result = new RecsWithCP(recs, startCp, endCp, hasCont);
            return result;
        }
    }
}