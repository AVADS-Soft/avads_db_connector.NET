using FlowBufferEnvironment;

namespace TSDBConnector
{
    public static class TsdbRowsExtensions
    {
        public static async Task DataAddRows(this TsdbClient client, string baseName, RowsCacheT rows)
        {
            var baseId = client.GetTempBaseId(baseName);
            if (baseId == -1) throw new BaseIsNotOpenedException();
           
            var reqPack = 
                new FlowBuffer((byte)CmdType.DataAddRows)
                .AddInt64(baseId)
                .AddBytes(rows.Cache)
                .GetPayloadPack();

            await client.Fetch(reqPack, ResponseType.State);
        }

        public static async Task<long> DataAddRowsCache(this TsdbClient client, string baseName, RowsCacheT rows)
        {
            var baseId = client.GetTempBaseId(baseName);
            if (baseId == -1) throw new BaseIsNotOpenedException();
            
            var reqPack = 
                new FlowBuffer((byte)CmdType.DataAddRowCache)
                .AddInt64(baseId)
                .AddBytes(rows.Cache)
                .GetPayloadPack();

            var readBuffer = await client.Fetch(reqPack);
            var count = readBuffer.GetInt64();
            return count;
        }

        public static async Task DataAddRow(
            this TsdbClient client,
            string baseName,
            long seriesId,
            DataClass dataClass,
            long time,
            UInt32 quality,
            object value)
        {
            var baseId = client.GetTempBaseId(baseName);
            if (baseId == -1) throw new BaseIsNotOpenedException();
            
            var reqPack = 
                new FlowBuffer((byte)CmdType.DataAddRow)
                .AddInt64(baseId)
                .AddRec(seriesId, dataClass, time, quality, value)
                .GetPayloadPack();

            await client.Fetch(reqPack, ResponseType.State);
        }


        public static async Task<RowT?> DataGetLastValue(this TsdbClient client, string baseName, long seriesId, DataClass dataClass)
        {
            var baseId = client.GetTempBaseId(baseName);
            if (baseId == -1) throw new BaseIsNotOpenedException();
            try
            {
                var reqPack = 
                new FlowBuffer((byte)CmdType.DataGetLastValue)
                    .AddInt64(baseId)
                    .AddInt64(seriesId)
                    .GetPayloadPack();

                var readBuffer = await client.Fetch(reqPack);
                var rec = readBuffer.GetRec(dataClass);
                return rec;
            }
            catch (Exception)
            {
                return null;
            }
        }

        public static async Task<RowT?> DataGetValueAtTime(this TsdbClient client, string baseName, long seriesId, DataClass dataClass, long t)
        {
            var baseId = client.GetTempBaseId(baseName);
            if (baseId == -1) throw new BaseIsNotOpenedException();
            try
            {
                var reqPack = 
                    new FlowBuffer((byte)CmdType.DataGetValueAtTime)
                    .AddInt64(baseId)
                    .AddInt64(seriesId)
                    .AddInt64(t)
                    .GetPayloadPack();

                var readBuffer = await client.Fetch(reqPack);
                var rec = readBuffer.GetRec(dataClass);
                return rec;
            }
            catch (Exception)
            {
                return null;
            }
        }

        public static async Task<string> DataGetCP(this TsdbClient client, string baseName, long seriesId, long t)
        {
            var baseId = client.GetTempBaseId(baseName);
            if (baseId == -1) throw new BaseIsNotOpenedException();

            var reqPack = 
                new FlowBuffer((byte)CmdType.DataGetCP)
                .AddInt64(baseId)
                .AddInt64(seriesId)
                .AddInt64(t)
                .GetPayloadPack();

            var readBuffer = await client.Fetch(reqPack);
            var result = readBuffer.GetString();
            return result;
        }
        
        public static async Task<RecsWithCP?> DataGetRange(
            this TsdbClient client,
            string baseName,
            long seriesId,
            DataClass dataClass,
            SeekDirection direct,
            long limit,
            long min,
            long max,
            short dpi = 0)
        {
            var baseId = client.GetTempBaseId(baseName);
            if (baseId == -1) throw new BaseIsNotOpenedException();
            try
            {
                var reqPack = 
                    new FlowBuffer((byte)CmdType.DateGetRangeDirection)
                    .AddInt64(baseId)
                    .AddInt64(seriesId)
                    .AddByte((byte)direct)
                    .AddInt64(limit)
                    .AddInt64(min)
                    .AddInt64(max)
                    .AddInt16(dpi)
                    .GetPayloadPack();

                var readBuffer = await client.Fetch(reqPack);
                var result = ExtractRecs(readBuffer, dataClass);
                return result;
            }
            catch (Exception)
            {
                return null;
            }
            
        }
        
        public static async Task<RecsWithCP?> DataGetFromCP(
            this TsdbClient client,
            string baseName,
            DataClass dataClass,
            string cp,
            SeekDirection direct,
            long limit)
        {
            var baseId = client.GetTempBaseId(baseName);
            if (baseId == -1) throw new BaseIsNotOpenedException();
            try
            {
                var reqPack = 
                    new FlowBuffer((byte)CmdType.DataGetFromCP)
                    .AddInt64(baseId)
                    .AddString(cp)
                    .AddByte((byte)direct)
                    .AddInt64(limit)
                    .GetPayloadPack();

                var readBuffer = await client.Fetch(reqPack);
                var result = ExtractRecs(readBuffer, dataClass);
                return result;
            }
            catch (Exception)
            {
                return null;
            }
            
        }

        public static async Task<RecsWithCP?> DataGetRangeFromCP(
            this TsdbClient client,
            string baseName,
            DataClass dataClass,
            string cp,
            SeekDirection direct,
            long limit,
            long min,
            long max,
            short dpi = 0)
        {
            var baseId = client.GetTempBaseId(baseName);
            if (baseId == -1) throw new BaseIsNotOpenedException();
            try
            {
                var reqPack = 
                    new FlowBuffer((byte)CmdType.DateGetRangeFromCP)
                    .AddInt64(baseId)
                    .AddString(cp)
                    .AddByte((byte)direct)
                    .AddInt64(limit)
                    .AddInt64(min)
                    .AddInt64(max)
                    .AddInt16(dpi)
                    .GetPayloadPack();

                var readBuffer = await client.Fetch(reqPack);
                var result = ExtractRecs(readBuffer, dataClass);
                return result;
            }
            catch (Exception)
            {
                return null;
            }
        }
        
        public static async Task DataDeleteRow(this TsdbClient client, string baseName, long seriesId, long t)
        {
            var baseId = client.GetTempBaseId(baseName);
            if (baseId == -1) throw new BaseIsNotOpenedException();
            var reqPack = 
                new FlowBuffer((byte)CmdType.DataDeleteRow)
                .AddInt64(baseId)
                .AddInt64(seriesId)
                .AddInt64(t)
                .GetPayloadPack();

            await client.Fetch(reqPack, ResponseType.State);
        }
        
        public static async Task<long> DataDeleteRows(this TsdbClient client, string baseName, long seriesId, long timeStart, long timeEnd)
        {
            var baseId = client.GetTempBaseId(baseName);
            if (baseId == -1) throw new BaseIsNotOpenedException();
                
            var reqBuffer = 
                new FlowBuffer((byte)CmdType.DataDeleteRows)
                .AddInt64(baseId)
                .AddInt64(seriesId)
                .AddInt64(timeStart)
                .AddInt64(timeEnd)
                .GetPayloadPack();

            var readBuffer = await client.Fetch(reqBuffer);
            var count = readBuffer.GetInt64();
            return count;
        }

        public static async Task<BoundaryT?> DataGetBoundary(this TsdbClient client, string baseName, long seriesId)
        {
            var baseId = client.GetTempBaseId(baseName);
            if (baseId == -1) throw new BaseIsNotOpenedException();
            try
            {
                var reqBuffer = 
                    new FlowBuffer((byte)CmdType.DataGetBoundary)
                    .AddInt64(baseId)
                    .AddInt64(seriesId)
                    .GetPayloadPack();

                var readBuffer = await client.Fetch(reqBuffer);
                var min = readBuffer.GetInt64();
                var max = readBuffer.GetInt64();
                var count = readBuffer.GetInt64();
                var startCp = readBuffer.GetString();
                var endCp = readBuffer.GetString();

                return new BoundaryT(min, max, count, startCp, endCp);
            }
            catch (Exception)
            {
                return null;
            }
        }

        private static RecsWithCP ExtractRecs(ReadBuffer readBuffer, DataClass dataClass)
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

        public static FlowBuffer AddRec(this FlowBuffer buffer, long seriesId, DataClass dataClass, long time, UInt32 quality, object value)
        {
            buffer.AddInt64(seriesId).AddByte((byte)dataClass).AddInt64(time);
            if (dataClass == DataClass.Atomic)
            {
                byte[] bytes = ByteConverter.AtomicToBytes(value);
                buffer.AddBytes(bytes);
            }
            else if (dataClass == DataClass.Blob)
            {
                byte[] bytes = ByteConverter.BlobToBytes(value);
                buffer.AddInt32(bytes.Length).AddBytes(bytes);
            }
            buffer.AddInt32((Int32)quality);
            return buffer;
        }
        public static RowT GetRec(this ReadBuffer buffer, DataClass dataClass)
        {
            var value = new byte[0];
            var time = buffer.GetInt64();
            if (dataClass == DataClass.Atomic)
            {
                value = buffer.GetBytes(8);
            }
            else if (dataClass == DataClass.Blob)
            {
                value = buffer.GetBlob();
            }
            var q = buffer.GetBytes(4);
            var rec = new RowT(time, value, q);
            return rec;
        }
    }
}