using FlowBufferEnvironment;

namespace TSDBConnector
{
    public static class TsdbRowsExtension
    {
        public static void AddRec(this FlowBuffer buffer, long seriesId, byte dataClass, long time, UInt32 quality, object value)
        {
            buffer.AddInt64(seriesId);
            buffer.AddByte(dataClass);
            buffer.AddInt64(time);

            if (dataClass == 0)
            {
                byte[] bytes = ByteConverter.AtomicToBytes(value);
                buffer.AddBytes(bytes);
            }
            else if (dataClass == 1)
            {
                byte[] bytes = ByteConverter.BlobToBytes(value);
                buffer.AddInt32(bytes.Length);
                buffer.AddBytes(bytes);
            }
            buffer.AddInt32((Int32)quality);
        }
        public static async Task DataAddRows(this TsdbClient api, string baseName, RowsCacheT rows)
        {
            var baseId = api.GetTempBaseId(baseName);
            if (baseId != -1)
            {
                var reqBuffer = new FlowBuffer(CmdType.DataAddRow);
                reqBuffer.AddInt64(baseId);
                reqBuffer.AddBytes(rows.Cache);

                await api.SendRequest(reqBuffer.GetPackWithPayload());

                await api.CheckResponseState();
            }
        }

        public static async Task<long> DataAddRowsCache(this TsdbClient api, string baseName, RowsCacheT rows)
        {
            var baseId = api.GetTempBaseId(baseName);
            if (baseId != -1)
            {
                var reqBuffer = new FlowBuffer(CmdType.DataAddRowCache);
                reqBuffer.AddInt64(baseId);
                reqBuffer.AddBytes(rows.Cache);

                await api.SendRequest(reqBuffer.GetPackWithPayload());

                var response = await api.GetResponse();

                var readBuffer = new ReadBuffer(response);

                var count = readBuffer.GetInt64();

                return count;
            }
            // TODO: throw exception everywhere on failed get base
            return 0;
        }

        // TODO: create shorthand adding row data to buffer;

        // TODO: create override method 'add row' with rec as argument
        // TODO: create enum for dataClass
        public static async Task DataAddRow(this TsdbClient api, string baseName, long seriesId, byte dataClass, long time, UInt32 quality, object value)
        {
            var baseId = api.GetTempBaseId(baseName);
            if (baseId != -1)
            {
                var reqBuffer = new FlowBuffer(CmdType.DataAddRow);
                reqBuffer.AddInt64(baseId);

                reqBuffer.AddRec(seriesId, dataClass, time, quality, value);

                await api.SendRequest(reqBuffer.GetPackWithPayload());

                await api.CheckResponseState();
            }
            else throw new Exception("no available bases, possibly base is not opened");
        }


        public static async Task<RowT?> DataGetLastValue(this TsdbClient api, string baseName, long seriesId, byte dataClass)
        {
            var baseId = api.GetTempBaseId(baseName);
            if (baseId != -1)
            {
                var reqBuffer = new FlowBuffer(CmdType.DataGetLastValue);
                reqBuffer.AddInt64(baseId);
                reqBuffer.AddInt64(seriesId);

                await api.SendRequest(reqBuffer.GetPackWithPayload());

                var response = await api.GetResponse();

                var readBuffer = new ReadBuffer(response);

                var time = readBuffer.GetInt64();
                byte[] value = new byte[0];
                if (dataClass == 0)
                {
                    value = readBuffer.GetBytes(8);
                }
                else if (dataClass == 1)
                {
                    // TODO: mb create method inserting bytes with length
                    var tempStr = readBuffer.GetString();
                    value = ByteConverter.StringToBytes(tempStr);
                }

                var q = readBuffer.GetBytes(4);
                return new RowT(time, value, q);
            }
            return null;
        }

        public static async Task<RowT?> DataGetValueAtTime(this TsdbClient api, string baseName, long seriesId, byte dataClass, long t)
        {
            var baseId = api.GetTempBaseId(baseName);
            if (baseId != -1)
            {
                var reqBuffer = new FlowBuffer(CmdType.DataGetValueAtTime);
                reqBuffer.AddInt64(baseId);
                reqBuffer.AddInt64(seriesId);
                reqBuffer.AddInt64(t);

                await api.SendRequest(reqBuffer.GetPackWithPayload());

                var response = await api.GetResponse();

                // TODO: create method, remove duplicate
                var readBuffer = new ReadBuffer(response);
                var time = readBuffer.GetInt64();

                byte[] value = new byte[0];
                if (dataClass == 0)
                {
                    value = readBuffer.GetBytes(8);
                }
                else if (dataClass == 1)
                {
                    // TODO: mb create method inserting bytes with length
                    var tempStr = readBuffer.GetString();
                    value = ByteConverter.StringToBytes(tempStr);
                }

                var q = readBuffer.GetBytes(4);
                return new RowT(time, value, q);
            }
            return null;
        }

        public static async Task<string> DataGetCP(this TsdbClient api, string baseName, long seriesId, long t)
        {
            var baseId = api.GetTempBaseId(baseName);
            if (baseId != -1)
            {
                // TODO: remove dupl, send with t request?
                var reqBuffer = new FlowBuffer(CmdType.DataGetCP);
                reqBuffer.AddInt64(baseId);
                reqBuffer.AddInt64(seriesId);
                reqBuffer.AddInt64(t);

                await api.SendRequest(reqBuffer.GetPackWithPayload());

                var response = await api.GetResponse();

                var readBuffer = new ReadBuffer(response);

                var result = readBuffer.GetString();
                return result;
            }
            return String.Empty;
        }
        
        // TODO: add direction enum (tomax = 1, tomin = 2)
        public static async Task<RecsWithCP?> DataGetRange(this TsdbClient api, string baseName, long seriesId, byte direct, long limit, long min, long max, short dpi)
        {
            var baseId = api.GetTempBaseId(baseName);
            if (baseId != -1)
            {
                var reqBuffer = new FlowBuffer(CmdType.DateGetRangeDirection);
                reqBuffer.AddInt64(baseId);
                reqBuffer.AddInt64(seriesId);
                reqBuffer.AddByte(direct);
                reqBuffer.AddInt64(limit);
                reqBuffer.AddInt64(min);
                reqBuffer.AddInt64(max);
                reqBuffer.AddInt16(dpi);
                
                await api.SendRequest(reqBuffer.GetPackWithPayload());

                var response = await api.GetResponse();

                var readBuffer = new ReadBuffer(response);

                var startCp = readBuffer.GetString();
                var endCp = readBuffer.GetString();
                var hasCont = readBuffer.GetBool();

                var recsCount = readBuffer.GetInt64();
                var recs = new RowT[recsCount];
                for (long i = 0; i < recsCount; i++)
                {
                    var time = readBuffer.GetInt64();
                    // TODO: it possible be a blob?
                    var value = readBuffer.GetBytes(8);
                    var q = readBuffer.GetBytes(4);

                    var rec = new RowT(time, value, q);
                    recs[i] = rec;
                }
                var result = new RecsWithCP(recs, startCp, endCp, hasCont);
                return result;
            }
            return null;
        }
        
        public static async Task<RecsWithCP?> DataGetFromCP(this TsdbClient api, string baseName, string cp, byte direct, long limit)
        {
            var baseId = api.GetTempBaseId(baseName);
            if (baseId != -1)
            {
                var reqBuffer = new FlowBuffer(CmdType.DataGetFromCP);
                reqBuffer.AddInt64(baseId);
                reqBuffer.AddString(cp);
                reqBuffer.AddByte(direct);
                reqBuffer.AddInt64(limit);
                
                await api.SendRequest(reqBuffer.GetPackWithPayload());

                var response = await api.GetResponse();

                var readBuffer = new ReadBuffer(response);
                // TODO: get recWith cp shorthand method, remove duplicate
                var startCp = readBuffer.GetString();
                var endCp = readBuffer.GetString();
                var hasCont = readBuffer.GetBool();

                var recsCount = readBuffer.GetInt64();
                var recs = new RowT[recsCount];
                for (long i = 0; i < recsCount; i++)
                {
                    var time = readBuffer.GetInt64();
                    // TODO: it possible be a blob?
                    var value = readBuffer.GetBytes(8);
                    var q = readBuffer.GetBytes(4);

                    var rec = new RowT(time, value, q);
                    recs[i] = rec;
                }
                var result = new RecsWithCP(recs, startCp, endCp, hasCont);
                return result;

            }
            return null;
        }

        // TODO: create arguments default values
        public static async Task<RecsWithCP?> DataGetRangeFromCP(this TsdbClient api, string baseName, string cp, byte direct, long limit, long min, long max, short dpi)
        {
            var baseId = api.GetTempBaseId(baseName);
            if (baseId != -1)
            {
                var reqBuffer = new FlowBuffer(CmdType.DateGetRangeFromCP);
                reqBuffer.AddInt64(baseId);
                reqBuffer.AddString(cp);
                reqBuffer.AddByte(direct);
                reqBuffer.AddInt64(limit);
                reqBuffer.AddInt64(min);
                reqBuffer.AddInt64(max);
                reqBuffer.AddInt16(dpi);
                
                await api.SendRequest(reqBuffer.GetPackWithPayload());

                var response = await api.GetResponse();

                var readBuffer = new ReadBuffer(response);
                // TODO: get recWith cp shorthand method, remove duplicate
                var startCp = readBuffer.GetString();
                var endCp = readBuffer.GetString();
                var hasCont = readBuffer.GetBool();

                var recsCount = readBuffer.GetInt64();
                var recs = new RowT[recsCount];
                for (long i = 0; i < recsCount; i++)
                {
                    var time = readBuffer.GetInt64();
                    // TODO: it possible be a blob?
                    var value = readBuffer.GetBytes(8);
                    var q = readBuffer.GetBytes(4);

                    var rec = new RowT(time, value, q);
                    recs[i] = rec;
                }
                var result = new RecsWithCP(recs, startCp, endCp, hasCont);
                return result;

            }
            return null;
            
        }
        
        public static async Task DataDeleteRow(this TsdbClient api, string baseName, long seriesId, long t)
        {
            var baseId = api.GetTempBaseId(baseName);
            if (baseId != -1)
            {
                // TODO: remove dupl, send with t request?
                var reqBuffer = new FlowBuffer(CmdType.DataDeleteRow);
                reqBuffer.AddInt64(baseId);
                reqBuffer.AddInt64(seriesId);
                reqBuffer.AddInt64(t);

                await api.SendRequest(reqBuffer.GetPackWithPayload());

                await api.CheckResponseState();
            }
        }
        
        public static async Task<long> DataDeleteRows(this TsdbClient api, string baseName, long seriesId, long timeStart, long timeEnd)
        {
            var baseId = api.GetTempBaseId(baseName);
            if (baseId != -1)
            {
                // TODO: remove dupl, send with t request?
                var reqBuffer = new FlowBuffer(CmdType.DataDeleteRows);
                reqBuffer.AddInt64(baseId);
                reqBuffer.AddInt64(seriesId);
                reqBuffer.AddInt64(timeStart);
                reqBuffer.AddInt64(timeEnd);

                await api.SendRequest(reqBuffer.GetPackWithPayload());

                var response = await api.GetResponse();

                var readBuffer = new ReadBuffer(response);
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
                // TODO: remove dupl, send with t request?
                var reqBuffer = new FlowBuffer(CmdType.DataGetBoundary);
                reqBuffer.AddInt64(baseId);
                reqBuffer.AddInt64(seriesId);
                // TODO: refactor GETPACK, compute according buffer entries
                await api.SendRequest(reqBuffer.GetPackWithPayload());

                var response = await api.GetResponse();

                var readBuffer = new ReadBuffer(response);
                var min = readBuffer.GetInt64();
                var max = readBuffer.GetInt64();
                var count = readBuffer.GetInt64();
                var startCp = readBuffer.GetString();
                var endCp = readBuffer.GetString();

                return new BoundaryT(min, max, count, startCp, endCp);
            }
            return null;
        }
    }
}