using FlowBufferEnvironment;

namespace TSDBConnector
{
    public static class TsdbBaseExtension
    {
        public static async Task<List<BaseT>> GetBasesList(this TsdbClient client)
        {
            var reqPack = new FlowBuffer((byte)CmdType.BaseGetList).GetCmdPack();
            var readBuffer = await client.Fetch(reqPack);
            var count = readBuffer.GetInt64();
            var result = new List<BaseT>();
            for (int i = 0; i < count; i++)
            {
                try
                {
                    var baseT = ExtractBase(ref readBuffer);
                    result.Add(baseT);
                }
                catch (Exception e)
                {
                    Console.WriteLine("buffer extracting error" + e.Message);
                }
            }
            return result;
        }

        public static async Task<BaseT?> GetBase(this TsdbClient client, string baseName)
        {
            try
            {
                var reqBuffer = new FlowBuffer((byte)CmdType.BaseGetInfo).AddString(baseName).GetPayloadPack();
                var readBuffer =  await client.Fetch(reqBuffer);
                var baseT = ExtractBase(ref readBuffer);
                return baseT;
            }
            catch (Exception e)
            {
                if (e.Message.Contains("base not found"))
                {
                    return null;
                }
                else throw;
            }
        }

        public static async Task CreateBase(this TsdbClient client, BaseT baseT)
        {
            var reqBuffer = 
                new FlowBuffer((byte)CmdType.BaseCreate)
                .AddString(baseT.Name)
                .AddString(baseT.Comment)
                .AddString(baseT.Path)
                .AddString(baseT.FsType)
                .AddString(baseT.DbSize)
                .AddByte(baseT.Looping.Type)
                .AddString(baseT.Looping.Lt)
                .AddBool(baseT.AutoAddSeries)
                .AddBool(baseT.AutoSave)
                .AddString(baseT.AutoSaveDuration)
                .AddString(baseT.AutoSaveInterval)
                .GetPayloadPack();

            await client.Fetch(reqBuffer, ResponseType.State);
        }

        public static async Task RemoveBase(this TsdbClient client, string baseName)
        {
            var reqPack = new FlowBuffer((byte)CmdType.BaseRemove).AddString(baseName).GetPayloadPack();
            await client.Fetch(reqPack, ResponseType.State);
        }

        public static async Task UpdateBase(this TsdbClient client, string baseName, BaseT upd)
        {
            var reqPack = 
                new FlowBuffer((byte)CmdType.BaseUpdate)
                .AddString(baseName)
                .AddString(upd.Name)
                .AddString(upd.Comment)
                .AddString(upd.Path)
                .AddString(upd.DbSize)
                .AddByte(upd.Looping.Type)
                .AddString(upd.Looping.Lt)
                .AddBool(upd.AutoAddSeries)
                .AddBool(upd.AutoSave)
                .AddString(upd.AutoSaveDuration)
                .AddString(upd.AutoSaveInterval)
                .GetPayloadPack();

            await client.Fetch(reqPack, ResponseType.State);

            if (client.OpenedBases.TryGetValue(baseName, out long id))
            {
                client.OpenedBases.Remove(baseName);
                client.OpenedBases.Add(upd.Name, id);
            }
        }

        public static async Task OpenBase(this TsdbClient client, string baseName, long id = -1)
        {
            if (client.TryOpenBases.TryGetValue(baseName, out id))
            {
                return;
            }

            if (id == -1)
            {
                var random = new Random();
                id = random.Next();
            }

            client.TryOpenBases.Add(baseName, id);

            var reqPack = new FlowBuffer((byte)CmdType.BaseOpen).AddInt64(id).AddString(baseName).GetPayloadPack();

            await client.Fetch(reqPack, ResponseType.State);

            client.TryOpenBases.Remove(baseName);
            client.OpenedBases.Add(baseName, id);
        }

        public static async Task CloseBase(this TsdbClient client, string baseName)
        {
            if (client.OpenedBases.TryGetValue(baseName, out long id))
            {
                var reqPack = new FlowBuffer((byte)CmdType.BaseClose).AddInt64(id).GetPayloadPack();
                await client.Fetch(reqPack, ResponseType.State);
                client.OpenedBases.Remove(baseName);
            }
        }


        private static BaseT ExtractBase(ref ReadBuffer buffer)
        {
            var name = buffer.GetString();
            var path = buffer.GetString();
            var comment = buffer.GetString();
            var status = buffer.GetInt64();
            var loopType = buffer.GetByte();
            var loopTime = buffer.GetString();
            var dbSize = buffer.GetString();
            var fsType = buffer.GetString();
            var autoAddSeries = buffer.GetBool();
            var autoSave = buffer.GetBool();
            var autoSaveDuration = buffer.GetString();
            var autoSaveInterval = buffer.GetString();
            var loopT = new LoopingT(loopType, loopTime);
            var baseT = new BaseT(name, path, dbSize, fsType,comment, loopT, autoAddSeries, autoSave, autoSaveDuration, autoSaveInterval, status);
            return baseT;
        }
    }
}
