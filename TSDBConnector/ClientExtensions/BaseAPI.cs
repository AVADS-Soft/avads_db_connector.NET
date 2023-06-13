using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using FlowBufferEnvironment;

namespace TSDBConnector
{
    public static class TsdbBaseExtension
    {
        public static async Task<List<BaseT>> GetBasesList(this TsdbClient api)
        {
            var reqPack = new FlowBuffer(CmdType.BaseGetList).GetCmdPack();
            var readBuffer = await api.Fetch(reqPack);
            var count = readBuffer.GetInt64();

            var result = new List<BaseT>();
            for (int i = 0; i < count; i++)
            {
                try
                {
                    var baseT = ExtractBase(ref readBuffer);
                    result.Add(baseT);
                }
                catch(Exception e)
                {
                    Console.WriteLine("buffer extracting error" + e.Message);
                }
            }
            return result;
        }

        public static async Task<BaseT?> GetBase(this TsdbClient api, string baseName)
        {
            try
            {
                var reqBuffer = new FlowBuffer(CmdType.BaseGetInfo).AddString(baseName).GetPayloadPack();
                var readBuffer =  await api.Fetch(reqBuffer);
                var baseT = ExtractBase(ref readBuffer);
                return baseT;
            }
            catch(Exception e)
            {
                if (e.Message == "base not found")
                {
                    return null;
                }
                else throw;
            }

        }

        public static async Task CreateBase(this TsdbClient api, BaseT baseT)
        {
            var reqBuffer = 
                new FlowBuffer(CmdType.BaseCreate)
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

            await api.Fetch(reqBuffer, ResponseType.State);
        }

        public static async Task RemoveBase(this TsdbClient api, string baseName)
        {
            var reqPack = new FlowBuffer(CmdType.BaseRemove).AddString(baseName).GetPayloadPack();
            await api.Fetch(reqPack, ResponseType.State);
        }

        public static async Task UpdateBase(this TsdbClient api, string baseName, BaseT upd)
        {
            var reqPack = 
                new FlowBuffer(CmdType.BaseUpdate)
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

            await api.Fetch(reqPack, ResponseType.State);

            if (api.OpenedBases.TryGetValue(baseName, out long id))
            {
                api.OpenedBases.Remove(baseName);
                api.OpenedBases.Add(upd.Name, id);
            }
        }

        public static async Task OpenBase(this TsdbClient api, string baseName)
        {
            long id;
            if (api.TryOpenBases.TryGetValue(baseName, out id) || api.OpenedBases.TryGetValue(baseName, out id))
            {
                return;
            }
            var random = new Random();

            // TODO: sure that id is unique 
            id = random.Next();
            api.TryOpenBases.Add(baseName, id);

            var reqPack = new FlowBuffer(CmdType.BaseOpen).AddInt64(id).AddString(baseName).GetPayloadPack();

            await api.Fetch(reqPack, ResponseType.State);

            api.TryOpenBases.Remove(baseName);
            api.OpenedBases.Add(baseName, id);
        }

        public static async Task CloseBase(this TsdbClient api, string baseName)
        {
            if (api.OpenedBases.TryGetValue(baseName, out long id))
            {
                var reqPack = new FlowBuffer(CmdType.BaseClose).AddInt64(id).GetPayloadPack();
                await api.Fetch(reqPack, ResponseType.State);
                api.OpenedBases.Remove(baseName);
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
