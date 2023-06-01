using FlowBufferEnvironment;

namespace TSDBConnector;

public static class TsdbBaseExtension
{
    public static async Task<List<BaseT>> GetBasesList(this TsdbClient api)
    {
        var reqBuffer = new FlowBuffer(CmdType.BaseGetList);
        await api.wrap.SendRequest(reqBuffer.GetCmdPack());
        var response = await api.wrap.GetResponse();
        var readBuffer = new ReadBuffer(response);
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
            var reqBuffer = new FlowBuffer(CmdType.BaseGetInfo);
            reqBuffer.AddString(baseName);
            await api.wrap.SendRequest(reqBuffer.GetPackWithPayload());
            var response = await api.wrap.GetResponse();
            var readBuffer = new ReadBuffer(response);
            var baseT = ExtractBase(ref readBuffer);
            return baseT;
        }
        catch(Exception e)
        {
            if (e.Message == "base not found")
            {
                return null;
            }
            else throw e;
        }
        
    }

    public static async Task CreateBase(this TsdbClient api, BaseT baseT)
    {
        var reqBuffer = new FlowBuffer(CmdType.BaseCreate);
        reqBuffer.AddString(baseT.Name);
        reqBuffer.AddString(baseT.Comment);
        reqBuffer.AddString(baseT.Path);
        reqBuffer.AddString(baseT.FsType);
        reqBuffer.AddString(baseT.DbSize);
        reqBuffer.AddByte(baseT.Looping.Type);
        reqBuffer.AddString(baseT.Looping.Lt);
        reqBuffer.AddBool(baseT.AutoAddSeries);
        reqBuffer.AddBool(baseT.AutoSave);
        reqBuffer.AddString(baseT.AutoSaveDuration);
        reqBuffer.AddString(baseT.AutoSaveInterval);
        await api.wrap.SendRequest(reqBuffer.GetPackWithPayload());
        await api.wrap.CheckResponseState();
    }

    public static async Task RemoveBase(this TsdbClient api, string baseName)
    {
        var reqBuffer = new FlowBuffer(CmdType.BaseRemove);
        reqBuffer.AddString(baseName);
        await api.wrap.SendRequest(reqBuffer.GetPackWithPayload());
        await api.wrap.CheckResponseState();
    }

    public static async Task UpdateBase(this TsdbClient api, string baseName, BaseT upd)
    {
        var reqBuffer = new FlowBuffer(CmdType.BaseUpdate);
        reqBuffer.AddString(baseName);
        reqBuffer.AddString(upd.Name);
        reqBuffer.AddString(upd.Comment);
        reqBuffer.AddString(upd.Path);
        reqBuffer.AddString(upd.DbSize);
        reqBuffer.AddByte(upd.Looping.Type);
        reqBuffer.AddString(upd.Looping.Lt);
        reqBuffer.AddBool(upd.AutoAddSeries);
        reqBuffer.AddBool(upd.AutoSave);
        reqBuffer.AddString(upd.AutoSaveDuration);
        reqBuffer.AddString(upd.AutoSaveInterval);
        await api.wrap.SendRequest(reqBuffer.GetPackWithPayload());
        await api.wrap.CheckResponseState();

        // TODO: change OpenedBaseList
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
