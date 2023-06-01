using FlowBufferEnvironment;
using System.Security.Cryptography;

namespace TSDBConnector;
public class TsdbClient : IDisposable
{    
    public TcpWrapper wrap;

    // TODO: add getters/setters
    public bool isConnected = false;

    public string sessionKey = String.Empty;

    public TsdbClient()
    {
        wrap = new TcpWrapper();
    }

    public async Task CreateConnection(string host, int port, string login, string password)
    {
        await wrap.InitConnection(host, port);
        
        await Login(login, password);
    }   

    // TODO: refactor login
    private async Task Login(string login, string password)
    {
        var flow = new FlowBuffer(ProtocolCmd.LoginGetKeys);

        flow.AddString(login);

        await wrap.SendRequest(flow.GetCmdPack());

        var resp = await wrap.GetResponse();

        Tuple<string, string> info = SplitLoginInfo(resp);

        string hash = GetAuthHash(info, login, password);

        var authBuff = new FlowBuffer(ProtocolCmd.LoginValidPass);
        authBuff.AddString(hash);

        await wrap.SendRequest(authBuff.GetCmdPack());

        var authResp = await wrap.GetResponse();

        var authAnsBuffer = new ReadBuffer(authResp);

        sessionKey = authAnsBuffer.GetString();
        isConnected = true;
    }

    private Tuple<string, string> SplitLoginInfo(byte [] bytes)
    {
        var str = System.Text.Encoding.UTF8.GetString(bytes);
        string[] ident = str.Split('\0');
        return new Tuple<string, string>(ident[0], ident[1]);
    }

    // TODO: refactor get hash, code ToHex method, support 4.6
    private string GetAuthHash(Tuple<string, string> info, string login, string pass)
    {
        var passBytes = System.Text.Encoding.UTF8.GetBytes(pass + info.Item1);

        using(var md5 = MD5.Create())
        {
            var saltedPass = md5.ComputeHash(passBytes);
            var hexSalted = Convert.ToHexString(saltedPass).ToLower();

            var keyBytes = System.Text.Encoding.UTF8.GetBytes(hexSalted + info.Item2);

            byte[] hash = md5.ComputeHash(keyBytes);
            var hexKey = Convert.ToHexString(hash).ToLower();
            return hexKey;
        }
        
    }

    // private T[] ConcatArrays<T>(T[] x, T[] y)
    // {
    //     var concat = new T[x.Length + y.Length];
    //     Buffer.BlockCopy(x, 0, concat, 0, x.Length);
    //     Buffer.BlockCopy(y, 0, concat, x.Length, y.Length);
    //     return concat;
    // }


    public void Dispose()
    {
        isConnected = false;
        sessionKey = String.Empty;
        wrap.Dispose();
    }
}