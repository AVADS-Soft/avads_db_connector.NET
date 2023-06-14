using System;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Threading.Tasks;
using FlowBufferEnvironment;

namespace TSDBConnector
{
    public class TsdbClient : IDisposable
    {
        private TsdbCredentials credentials;
        private TcpWrapper wrap;
        private long version;
        private bool isConnected = false;
        private string sessionKey = String.Empty;
        private int reconnectAttemptsCount = -1;
        private int reconnectAttemptsInterval = 1000;
        private int currentAttempt = 0;
        private bool inReconnectNow = false;
        public long Version { get => version; }
        public bool IsConnected { get => isConnected; }
        public string SessionKey { get => sessionKey; }
        public int ReconnectAttemptsCount { get => reconnectAttemptsCount; set => reconnectAttemptsCount = value; }
        public int ReconnectAttemptsInterval { get => reconnectAttemptsInterval; set => reconnectAttemptsInterval = value; }
        public Dictionary<string, long> TryOpenBases = new();
        public Dictionary<string, long> OpenedBases = new();
        public TsdbClient(TsdbCredentials credentials)
        {
            this.credentials = credentials;
            wrap = new TcpWrapper();
        }

        public async Task Init()
        {
            await wrap.InitConnection(credentials.ip, credentials.port);
            version = await GetVersion();
            await Login(credentials.login, credentials.password);
        }   

        private async Task Login(string login, string password)
        {
            sessionKey = String.Empty;
            var keysPack = new FlowBuffer(ProtocolCmd.LoginGetKeys).AddString(login).GetCmdPack();

            var resp = await SocketWriteRead(keysPack);

            Tuple<string, string> info = SplitLoginInfo(resp.GetBuffer());
            string hash = GetAuthHash(info, login, password);

            var authPack = new FlowBuffer(ProtocolCmd.LoginValidPass).AddString(hash).GetCmdPack();

            var authBuff = await SocketWriteRead(authPack);

            sessionKey = authBuff.GetString();
            isConnected = true;
        }

        private Tuple<string, string> SplitLoginInfo(byte [] bytes)
        {
            var str = ByteConverter.BytesToString(bytes);
            string[] ident = str.Split('\0');
            return new Tuple<string, string>(ident[0], ident[1]);
        }

        private string GetAuthHash(Tuple<string, string> info, string login, string pass)
        {
            var passBytes = ByteConverter.StringToBytes(pass + info.Item1);
            using(var md5 = MD5.Create())
            {
                var saltedPass = md5.ComputeHash(passBytes);
                var hexSalted = Convert.ToHexString(saltedPass).ToLower();
                var keyBytes = ByteConverter.StringToBytes(hexSalted + info.Item2);
                byte[] hash = md5.ComputeHash(keyBytes);
                var hexKey = Convert.ToHexString(hash).ToLower();
                return hexKey;
            }
        }

        private async Task<long> GetVersion()
        {
            var pack = new FlowBuffer(ProtocolCmd.GetProtocolVersion).GetCmdPack();
            var buff = await Fetch(pack);
            var vers = (long)buff.GetByte();
            return vers;
        }

        public async Task<ReadBuffer> Fetch(byte[] request, ResponseType type = ResponseType.Payload)
        {
            try
            {
                return await SocketWriteRead(request, type);
            }
            catch (Exception ex)
            {
                switch (ex)
                {
                    case SocketException:
                    case TsdbTimeOutException:
                    case TsdbProtocolException:
                        isConnected = false;
                        await Reconnect();
                        await RestoreState();
                        return await Fetch(request, type);
                    case TsdbCustomError:
                        throw;
                    default:
                        throw new Exception("fetch error: " + ex.Message);
                }
            }
        }

        private async Task<ReadBuffer> SocketWriteRead(byte[] request, ResponseType type = ResponseType.Payload)
        {
            await wrap.WriteBytesAsync(request);
            var state = await wrap.ReadBytesAsync(1);
            if (state != null && state[0] == 0)
            {
                if (type == ResponseType.Payload)
                { 
                    var response = await wrap.ReadAnswerBytes();
                    return new ReadBuffer(response);
                }
                else return new ReadBuffer(state);
            }
            else
            {
                var err = await wrap.ReadError();
                throw new TsdbCustomError(err);
            }
        }


        public async Task Reconnect()
        {            
            var inReconnectAttempt = true;
            var restorePack = new FlowBuffer(ProtocolCmd.RestoreSession).AddString(sessionKey).GetCmdPack();
            while (inReconnectAttempt)
            {
                try
                {
                    if (reconnectAttemptsCount != -1 && currentAttempt > reconnectAttemptsCount)
                    {
                        throw new Exception("Reconnect failed");
                    }
                    currentAttempt ++;

                    await wrap.InitConnection(credentials.ip, credentials.port);
                    if (String.IsNullOrEmpty(sessionKey)) {
                        await Init();
                    } else {
                        await SocketWriteRead(restorePack, ResponseType.State);
                        isConnected = true;
                    }
                    inReconnectAttempt = false;
                }
                catch(Exception ex)
                {
                    switch (ex)
                    {
                        case IOException:
                        case SocketException:
                        case TsdbTimeOutException:
                        case TsdbConnectionRefused:
                            await Task.Delay(reconnectAttemptsInterval);
                            continue;
                        case TsdbCustomError:
                            if (ex.Message.StartsWith("#17001")) {
                                await Init();
                                inReconnectAttempt = false;
                                break;
                            } else throw;
                        default:
                            throw;
                    }
                }
            }
        }

        private async Task RestoreState()
        {
            // TODO: create restore method
            // TODO: move opend bases to client class
            var formerState = OpenedBases;

            this.OpenedBases = new Dictionary<string, long>();
            this.TryOpenBases = new Dictionary<string, long>();
            foreach (KeyValuePair<string, long> entry in formerState)
            {
                await this.OpenBase(entry.Key);
            }
        }

        public long GetTempBaseId(string baseName)
        {
            long id = -1;
            OpenedBases.TryGetValue(baseName, out id);
            return id;
        }

        public void CloseConnection()
        {
            isConnected = false;
            wrap.CloseConnection();
            wrap.Dispose();
        }

        public void Dispose()
        {
            isConnected = false;
            sessionKey = String.Empty;
            wrap.CloseConnection();
            wrap.Dispose();
        }
    }
}

public struct TsdbCredentials
{
    public string ip;
    public int port;
    public string login;
    public string password;
    public TsdbCredentials(string ip, int port, string login, string password)
    {
        this.ip = ip;
        this.port = port;
        this.login = login;
        this.password = password;
    }
}

public enum ResponseType
{
    Payload = 0,
    State = 1,
}
