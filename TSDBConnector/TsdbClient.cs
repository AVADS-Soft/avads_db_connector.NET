using System;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Threading.Tasks;
using FlowBufferEnvironment;

namespace TSDBConnector
{
    public class TsdbClient : IDisposable
    {    
        private TcpWrapper wrap;

        // TODO: add getters/setters
        private long version;
        public long Version { get => version; }
        private bool isConnected = false;

        public bool IsConnected { get => isConnected; }

        public string sessionKey = String.Empty;

        //private bool inReconnectState = false;

        public TsdbClient()
        {
            wrap = new TcpWrapper();
        }

        private string host = "";
        private int port;
        private string login = "";
        private string password = "";

        public async Task CreateConnection(string host, int port, string login, string password)
        {
            await wrap.InitConnection(host, port);

            this.host = host;
            this.port = port;
            this.login = login;
            this.password = password;

            version = await GetVersion();
            await Login(login, password);
        }   

        // TODO: refactor login
        private async Task Login(string login, string password)
        {
            var flow = new FlowBuffer(ProtocolCmd.LoginGetKeys);

            flow.AddString(login);

            //await SendRequest(flow.GetCmdPack());
            //var resp = await GetResponse();

            var resp = await Fetch(flow.GetCmdPack());

            Tuple<string, string> info = SplitLoginInfo(resp.GetBuffer());

            string hash = GetAuthHash(info, login, password);

            var authBuff = new FlowBuffer(ProtocolCmd.LoginValidPass);
            authBuff.AddString(hash);

            await SendRequest(authBuff.GetCmdPack());

            var authResp = await GetResponse();

            //var authResp = await Fetch(authBuff.GetCmdPack());

            var authAnsBuffer = new ReadBuffer(authResp);

            sessionKey = authAnsBuffer.GetString();
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

        // TODO: move get version
        public async Task<long> GetVersion()
        {
            var getVersBuff = new FlowBuffer(ProtocolCmd.GetProtocolVersion);
            await wrap.WriteBytesAsync(getVersBuff.GetCmdPack());
            var bytes = await GetResponse();
            var vers = (long)bytes[0];
            return vers;
        }

        public async Task SendRequest(byte[] bytes)
        {
            try
            {
                await wrap.WriteBytesAsync(bytes);
            }
            catch(Exception e)
            {
                throw new Exception("Error on send: " + e.Message);
            }
        }

        public async Task<byte[]> GetResponse()
        {
            try
            {
                var state = await wrap.ReadBytesAsync(1);
                if (state != null && state[0] == 0)
                {
                    return await wrap.ReadAnswerBytes();
                }
                else
                {
                    var err = await wrap.ReadError();
                    throw new TsdbCustomError(err);
                }
            }
            catch(TsdbProtocolException)
            {
                throw;
            }
            catch(Exception e)
            {
                throw new Exception("Error on send: " + e.Message);
            }
        }

        public async Task CheckResponseState()
        {
            var state = await wrap.ReadBytesAsync(1);
            if (state[0] != 0)
            {
                var err = await wrap.ReadError();
                throw new TsdbCustomError(err);
            }
        }

        public async Task<ReadBuffer> Fetch(byte[] request, byte type = 0)
        {
            try
            {
                await wrap.WriteBytesAsync(request);
                var state = await wrap.ReadBytesAsync(1);
                if (state != null && state[0] == 0)
                {
                    if (type == 0)
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
            catch (Exception ex)
            {
                switch (ex)
                {
                    case IOException:
                    case SocketException:
                    case TsdbTimeOutException:
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

        private int AttemptsCount = -1;

        private int AttemptsInterval = 1000;

        private int currentAttempt = 0;

        public async Task Reconnect()
        {
            if (isConnected) return;
            if (String.IsNullOrEmpty(sessionKey)) throw new Exception("Unable to reconnect without session key");            
            
            //inReconnectState = true;
            var inReconnectAttempt = true;
            var restorePack = new FlowBuffer(ProtocolCmd.RestoreSession);
                restorePack.AddString(sessionKey);
            while (inReconnectAttempt)
            {
                try
                {
                    if (AttemptsCount != -1 && currentAttempt > AttemptsCount)
                    {
                        throw new Exception("Reconnect failed");
                    }
                    currentAttempt ++;
                    
                    // TODO: create options struct, and pass it in constructor
                    await wrap.InitConnection(host, port);
                    await wrap.WriteBytesAsync(restorePack.GetPackWithPayload());
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
                            await Task.Delay(AttemptsInterval);
                            continue;
                        default:
                            throw;
                    }
                }
                
            }

        }

        private async Task RestoreState()
        {
            await Login(login, password);
            // then reopen bases
        }

        public void Dispose()
        {
            isConnected = false;
            sessionKey = String.Empty;
            wrap.Dispose();
        }
    }
}