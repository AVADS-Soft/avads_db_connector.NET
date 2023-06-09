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
        public bool isConnected = false;

        public string sessionKey = String.Empty;

        private bool inReconnectState = false;

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

            await SendRequest(flow.GetCmdPack());

            var resp = await GetResponse();

            Tuple<string, string> info = SplitLoginInfo(resp);

            string hash = GetAuthHash(info, login, password);

            var authBuff = new FlowBuffer(ProtocolCmd.LoginValidPass);
            authBuff.AddString(hash);

            await SendRequest(authBuff.GetCmdPack());

            var authResp = await GetResponse();

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
            catch(SocketException)
            {
                // TryReconnect();
            }
            catch(Exception e)
            {
                throw new Exception("Error on send: " + e.Message);
            }
        }

        public async Task<byte[]> GetResponse()
        {
            var state = await wrap.ReadBytesAsync(1);
            if (state != null && state[0] == 0)
            {
                return await wrap.ReadAnswerBytes();
            }
            else
            {
                var err = await wrap.ReadError();
                throw new TsdbProtocolException(err);
            }
        }

        public async Task CheckResponseState()
        {
            var state = await wrap.ReadBytesAsync(1);
            if (state[0] != 0)
            {
                var err = await wrap.ReadError();
                throw new TsdbProtocolException(err);
            }
        }




        public void TryReconnect()
        {
            // if (inReconnectState) return;
        }

        public void Dispose()
        {
            isConnected = false;
            sessionKey = String.Empty;
            wrap.Dispose();
        }
    }
}