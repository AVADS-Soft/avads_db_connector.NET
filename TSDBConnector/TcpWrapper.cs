using System;
using System.Net.Sockets;
using System.Threading.Tasks;
using FlowBufferEnvironment;

namespace TSDBConnector
{
    public class TcpWrapper : IDisposable
    {
        private long version;
        public long Version { get => version; }
        private NetworkStream? stream;
        private int awaitTick = 10;        
        private bool useTimeout = true;
        private int timeout = 5000;

        public int Timeout
        {
            get => timeout;
            set => timeout = value;
        }

        private bool isConnected = false;

        public bool IsConnected { get => isConnected; }



        public async Task InitConnection(string ip, int port)
        {
            TcpClient client = new();
            try
            {
                await client.ConnectAsync(ip, port);
                stream = client.GetStream();
                //version = await GetVersion();
                isConnected = true;
            } 
            catch (Exception e)
            {
                // TODO: create special exceptions for various cases
                throw new Exception("Connection init fail: " + e.Message);
            }
        }

        public void CloseConnection()
        {
            if(stream != null)
            {
                isConnected = false;
                stream.Close();
            }
        }

        public async Task<byte[]> ReadBytesAsync(int count)
        {
            var timeout = Timeout;
            if (stream == null)
            {
                throw new Exception("Connection is not inited");
            }
            while(!stream.DataAvailable)
            {
                timeout -= awaitTick;
                await Task.Delay(awaitTick);
                if (useTimeout && timeout < 0)
                {
                    throw new TsdbTimeOutException();
                }
            }
            if (stream.CanRead && stream.DataAvailable)
            {
                try
                {
                    var bytes = new byte[count];
                    await stream.ReadAsync(bytes);
                    return bytes;
                }
                catch (SocketException e)
                {
                    throw e;
                }
                catch (Exception e)
                {
                    throw new Exception("Error on read" + e.Message);
                }
            }
            throw new Exception("Cannot read");
        }

        public async Task WriteBytesAsync(byte[] bytes)
        {
            if (stream == null)
            {
                throw new Exception("Connection is not inited");
            }
            if (stream.CanWrite)
            {
                await stream.WriteAsync(bytes, 0, bytes.Length);
            }
        }

        public async Task<byte[]> ReadAnswerBytes()
        {
            var size = await GetPacketLength();
            return await ReadBytesAsync(size);
        }

        public async Task<string> ReadError()
        {
            var bytes = await ReadAnswerBytes();
            var buff = new ReadBuffer(bytes);
            return buff.GetString();
        }

        private async Task<int> GetPacketLength()
        {
            var packLenBytes = await ReadBytesAsync(4);
            return ByteConverter.BytesToInt32(packLenBytes);
        }



        public void Dispose()
        {
            stream?.Dispose();
        }
    }
}


class TsdbTimeOutException : Exception
{
    public TsdbTimeOutException(): base ("Timeout"){}
}

class TsdbProtocolException : Exception
{
    public TsdbProtocolException(string msg): base (msg){}
}