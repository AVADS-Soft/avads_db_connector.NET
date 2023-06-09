﻿using System;
using System.Net.Sockets;
using System.Threading.Tasks;
using FlowBufferEnvironment;

namespace TSDBConnector
{
    public class TcpWrapper : IDisposable
    {
        private NetworkStream? stream;
        private int awaitTick = 10;        
        private bool useTimeout = true;
        private int timeout = 5000;

        public int Timeout
        {
            get => timeout;
            set => timeout = value;
        }

        public async Task InitConnection(string ip, int port)
        {
            TcpClient client = new();
            try
            {
                await client.ConnectAsync(ip, port);
                stream = client.GetStream();
            } 
            catch (Exception)
            {
                throw new TsdbConnectionRefused();
            }
        }

        public void CloseConnection()
        {
            if(stream != null)
            {
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
                catch (SocketException)
                {
                    throw;
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
            else throw new Exception("Cannot write");
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

class TsdbConnectionRefused : Exception
{
    public TsdbConnectionRefused(): base ("Connection refused"){}
}

class TsdbProtocolException : Exception
{
    public TsdbProtocolException() {}
}

class TsdbCustomError : Exception
{
    public TsdbCustomError(string msg): base (msg){}
}