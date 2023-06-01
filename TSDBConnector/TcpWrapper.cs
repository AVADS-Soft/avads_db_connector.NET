using System.Net.Sockets;
using FlowBufferEnvironment;

namespace TSDBConnector;
public class TcpWrapper : IDisposable
{
    public long Version;
    private NetworkStream? stream;

    private int awaitTick = 10;
    private int timeout = 5000;

    private bool useTimeout = false;
    public int Timeout
    {
        get => timeout;
        set => timeout = value;
    }

    public async Task InitConnection(string ip, int port)
    {
        TcpClient client = new();
        client.SendTimeout = client.ReceiveTimeout = Timeout;
        try
        {
            await client.ConnectAsync(ip, port);
            stream = client.GetStream();
            Version = await GetVersion();
        } 
        catch(Exception e)
        {
            // TODO: create special exceptions for various cases
            throw new Exception("Connection init fail: " + e.Message);
        }
    }

    public void CloseConnection()
    {
        if(stream != null)
        {
            stream.Close();
        }
    }


    private async Task<byte[]> ReadBytesAsync(int count)
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
                throw new Exception("Time out");
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
            catch (Exception e)
            {
                throw new Exception("Error on read" + e.Message);
            }
        }
        throw new Exception("Cannot read");
    }

    private async Task WriteAsync(byte[] bytes)
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

    public async Task SendRequest(byte[] bytes)
    {
        try
        {
            await WriteAsync(bytes);
        }
        catch(Exception e)
        {
            throw new Exception("Error on send: " + e.Message);
        }
        
    }

    public async Task CheckResponseState()
    {
        var state = await ReadBytesAsync(1);

        if (state[0] != 0)
        {
            var err = await ReadError();
            throw new Exception(err);
        }
    }

    public async Task<byte[]> GetResponse()
    {
        var state = await ReadBytesAsync(1);

        if (state != null && state[0] == 0)
        {
            return await ReadAnswerBytes();
        }
        else
        {
            var err = await ReadError();
            throw new Exception(err);
        }
    }

    private async Task<byte[]> ReadAnswerBytes()
    {
        var size = await GetPacketLength();

        return await ReadBytesAsync(size);
    }

    private async Task<string> ReadError()
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

    public async Task<long> GetVersion()
    {
        var getVersBuff = new FlowBuffer(ProtocolCmd.GetProtocolVersion);

        await WriteAsync(getVersBuff.GetCmdPack());

        var bytes = await GetResponse();

        var vers = (long)bytes[0];

        return vers;

    }

    public void Dispose()
    {
        stream?.Dispose();
    }
}