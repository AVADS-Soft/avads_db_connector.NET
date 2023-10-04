namespace FlowBufferEnvironment
{
    public class ReadBuffer
    {
        int offset = 0;

        byte[] buffer;

        public ReadBuffer(byte[] bytes) {
            buffer = bytes;
        }

        public byte[] GetBuffer()
        {
            return buffer;
        }

        public string GetString()
        {
            var strBytes = GetBlob();
            if (strBytes.Length == 0) return String.Empty;

            return ByteConverter.BytesToString(strBytes);
        }

        public Int64 GetInt64()
        {
            byte[] intBytes = new byte[8];
            Buffer.BlockCopy(buffer, offset, intBytes, 0, 8);
            offset += 8;
            return ByteConverter.BytesToInt64(intBytes);
        }

        public Int32 GetInt32()
        {
            byte[] intBytes = new byte[4];
            Buffer.BlockCopy(buffer, offset, intBytes, 0, 4);
            offset += 4;
            return ByteConverter.BytesToInt32(intBytes);
        }

        public Int16 GetInt16()
        {
            byte[] intBytes = new byte[2];
            Buffer.BlockCopy(buffer, offset, intBytes, 0, 2);
            offset += 2;
            return ByteConverter.BytesToInt16(intBytes);
        }

        public double GetDouble()
        {
            byte[] doubleBytes = new byte[8];
            Buffer.BlockCopy(buffer, offset, doubleBytes, 0, 8);
            offset += 8;
            return ByteConverter.BytesToDouble(doubleBytes);
        }

        public byte GetByte()
        {
            byte[] bytes = new byte[1];
            Buffer.BlockCopy(buffer, offset, bytes, 0, 1);
            offset += 1;
            return bytes[0];
        }

        public byte[] GetBytes(int length)
        {
            byte[] bytes = new byte[length];
            Buffer.BlockCopy(buffer, offset, bytes, 0, length);
            offset += length;
            return bytes;
        }

        public byte[] GetBlob()
        {
            var empty = new byte[0];
            if (buffer.Length < offset + 4)
            {
                return empty;
            }
            var lenInBytes = new byte[4];
            Buffer.BlockCopy(buffer, offset, lenInBytes, 0, 4);
            Int32 strLength = ByteConverter.BytesToInt32(lenInBytes);
            offset += 4;

            if (strLength == 0)
            {
                return empty;
            }
            if (buffer.Length < offset + strLength)
            {
                return empty;
            }
            var blob = new byte[strLength];
            Buffer.BlockCopy(buffer, offset, blob, 0, strLength);
            offset += blob.Length;

            if (blob.Length == 1 && blob[0] == 0)
            {
                return empty;
            }

            return blob;
        }

        public bool GetBool()
        {
            byte[] bytes = new byte[1];
            Buffer.BlockCopy(buffer, offset, bytes, 0, 1);
            offset += 1;
            return BitConverter.ToBoolean(bytes, 0);
        }

        public void GetMap()
        {

        }
    }
}