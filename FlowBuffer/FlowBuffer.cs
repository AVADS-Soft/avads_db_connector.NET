namespace FlowBufferEnvironment
{
    public class FlowBuffer
    {
        List<byte> buffer = new List<byte>();
        private byte type;

        public byte Type { get { return type;} set {type = value;} }

        public FlowBuffer()
        {
        }

        public FlowBuffer(byte cmd)
        {
            type = cmd;
        }

        public byte[] GetBuffer()
        {
            return buffer.ToArray();
        }

        public byte[] GetCmdPack()
        {   
            buffer.Insert(0, type);
            return buffer.ToArray();
        }


        public byte[] GetPayloadPack()
        {
            var buffLen = buffer.Count;
            buffer.Insert(0, type);
            if (buffLen > 0)
            {
                var lenInBytes = ByteConverter.Int32ToBytes(buffLen);
                buffer.InsertRange(1, lenInBytes);
            }
            return buffer.ToArray();
        }

        public FlowBuffer AddString(string value)
        {
            Int32 len;
            byte[] stringInBytes = new byte[0];
            if (!string.IsNullOrEmpty(value))
            {
                stringInBytes = ByteConverter.StringToBytes(value);
            }
            len = stringInBytes.Length;
            var lenInBytes = ByteConverter.Int32ToBytes(len);

            buffer.AddRange(lenInBytes);
            buffer.AddRange(stringInBytes);
            return this;
        }

        public FlowBuffer AddInt64(Int64 value)
        {
            var bytes = ByteConverter.Int64ToBytes(value);
            buffer.AddRange(bytes);
            return this;
        }

        public FlowBuffer AddInt32(Int32 value)
        {
            var bytes = ByteConverter.Int32ToBytes(value);
            buffer.AddRange(bytes);
            return this;
        }

        public FlowBuffer AddInt16(Int16 value)
        {
            var bytes = ByteConverter.Int16ToBytes(value);
            buffer.AddRange(bytes);
            return this;
        }

        public FlowBuffer AddDouble(double value)
        {
            var bytes = ByteConverter.DoubleToBytes(value);
            buffer.AddRange(bytes);
            return this;
        }

        public FlowBuffer AddByte(byte value)
        {
            buffer.Add(value);
            return this;
        }

        public FlowBuffer AddBytes(byte[] value)
        {
            buffer.AddRange(value);
            return this;
        }

        public FlowBuffer AddBool(bool value)
        {
            buffer.Add(ByteConverter.BoolToByte(value));
            return this;
        }

        // TODO: create add map method
        public void AddMap()
        {

        }
    }    

    public enum BufferMapValueType
    {
        String,
        Int,
        Float,
        Bool,
        Byte,
        Map,
        Bytes
    }
}