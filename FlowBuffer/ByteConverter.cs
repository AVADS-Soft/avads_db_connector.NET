namespace FlowBufferEnvironment
{
    public static class ByteConverter
    {
        // it always big endian in remote server
        public static byte[] Int16ToBytes(Int16 target)
        {
            var result = new byte[2];
            result[0] = (byte)(target >> 8);
            result[1] = (byte)(target);
            return result;
        }
        public static byte[] Int32ToBytes(Int32 target)
        {
            var result = new byte[4];
            result[0] = (byte)(target >> 24);
            result[1] = (byte)(target >> 16);
            result[2] = (byte)(target >> 8);
            result[3] = (byte)(target);
            return result;
        }

        public static byte[] Int64ToBytes(Int64 target)
        {
            var result = new byte[8];
            result[0] = (byte)(target >> 56);
            result[1] = (byte)(target >> 48);
            result[2] = (byte)(target >> 40);
            result[3] = (byte)(target >> 32);
            result[4] = (byte)(target >> 24);
            result[5] = (byte)(target >> 16);
            result[6] = (byte)(target >> 8);
            result[7] = (byte)(target);
            return result;
        }

        public static byte BoolToByte(bool target)
        {
            return target? (byte)1 : (byte)0;
        }

        public static byte[] FloatToBytes(float target)
        {
            Int32 raw = BitConverter.SingleToInt32Bits(target);
            return Int64ToBytes(raw);
        }

        public static byte[] DoubleToBytes(double target)
        {
            long raw = BitConverter.DoubleToInt64Bits(target);
            return Int64ToBytes(raw);
        }

        public static byte[] StringToBytes(string target)
        {
            return System.Text.Encoding.UTF8.GetBytes(target);
        }

        public static Int16 BytesToInt16(byte[] bytes)
        {
            if (System.BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }
            return System.BitConverter.ToInt16(bytes, 0);
        }

        public static Int32 BytesToInt32(byte[] bytes)
        {
            if (System.BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }
            return System.BitConverter.ToInt32(bytes, 0);
        }

        public static Int64 BytesToInt64(byte[] bytes)
        {
            if (System.BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }
            return System.BitConverter.ToInt64(bytes, 0);
        }

        public static double BytesToFloat(byte[] bytes)
        {
            if (System.BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }
            return System.BitConverter.ToSingle(bytes, 0);
        }

        public static double BytesToDouble(byte[] bytes)
        {
            if (System.BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }
            return System.BitConverter.ToDouble(bytes, 0);
        }

        public static string BytesToString(byte[] bytes)
        {
            return System.Text.Encoding.UTF8.GetString(bytes);
        }

        // Конвертация объекта в 8 байт
        public static byte[] AtomicToBytes(object value)
        {
            if (value == null) throw new ArgumentNullException();
            
            switch(value)
            {
                case byte[] bArr:
                    if (bArr.Length > 8) throw new Exception("Invalid type for atomic");
                    else return FillUpBytes(bArr);
                case Boolean b:
                {
                    byte[] bytes = new byte[8];
                    bytes[7] = BoolToByte(b);
                    return bytes;
                }
                case SByte sbte:
                {
                    byte[] bytes = new byte[8];
                    bytes[7] = (byte) sbte;
                    return bytes;
                }  
                case Int16 int16:
                    return Int64ToBytes(int16);
                case Int32 int32:
                    return Int64ToBytes(int32);
                case Int64 int64:
                    return Int64ToBytes(int64);
                case Byte bte:
                {
                    byte[] bytes = new byte[8];
                    bytes[7] = bte;
                    return bytes;
                }
                case UInt16 uint16:
                    return Int64ToBytes((Int64)uint16);
                case UInt32 uint32:
                    return Int64ToBytes((Int64)uint32);
                case UInt64 uint64:
                    return Int64ToBytes((Int64)uint64);
                case Single single:
                    return FloatToBytes(single);
                case Double dble:
                    return DoubleToBytes(dble);
                case Char ch:
                    var charBytes = StringToBytes(new String(new char[]{ ch }));
                    return FillUpBytes(charBytes);
                default:
                    throw new Exception("Invalid type for atomic");
            }
        }

        public static byte[] BlobToBytes(dynamic value)
        {
            if (value == null) throw new ArgumentNullException();
            
            switch(value)
            {
                case String str:
                    return StringToBytes(str);
                case byte[]:
                    return (byte[])value;
                default:
                    throw new Exception("Invalid type for blob"); 
            }
        }

        private static byte[] FillUpBytes(byte[] data, int arrCount = 8)
        {
            if (data.Length >= arrCount) return data;
            var fillment = new byte[arrCount];
            data.CopyTo(fillment, arrCount - data.Length);
            return fillment;
        }
    }
}