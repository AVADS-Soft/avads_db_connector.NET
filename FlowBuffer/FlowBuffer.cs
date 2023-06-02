﻿using System;
using System.Collections.Generic;

namespace FlowBufferEnvironment
{
    public class FlowBuffer
    {
        List<byte> buffer = new List<byte>();
        byte type; 
        public FlowBuffer(CmdType cmd)
        {
            type = (byte)cmd;
        }

        public FlowBuffer(ProtocolCmd cmd)
        {
            type = (byte)cmd;
        }


        public byte[] GetCmdPack()
        {   
            var buffBytes = buffer.ToArray();
            var buffLength = buffBytes.Length;

            byte[] pack = new byte[buffLength + 1];

            pack[0] = type;

            Buffer.BlockCopy(buffBytes, 0, pack, 1, buffLength);

            return pack;
        }


        public byte[] GetPackWithPayload()
        {
            byte[] pack;
            var buffLen = buffer.Count;
            if (buffLen > 0) {
                var total = 5 + buffLen;
                pack = new byte[total];
                var lenInBytes = ByteConverter.Int32ToBytes(buffLen);
                Buffer.BlockCopy(lenInBytes, 0, pack, 1, lenInBytes.Length);
                var buffBytes = buffer.ToArray();
                Buffer.BlockCopy(buffBytes, 0, pack, 5, buffBytes.Length);
            }
            else
            {
                pack = new byte[1];
            }
            pack[0] = type;

            return pack;
        }

        public void AddString(string value)
        {
            Int32 len;
            byte[] stringInBytes;
            if (string.IsNullOrEmpty(value))
            {
                len = 1;
                stringInBytes = new byte[]{0};
            }
            else
            {
                stringInBytes = System.Text.Encoding.UTF8.GetBytes(value);
                len = stringInBytes.Length;
            }
            var lenInBytes = ByteConverter.Int32ToBytes(len);

            buffer.AddRange(lenInBytes);
            buffer.AddRange(stringInBytes);
        }

        public void AddInt64(Int64 value)
        {
            var bytes = ByteConverter.Int64ToBytes(value);
            buffer.AddRange(bytes);
        }

        public void AddInt32(Int32 value)
        {
            var bytes = ByteConverter.Int32ToBytes(value);
            buffer.AddRange(bytes);
        }

        public void AddInt16(Int16 value)
        {
            var bytes = ByteConverter.Int16ToBytes(value);
            buffer.AddRange(bytes);
        }

        public void AddDouble(double value)
        {
            var bytes = ByteConverter.DoubleToBytes(value);
            buffer.AddRange(bytes);
        }

        public void AddByte(byte value)
        {
            buffer.Add(value);
        }

        public void AddBytes(byte[] value)
        {
            buffer.AddRange(value);
        }

        public void AddBytesWithLength(byte[] value)
        {
            var lengthBytes = ByteConverter.Int32ToBytes(value.Length);
            buffer.AddRange(lengthBytes);
            buffer.AddRange(value);
        }

        public void AddBool(bool value)
        {
            byte bVal = value? (byte)1 : (byte)0;
            buffer.Add(bVal);
        }

        public void AddMap()
        {

        }
    }

    public class ReadBuffer
    {
        int offset = 0;

        byte[] buffer;

        public ReadBuffer(byte[] bytes) {
            buffer = bytes;
        }

        public string GetString()
        {
            if (buffer.Length < offset + 4)
            {
                return string.Empty;
            }
            var lenInBytes = new byte[4];
            Buffer.BlockCopy(buffer, offset, lenInBytes, 0, 4);
            Int32 strLength = ByteConverter.BytesToInt32(lenInBytes);
            offset += 4;

            if (strLength == 0)
            {
                return string.Empty;
            }
            if (buffer.Length < offset + strLength)
            {
                return string.Empty;
            }
            var strInBytes = new byte[strLength];
            Buffer.BlockCopy(buffer, offset, strInBytes, 0, strLength);
            offset += strInBytes.Length;

            if (strInBytes.Length == 1 && strInBytes[0] == 0)
            {
                return string.Empty;
            }

            var str = System.Text.Encoding.UTF8.GetString(strInBytes);

            return str;
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

    public static class ByteConverter
    {
        // TODO: order bytes with endians

        // lil endian here
        // and lil end in tests


        // big endian on remote server


        // big endian hardcoded
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

        public static byte[] DoubleToBytes(double target)
        {
            long raw = BitConverter.DoubleToInt64Bits(target);
            return Int64ToBytes(raw);
        }

        public static Int16 BytesToInt16(byte[] bytes)
        {
            if (System.BitConverter.IsLittleEndian) {
                Array.Reverse(bytes);
            }
            return System.BitConverter.ToInt16(bytes, 0);
        }

        public static Int32 BytesToInt32(byte[] bytes)
        {
            if (System.BitConverter.IsLittleEndian) {
                Array.Reverse(bytes);
            }
            return System.BitConverter.ToInt32(bytes, 0);
        }

        public static Int64 BytesToInt64(byte[] bytes)
        {
            if (System.BitConverter.IsLittleEndian) {
                Array.Reverse(bytes);
            }
            return System.BitConverter.ToInt64(bytes, 0);
        }

        public static double BytesToDouble(byte[] bytes)
        {
            if (System.BitConverter.IsLittleEndian) {
                Array.Reverse(bytes);
            }
            return System.BitConverter.ToDouble(bytes, 0);
        }
    }

    public enum BufferMapValueType {
        String,
        Int,
        Float,
        Bool,
        Byte,
        Map,
        Bytes
    }

    public enum CmdType {
        Disconnect            = 0,
    	BaseCreate            = 1,
    	BaseOpen              = 2,
    	BaseGetInfo           = 3,
    	BaseGetList           = 4,
    	BaseRemove            = 5,
    	BaseUpdate            = 6,
    	BaseClose             = 7,
    	SeriesCreate          = 8,
    	SeriesRemove          = 9,
    	SeriesUpdate          = 10,
    	SeriesGetAll          = 11,
    	SeriesGetInfo         = 12,
    	UserGetList           = 13,
    	UserGetInfo           = 14,
    	UserCreate            = 15,
    	UserRemove            = 16,
    	UserUpdate            = 17,
    	PropsGetList          = 18,
    	PropsGetInfo          = 19,
    	PropsSet              = 20,
    	DataGetBoundary       = 21,
    	DataGetCP             = 22,
    	DataGetFromCP         = 23,
    	DateGetRangeFromCP    = 24,
    	DateGetRangeDirection = 25,
    	DataAddRow            = 26,
    	DataDeleteRow         = 27,
    	DataDeleteRows        = 28,
    	DataAddRowCache       = 29,
    	DataGetValueAtTime    = 30,
    	DataMathFunc          = 31,
    	DataAddRows           = 32,
    	DataGetLastValue      = 33,
    }

    public enum ProtocolCmd {
        LoginGetKeys       = 0,
    	LoginValidPass     = 1,
    	RestoreSession     = 2,
    	GetProtocolVersion = 254
    }
}