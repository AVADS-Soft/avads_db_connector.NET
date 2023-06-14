using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FlowBufferEnvironment;
using TSDBConnector;

namespace FlowBufferTest
{
    [TestClass]
    public class BufferTests
    {
        [TestMethod]
        public void Int16PackDepack()
        {
            var buff = new FlowBuffer((byte)CmdType.BaseCreate);
            var numbers = new Int16[] {12, 12345, -1010, -5432, 0 , -12345, Int16.MaxValue, Int16.MinValue};
            foreach(var num in numbers)
            {
                buff.AddInt16(num);
            }
            var nextBuff = new ReadBuffer(CutCmdByte(buff.GetCmdPack()));
            var extracted = new Int16[numbers.Length];
            for (int i = 0; i < numbers.Length; i++)
            {
                extracted[i] = nextBuff.GetInt16();
            }
            CollectionAssert.AreEqual(numbers, extracted);
        }

        [TestMethod]
        public void Int32PackDepack()
        {
            var buff = new FlowBuffer((byte)CmdType.BaseCreate);
            var numbers = new Int32[] {12, 44444, -100, 5021231, 0 , -1234567890, Int32.MaxValue, Int32.MinValue};
            foreach(var num in numbers)
            {
                buff.AddInt32(num);
            }
            var nextBuff = new ReadBuffer(CutCmdByte(buff.GetCmdPack()));
            var extracted = new Int32[numbers.Length];
            for (int i = 0; i < numbers.Length; i++)
            {
                extracted[i] = nextBuff.GetInt32();
            }
            CollectionAssert.AreEqual(numbers, extracted);
        }

        [TestMethod]
        public void Int64PackDepack()
        {
            var numbers = new Int64[] {12, 32767, -100, -32768, -1234511111111111, Int64.MaxValue, Int64.MinValue};
            var buff = new FlowBuffer((byte)CmdType.BaseCreate);
            foreach(var num in numbers)
            {
                buff.AddInt64(num);
            }
            var nextBuff = new ReadBuffer(CutCmdByte(buff.GetCmdPack()));
            var extracted = new Int64[numbers.Length];
            for (int i = 0; i < numbers.Length; i++)
            {
                extracted[i] = nextBuff.GetInt64();
            }
            CollectionAssert.AreEqual(numbers, extracted);

        }

        [TestMethod]
        public void DoublePackDepack()
        {
            var numbers = new Double[] {12.222, 32767.000001, -100.0, -32768.123123123, -1234511111111111.00000000001, Double.MaxValue, Double.MinValue};
            var buff = new FlowBuffer((byte)CmdType.BaseCreate);
            foreach(var num in numbers)
            {
                buff.AddDouble(num);
            }
            var nextBuff = new ReadBuffer(CutCmdByte(buff.GetCmdPack()));
            var extracted = new Double[numbers.Length];
            for (int i = 0; i < numbers.Length; i++)
            {
                extracted[i] = nextBuff.GetDouble();
            }
            CollectionAssert.AreEqual(numbers, extracted);
        }

        [TestMethod]
        public void BoolPackDepack()
        {
            var buff = new FlowBuffer((byte)ProtocolCmd.GetProtocolVersion);
            buff.AddBool(true);
            buff.AddBool(false);
            buff.AddBool(true);
            var pack = new ReadBuffer(CutCmdByte(buff.GetCmdPack()));
            Assert.AreEqual(true, pack.GetBool());
            Assert.AreEqual(false, pack.GetBool());
            Assert.AreEqual(true, pack.GetBool());
        }

        [TestMethod]
        public void StringPackDepack()
        {
            var buff = new FlowBuffer((byte)CmdType.BaseCreate);
            var strs = new string[] {"Kekekek", "qqq0112sld,al", "maw21../123=-1", "", "222value", "", ""};
            foreach(var str in strs)
            {
                buff.AddString(str);
            }
            var nextBuff = new ReadBuffer(CutCmdByte(buff.GetCmdPack()));
            var extracted = new string[strs.Length];
            for (int i = 0; i < strs.Length; i++)
            {
                extracted[i] = nextBuff.GetString();
            }
            CollectionAssert.AreEqual(strs, extracted);
        }

        private byte[] CutCmdByte(byte[] input)
        {
            var newLen = input.Length - 1;
            var newArr = new byte[newLen];
            Array.Copy(input, 1, newArr, 0, newLen);
            return newArr;
        }
    }
}
