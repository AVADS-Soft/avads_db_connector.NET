using FlowBufferEnvironment;

namespace TSDBConnector
{
    public class RowT
    {
        private Int64 time;
        private byte[] value;
        private byte[] q;

        public RowT(Int64 time, byte[] value, byte[] q)
        {
            this.time = time;
            this.value = value;
            this.q = q;
        }

        public Int64 T  { get { return time; } }
        public byte[] Value  { get { return value; } }
        public byte[] Q  { get { return q; } }
    }

    public class RecsWithCP
    {
        private RowT[] recs;
        private string startCp = "";
        private string endCp = "";
        private bool hasContinuation;

        public RecsWithCP(RowT[] recs, string startCp, string endCp, bool hasContinuation)
        {
            this.recs = recs;
            this.startCp = startCp;
            this.endCp = endCp;
            this.hasContinuation = hasContinuation;
        }

        public RowT[] Recs { get { return recs; } }
    }

    public class BoundaryT
    {
        private Int64 min;
        private Int64 max;
        private Int64 rowCount;
        private string startCp = "";
        private string endCp = "";

        public BoundaryT(Int64 min, Int64 max, Int64 rowCount, string startCp, string endCp)
        {
            this.min = min;
            this.max = max;
            this.rowCount = rowCount;
            this.startCp = startCp;
            this.endCp = endCp;
        }
    }

    public class RowsCacheT
    {
        private FlowBuffer buffer;

        public RowsCacheT()
        {
            buffer = new FlowBuffer();
        }
        public byte[] Cache  { get { return buffer.GetBuffer(); } }

        public void AddRec(long seriesId, byte dataClass, long time, UInt32 quality, object value)
        {
            buffer.AddRec(seriesId, dataClass, time, quality, value);
        }
    }    
}