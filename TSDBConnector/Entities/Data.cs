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
        private Int32 rowCount;
        private string startCp = "";
        private string endCp = "";

        public BoundaryT() {}
    }

    public class RowsCacheT
    {
        private byte[] cache;

        public RowsCacheT(byte[] cache)
        {
            this.cache = cache;
        }
        public byte[] Cache  { get { return cache; } }
    }    
}