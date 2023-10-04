namespace TSDBConnector
{
    public class SeriesT
    {
        // parent base id;
        private string parentBaseName;
        private string name;
        private Int64 type;
        private Int64 id;
        private string comment;
        private Int32 viewTimeMod;
        private LoopingT? loop;
        private DataClass dataClass;
        public SeriesT
        (
            string name,
            Int64 id,
            Int64 type,
            string parentBaseName,
            string comment = "",
            Int32 viewTimeMod = 0,
            LoopingT? loop = null,
            DataClass dataClass = DataClass.Atomic
        ) {
            this.name = name;
            this.type = type;
            this.id = id;
            this.parentBaseName = parentBaseName;
            this.comment = comment;
            this.viewTimeMod = viewTimeMod;
            if (loop == null) this.loop = new LoopingT();
            this.dataClass = dataClass;
        }

        public SeriesT(SeriesT actual)
        {
            this.name = actual.name;
            this.type = actual.type;
            this.id = actual.id;
            this.parentBaseName = actual.parentBaseName;
            this.comment = actual.comment;
            this.viewTimeMod = actual.viewTimeMod;
            if (loop == null) this.loop = new LoopingT();
            this.dataClass = actual.dataClass;
        }

        public string Name  { get { return name; } }
        public Int64 Type  { get { return type; } }
        public Int64 Id  { get { return id; } }
        public string ParentBaseName { get { return parentBaseName; } }
        public string Comment { get { return comment; } }
        public Int32 ViewTimeMod  { get { return viewTimeMod; } }
        public LoopingT Looping  { get { return loop ?? new LoopingT(); } }
        public DataClass DataClass  { get { return dataClass; } }
    }
}