using TSDBConnector;
public class SeriesT
{
    private string name;
    private Int64 type;
    private Int64 id;
    private string comment;
    private Int32 viewTimeMod;
    private LoopingT? loop;
    private byte dataClass;
    public SeriesT
    (
        string name,
        Int64 id,
        Int64 type,
        string comment = "",
        Int32 viewTimeMod = 0,
        LoopingT? loop = null,
        byte dataClass = 0
    ) {
        this.name = name;
        this.type = type;
        this.id = id;
        this.comment = comment;
        this.viewTimeMod = viewTimeMod;
        if (loop == null) this.loop = new LoopingT();
        this.dataClass = dataClass;
    }

    public string Name  { get { return name; } }
    public Int64 Type  { get { return type; } }
    public Int64 Id  { get { return id; } }
    public string Comment { get { return comment; } }
    public Int32 ViewTimeMod  { get { return viewTimeMod; } }
    public LoopingT Looping  { get { return loop ?? new LoopingT(); } }
    public byte Class  { get { return dataClass; } }
}