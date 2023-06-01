namespace TSDBConnector;
public class LoopingT
{
    private byte type;
    private string lt;
    public LoopingT(byte type = 0, string lt = "")
    {
        this.type = type;
        this.lt = lt;
    }

    public byte Type { get { return type; } }
    public string Lt { get { return lt; } }
}