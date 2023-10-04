namespace TSDBConnector
{
    public enum ProtocolCmd : byte
    {
        LoginGetKeys       = 0,
    	LoginValidPass     = 1,
    	RestoreSession     = 2,
    	GetProtocolVersion = 254
    }
    public enum CmdType : byte
    {
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
        GetSeriesById         = 34
    }

    public enum ResponseType
    {
        Payload = 0,
        State = 1,
    }

    public enum SeekDirection : byte
    {
        ToMax = 1,
        ToMin = 2,
    }

    public enum DataClass : byte
    {
        Atomic = 0,
        Blob = 1
    }

    public enum MecTypes
    {
        BOOL = 0,
        SINT = 1,
        INT  = 2,
        DINT = 3,
        LINT = 4,
        USINT = 5,
        UINT = 6,
        UDINT = 7,
        ULINT = 8,
        REAL = 9,
        LREAL = 10,
        TIME = 11,
        LTIME = 12,
        DATE = 13,
        LDATE = 14,
        TIME_OF_DAY = 15,
        LTIME_OF_DAY = 16,
        DATE_AND_TIME = 17,
        LDATE_AND_TIME = 18,
        STRING = 19,
        WSTRING = 20,
        CHAR = 21,
        WCHAR = 22, 
        BYTE = 23,
        WORD = 24,
        DWORD = 25,
        LWORD = 26,
        SANY = 27,
        BANY = 28,
        TOD  = TIME_OF_DAY,
        LTOD = LTIME_OF_DAY,
        DT   = DATE_AND_TIME,
        LDT  = LDATE_AND_TIME,
    }
}