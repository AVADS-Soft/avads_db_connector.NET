namespace TSDBConnector
{
    public struct TsdbCredentials
    {
        public string ip;
        public int port;
        public string login;
        public string password;
        public TsdbCredentials(string ip, int port, string login, string password)
        {
            this.ip = ip;
            this.port = port;
            this.login = login;
            this.password = password;
        }
    }
    class TsdbTimeOutException : Exception
    {
        public TsdbTimeOutException(): base ("Timeout"){}
    }

    class TsdbConnectionRefused : Exception
    {
        public TsdbConnectionRefused(): base ("Connection refused"){}
    }

    class TsdbProtocolException : Exception
    {
        public TsdbProtocolException(string msg): base (msg){}
    }

    class TsdbCustomError : Exception
    {
        public TsdbCustomError(string msg): base (msg){}
    }
    class BaseIsNotOpenedException : Exception
    {
        public BaseIsNotOpenedException(): base ("Possibly base is not opened"){}
    }
}