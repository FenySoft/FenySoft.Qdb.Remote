using FenySoft.Core.IO;
using FenySoft.Qdb.Remote;
using FenySoft.Qdb.Storage;
using FenySoft.Qdb.WaterfallTree;
using FenySoft.Remote;

namespace FenySoft.Qdb.Database
{
    public static class TRemoteQdb
    {
        public static ITStorageEngine FromNetwork(string host, int port = 7182)
        {
            return new TStorageEngineClient(host, port);
        }

        public static TStorageEngineServer CreateServer(ITStorageEngine engine, int port = 7182)
        {
            TTcpServer server = new TTcpServer(port);
            TStorageEngineServer engineServer = new TStorageEngineServer(engine, server);

            return engineServer;
        }
    }
}