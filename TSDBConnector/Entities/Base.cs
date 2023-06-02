using System;

namespace TSDBConnector
{
    public class BaseT
    {
        private string name;
        private string path;
        private string dbSize;
        private string comment;
        private string fsType; 
        private LoopingT? loop;
        private bool autoAddSeries;
        private bool autoSave;
        private string autoSaveDuration;
        private string autoSaveInterval;

        private Int64 status = 0;

        public BaseT
        (
            string name,
            string path,
            string dbSize,
            string fsType = "",
            string comment = "",
            LoopingT? loop = null,        
            bool autoAddSeries = true,
            bool autoSave = false,
            string autoSaveDuration = "",
            string autoSaveInterval = "",
            Int64 status = 0
        ) {
            this.name = name;
            this.path = path;
            this.dbSize = dbSize;
            this.comment = comment;
            this.fsType = fsType == "" ? FsTypes.FS_MULTIPART : fsType;
            if (loop == null) this.loop = new LoopingT();
            this.autoAddSeries = autoAddSeries;
            this.autoSave = autoSave;
            this.autoSaveDuration = autoSaveDuration;
            this.autoSaveInterval = autoSaveInterval;
            this.status = status;
        }

        public string Name  { get { return name; } }
        public string Comment { get { return comment; } }
        public string Path { get { return path; } }
        public Int64 Status{ get { return status; } set { status = value; } }
        public string FsType { get { return fsType; } }
        public string DbSize { get { return dbSize; } }
        public LoopingT Looping { get { return loop ?? new LoopingT(); } }
        public bool AutoAddSeries { get { return autoAddSeries; } }
        public bool AutoSave { get { return autoSave; } }
        public string AutoSaveDuration { get { return autoSaveDuration; } }
        public string AutoSaveInterval { get { return autoSaveInterval; } }
    }

    public static class  FsTypes
    {
        public static readonly string FS_FS = "fs";
        public static readonly string FS_MULTIPART = "fs_mp";
        public static readonly string FS_MEMORY = "mem_fs";
    }
}
