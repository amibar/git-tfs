using System;

namespace Sep.Git.Tfs.Core.TfsInterop
{
    [Flags]
    public enum TfsChangeType
    {
        None = 0x1,
        Add = 0x2,
        Edit = 0x4,
        Encoding = 0x8,
        Rename = 0x10,
        Delete = 0x20,
        Undelete = 0x40,
        Branch = 0x80,
        Merge = 0x100,
        Lock = 0x200,
        Rollback = 0x400,
        SourceRename = 0x800,
        Property = 0x1000,
    }
}
