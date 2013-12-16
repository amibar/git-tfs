using System.IO;
using Sep.Git.Tfs.Core.TfsInterop;
using Sep.Git.Tfs.Util;

namespace Sep.Git.Tfs.Core
{
    public class TfsTreeEntry : ITreeEntry
    {
        private readonly string _pathInGitRepo;
        private readonly IItem _item;

        public TfsTreeEntry(string pathInGitRepo, IItem item, TfsChangeType changeType = TfsChangeType.None)
        {
            _pathInGitRepo = pathInGitRepo;
            _item = item;
            ChangeType = changeType;
        }

        public IItem Item { get { return _item; } }
        public string FullName { get { return _pathInGitRepo; } }
        public TfsChangeType ChangeType { get; private set; }
        public Stream OpenRead()
        {
            return new TemporaryFileStream(_item.DownloadFile());
        }

    }
}
