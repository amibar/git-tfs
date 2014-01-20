using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using LibGit2Sharp;
using NDesk.Options;
using StructureMap;
using Sep.Git.Tfs.Core;
using Sep.Git.Tfs.Core.TfsInterop;
using Sep.Git.Tfs.Util;
using Mode = LibGit2Sharp.Mode;

namespace Sep.Git.Tfs.Commands
{
    [Pluggable("clone-branch")]
    [Description("clone-branch tfs-url-or-instance-name repository-path \n  ex : git tfs clone-branch http://myTfsServer:8080/tfs/TfsRepository $/ProjectName/ProjectBranch\n")]
    public class CloneBranch : GitTfsCommand
    {
        private readonly Globals globals;
        private readonly ITfsHelper tfsHelper;
        private readonly RemoteOptions remoteOptions;
        private TextWriter stdout;
        private Init _init;

        private enum Verbosity
        {
            ErrorAndWarn = 0,
            Info         = 1,
            Debug        = 2,
        }

        #region BranchEntry

        private class BranchEntry
        {
            private BranchEntry _root;

            public IBranchObject BranchObject { get; private set; }
            public BranchEntry Parent { get; private set; }
            public int RootChangesetId { get; set; }
            public int FirstChangesetId { get; set; }
            public int CommitsCount { get; set; }
            public Commit LastCommit { get; set; }
            public string LastTag { get; set; }
            private readonly string _parentPath;
            private readonly string _parentPathSlash;
            private readonly string _gitBranchName;
            private Dictionary<int, int> _merges = new Dictionary<int, int>(); 

            public BranchEntry(IBranchObject branchObject, BranchEntry parent)
            {
                BranchObject = branchObject;
                Parent = parent;
                CommitsCount = -1;
                if (parent != null)
                {
                    _parentPath = parent.Path;
                    _parentPathSlash = _parentPath + "/";
                }
                _gitBranchName = branchObject.Path.Split('/').Last();
            }

            public bool IsRoot
            {
                get { return BranchObject.IsRoot; }
            }

            public string Path
            {
                get { return BranchObject.Path; }
            }

            public string ParentPath
            {
                get { return _parentPath; }
            }

            public string ParentPathSlash
            {
                get { return _parentPathSlash; }
            }

            public BranchEntry Root
            {
                get
                {
                    if (BranchObject.IsRoot)
                    {
                        return this;
                    }
                    if (_root == null)
                    {
                        _root = Parent;
                        while (!_root.BranchObject.IsRoot)
                        {
                            _root = _root.Parent;
                        }
                    }
                    return _root;
                }
            }

            public string GitBranchName
            {
                get { return _gitBranchName; }
            }

            public TfsTreeEntry[] ParentFullTree
            {
                get; set;
            }

            public void SetMerges(Dictionary<int, int> merges)
            {
                _merges = merges;
            }

            public int GetMergeSourceId(int targetId)
            {
                int sourceId;
                if (_merges.TryGetValue(targetId, out sourceId))
                {
                    return sourceId;
                }
                return -1;
            }

            public override string ToString()
            {
                return Path;
            }
        }

        #endregion

        #region ChangesetEntrys

        private class ChangesetEntry
        {
            public CommitEntry[] CommitEntries { get; private set; }

            public IEnumerable<Commit> Commits
            {
                get { return CommitEntries.Select(ce => ce.Commit); }
            }

            public int ChangesetId
            {
                get { return (int)CommitEntries[0].TfsChangeset.Summary.ChangesetId; }
            }

            public bool IsMergeChangeset
            {
                get { return CommitEntries.Any(c => c.TfsChangeset.IsMergeChangeset); }
            }

            public ChangesetEntry(CommitEntry[] commits)
            {
                CommitEntries = commits;
            }
        }

        #endregion

        #region CommitEntry

        private class CommitEntry
        {
            public TfsChangeset TfsChangeset { get; private set; }
            public BranchEntry Branch { get; private set; }
            public Commit Commit { get; set; }

            public CommitEntry(BranchEntry branch, TfsChangeset tfsChangeset)
            {
                Branch = branch;
                TfsChangeset = tfsChangeset;
            }
        }

        #endregion

        public CloneBranch(Globals globals, Init init, TextWriter stdout, ITfsHelper tfsHelper, RemoteOptions remoteOptions)
        {
            Verbose = Verbosity.ErrorAndWarn;
            _init = init;
            this.globals = globals;
            this.stdout = stdout;
            this.tfsHelper = tfsHelper;
            this.remoteOptions = remoteOptions;
        }

        private string FilesCachePath { get; set; }
        private Verbosity Verbose { get; set; }

        public OptionSet OptionSet
        {
            get
            {
                return new OptionSet
                {
                    { "fcp|files-cache-path=", "Path to files cache", v => FilesCachePath = v },
                    { "verbose|v", "Verbose", v => Verbose = v != null ? Verbosity.Info : 0 },
                    { "vv", "More verbose", v => Verbose = v != null ? Verbosity.Debug : 0 },
                }.Merge(remoteOptions.OptionSet);
            }
        }

        public int Run(string tfsUrl, string tfsRepositoryPath, string gitRepositoryPath)
        {
            Dictionary<int, ChangesetEntry> committedChangesets = new Dictionary<int, ChangesetEntry>();

            Stopwatch duration = Stopwatch.StartNew();
            Stopwatch swDownload = Stopwatch.StartNew();

            tfsHelper.Url = tfsUrl;
            tfsHelper.Username = remoteOptions.Username;
            tfsHelper.Password = remoteOptions.Password;
            tfsHelper.EnsureAuthenticated();

            if (!tfsHelper.CanGetBranchInformation)
            {
                throw new GitTfsException("error: this version of TFS doesn't support this functionality");
            }

            int retVal = _init.Run(tfsUrl, tfsRepositoryPath, gitRepositoryPath);

            BranchEntry[] branches = GetBranches(tfsRepositoryPath);

            if (branches.IsEmpty())
            {
                stdout.WriteLine("info: no TFS branch found !\n\nPS:perhaps you should convert your trunk folder into a branch in TFS.");
                return GitTfsExitCodes.InvalidArguments;
            }
            else
            {
                //IGitTfsRemote gitTfsRemote = globals.Repository.ReadTfsRemote(globals.RemoteId);
                IGitRepository gitRepository = globals.Repository;

                HashSet<BranchEntry> branchFirstCommit = new HashSet<BranchEntry>();

                HashSet<int> branchesRootChangesetIds = new HashSet<int>(branches.Where(b => !b.IsRoot).Select(b => b.RootChangesetId));
                var changesets = EnumerateBranchesChangesets(branches);

                int gcCommitsPerGC = 100;
                int gcCounter = 0;
                StringBuilder log = new StringBuilder();

                using (LibGit2Sharp.Repository repository = new Repository(gitRepositoryPath))
                {
                    ObjectDatabase odb = repository.ObjectDatabase;
                    
                    Stopwatch sw = new Stopwatch();
                    foreach (ChangesetEntry changeset in changesets)
                    {
                        foreach (CommitEntry commitEntry in changeset.CommitEntries)
                        {
                            sw.Restart();

                            WriteInfo("Fetching CS{0} @ {1}", changeset.ChangesetId, commitEntry.Branch.Path);

                            TfsTreeEntry [] changesTree = GetTfsChangesTree(commitEntry);

                            if (branchFirstCommit.Contains(commitEntry.Branch))
                            {
                                branchFirstCommit.Remove(commitEntry.Branch);

                                changesTree = GenerateDeletesOnBranch(commitEntry, changesTree);
                            }
                            WriteDebug("\tChanges end");

                            int edited = 0;
                            int added = 0;
                            int renamed = 0;
                            int deleted = 0;
                            int failed = 0;

                            swDownload.Restart();

                            WriteInfo("\tDownloading started");
                            int downloadsCounter = 0;
                            Stream[] filesStreams = new Stream[changesTree.Length];
                            
                            Parallel.For(0, changesTree.Length, i =>
                            {
                                TfsTreeEntry tfsTreeEntry = changesTree[i];

                                if (tfsTreeEntry.Item.ItemType == TfsItemType.File)
                                {
                                    if ((tfsTreeEntry.ChangeType & (TfsChangeType.Add | TfsChangeType.Edit | TfsChangeType.Rename | TfsChangeType.Branch | TfsChangeType.Undelete)) != 0)
                                    {
                                        string cachePath = null;
                                        if (FilesCachePath != null)
                                        {
                                            cachePath = Path.Combine(FilesCachePath, changeset.ChangesetId.ToString(), tfsTreeEntry.Item.ServerItem.Substring(2).Replace('/', '\\'));
                                        }
                                        Stream stream = null;
                                        int tries = 3;
                                        while (stream == null && tries-- > 0)
                                        {
                                            try
                                            {
                                                WriteDebug("\tDownloading {0} ({1:N0})", tfsTreeEntry.Item.ServerItem, tfsTreeEntry.Item.ContentLength);
                                                if (cachePath != null && File.Exists(cachePath))
                                                {
                                                    stream = File.Open(cachePath, FileMode.Open, FileAccess.Read, FileShare.Read);
                                                    WriteInfoExact(',');
                                                }
                                                else
                                                {
                                                    TemporaryFile temporaryFile = tfsTreeEntry.Item.DownloadFile();
                                                    if (cachePath != null)
                                                    {
                                                        string dir = Path.GetDirectoryName(cachePath);
                                                        if (!Directory.Exists(dir))
                                                        {
                                                            Directory.CreateDirectory(dir);
                                                        }
                                                        File.Copy(temporaryFile.Path, cachePath);
                                                    }
                                                    stream = new TemporaryFileStream(temporaryFile.Path);
                                                    WriteInfoExact('.');

                                                }
                                                Interlocked.Increment(ref downloadsCounter);
                                            }
                                            catch (Exception ex)
                                            {
                                                WriteInfoExact('X');
                                                WriteError("\tFailed to download {0} ({1:N0}): Exception : {2}", tfsTreeEntry.Item.ServerItem, tfsTreeEntry.Item.ContentLength, ex);

                                                stream = null;
                                            }
                                        }
                                        filesStreams[i] = stream;
                                    }
                                }
                            });

                            WriteInfo("\n\tDownloaded {0} files in {1} ms", downloadsCounter, swDownload.ElapsedMilliseconds);

                            TreeDefinition td =
                                commitEntry.Branch.LastCommit != null
                                    ? TreeDefinition.From(commitEntry.Branch.LastCommit)
                                    : new TreeDefinition();

                            WriteInfo("\tUpdating index started");

                            const TfsChangeType TfsChangeTypeUpdated = TfsChangeType.Add | TfsChangeType.Edit | TfsChangeType.Rename | TfsChangeType.Branch | TfsChangeType.Undelete;
                            const TfsChangeType TfsChangeTypeRemoved = TfsChangeType.Delete | TfsChangeType.SourceRename;

                            for (int i = 0; i < changesTree.Length; i++)
                            {
                                TfsTreeEntry tfsTreeEntry = changesTree[i];

                                if (string.IsNullOrWhiteSpace(tfsTreeEntry.FullName))
                                {
                                    continue;
                                }

                                string action = "";
                                bool warn = false;

                                // Check both to support the case of renaming current=>old, new=>current. current is both SourceRename and Rename.
                                if (((tfsTreeEntry.ChangeType & (TfsChangeType.Delete)) != 0) ||
                                    ((tfsTreeEntry.ChangeType & TfsChangeTypeRemoved) != 0 && 
                                    (tfsTreeEntry.ChangeType & TfsChangeTypeUpdated) == 0))
                                {
                                    td.Remove(tfsTreeEntry.FullName);
                                    WriteInfoExact('.');

                                    if ((tfsTreeEntry.ChangeType & (TfsChangeType.SourceRename)) != 0)
                                    {
                                        action = "Renaming from";
                                    }
                                    else if ((tfsTreeEntry.ChangeType & (TfsChangeType.Delete)) != 0)
                                    {
                                        action = "Deleting";
                                        ++deleted;
                                    }
                                }
                                else if ((tfsTreeEntry.ChangeType & TfsChangeTypeUpdated) != 0)
                                {
                                    if ((tfsTreeEntry.ChangeType & (TfsChangeType.Add)) != 0)
                                    {
                                        action = "Adding";
                                        ++added;
                                    }
                                    else
                                    {
                                        if ((tfsTreeEntry.ChangeType & (TfsChangeType.Edit | TfsChangeType.Rename)) == (TfsChangeType.Edit | TfsChangeType.Rename))
                                        {
                                            action += "Updating and Renaming to";
                                            ++renamed;
                                        }
                                        // Renamed not deleted
                                        else if ((tfsTreeEntry.ChangeType & (TfsChangeType.Rename)) != 0)
                                        {
                                            action = "Renaming to";
                                            ++renamed;
                                        }
                                        else if ((tfsTreeEntry.ChangeType & (TfsChangeType.Edit)) != 0)
                                        {
                                            action += "Updating";
                                            ++edited;
                                        }
                                        else if ((tfsTreeEntry.ChangeType & (TfsChangeType.Branch)) != 0)
                                        {
                                            action += "Branching";
                                            ++added;
                                        }
                                        else if ((tfsTreeEntry.ChangeType & (TfsChangeType.Undelete)) != 0)
                                        {
                                            action += "Undeleting";
                                            ++added;
                                        }
                                    }

                                    if (tfsTreeEntry.Item.ItemType == TfsItemType.File)
                                    {
                                        Stream stream = filesStreams[i];

                                        if (stream != null)
                                        {
                                            using (stream)
                                            {
                                                Blob blob = odb.CreateBlob(stream);
                                                td.Add(tfsTreeEntry.FullName, blob, Mode.NonExecutableFile);
                                                WriteInfoExact('.');
                                            }
                                        }
                                        else
                                        {
                                            warn = true;
                                            action = "Failed to download";
                                            ++failed;
                                        }
                                    }                                 
                                }

                                if (!string.IsNullOrWhiteSpace(action))
                                {
                                    string message = string.Concat("\t", action, " ", tfsTreeEntry.Item.ServerItem, " (" + tfsTreeEntry.FullName + ")");
                                    if (warn)
                                    {
                                        WriteWarn(message);
                                    }
                                    else
                                    {
                                        WriteDebug(message);
                                    }
                                }
                            }

                            LogEntry logEntry = commitEntry.TfsChangeset.MakeNewLogEntry();

                            log.Length = 0;
                            log.AppendLine(logEntry.Log);
                            log.AppendFormat(GitTfsConstants.TfsCommitInfoFormat, tfsUrl, tfsRepositoryPath, logEntry.ChangesetId);
                            log.AppendLine();

                            var workItemsIds = commitEntry.TfsChangeset.WorkItemsIds;
                            if (!workItemsIds.IsEmpty())
                            {
                                foreach (int workItemId in workItemsIds)
                                {
                                    log.AppendFormat("{0} {1} {2}", GitTfsConstants.GitTfsWorkItemPrefix, workItemId, "associate");
                                    log.AppendLine();                                    
                                }
                            }

                            ChangesetEntry parentChangesetEntry = null;

                            if (changeset.IsMergeChangeset)
                            {
                                int parentChangesetId = commitEntry.Branch.GetMergeSourceId(changeset.ChangesetId);
                                if (parentChangesetId != -1)
                                {
                                    committedChangesets.TryGetValue(parentChangesetId, out parentChangesetEntry);
                                }
                            }

                            // We create the treem there will no more files updates
                            // And we need to tree for merge compare
                            Tree commitTree = odb.CreateTree(td);

                            List<Commit> parents = new List<Commit>();
                            if (commitEntry.Branch.LastCommit != null)
                            {
                                parents.Add(commitEntry.Branch.LastCommit);
                            }

                            if (parentChangesetEntry != null)
                            {
                                WriteDebug("Merge check of CS{0} begin", changeset.ChangesetId);
                                // Get files only
                                Dictionary<string, TfsTreeEntry> tfsMergedFiles = 
                                    changesTree.Where(e => e.Item.ItemType == TfsItemType.File && (e.ChangeType & TfsChangeType.Merge) != 0).ToDictionary(e => e.FullName, e => e, StringComparer.OrdinalIgnoreCase);

                                WriteDebug("CS{0} merged files begin, {1} files", changeset.ChangesetId, tfsMergedFiles.Count);

                                foreach (var change in tfsMergedFiles.OrderBy(p => p.Key))
                                {
                                    WriteDebug("\t{0} : {1}", change.Value.FullName, change.Value.ChangeType);                                    
                                }

                                WriteDebug("CS{0} merged files end", changeset.ChangesetId);

                                Commit targetCommit = commitEntry.Branch.LastCommit;

                                WriteDebug("Target commit {0} ({1})", targetCommit.Id, targetCommit.MessageShort);
                                WriteDebug("Source commits count = {0}", parentChangesetEntry.Commits.Count());

                                foreach (Commit sourceCommit in parentChangesetEntry.Commits)
                                {
                                    WriteDebug("Source commit {0} ({1})", sourceCommit.Id, sourceCommit.MessageShort);

                                    var unmodifiedFiles = repository.Diff.Compare<TreeChanges>(commitTree, sourceCommit.Tree, null, null, new CompareOptions() { IncludeUnmodified = true, }).
                                        Where(c => c.Status == ChangeKind.Unmodified);

                                    foreach (var unmodifiedFile in unmodifiedFiles)
                                    {
                                        string filename = unmodifiedFile.Path.Replace('\\', '/');
                                        if (tfsMergedFiles.Remove(filename))
                                        {
                                            WriteDebug("File {0} removed (Unmodified) {1} left", filename, tfsMergedFiles.Count);

                                            if (tfsMergedFiles.Count == 0)
                                            {
                                                break;
                                            }
                                        }
                                    }

                                    if (tfsMergedFiles.Count == 0)
                                    {
                                        break;
                                    }

                                    Commit commonAncestor = repository.Commits.FindCommonAncestor(targetCommit, sourceCommit);

                                    WriteDebug("Common ancestor commit {0} ({1})", commonAncestor.Id, commonAncestor.MessageShort);

                                    TreeChanges compareResult = repository.Diff.Compare<TreeChanges>(commonAncestor.Tree, sourceCommit.Tree);
                                    if (!compareResult.IsEmpty())
                                    {
                                        WriteDebug("Compare result files begin, {0} files", compareResult.Count());

                                        foreach (TreeEntryChanges change in compareResult.OrderBy(c => c.Path))
                                        {
                                            string filename = change.Path.Replace('\\', '/');
                                            if (change.Status == ChangeKind.Renamed)
                                            {
                                                string oldFilename = change.OldPath.Replace('\\', '/');
                                                WriteDebug("\t{0} <= {1} : {2}", filename, oldFilename, change.Status);
                                            }
                                            else
                                            {
                                                WriteDebug("\t{0} : {1}", filename, change.Status);   
                                            }
                                        }

                                        WriteDebug("Compare result files end");

                                        foreach (TreeEntryChanges change in compareResult)
                                        {
                                            string filename = change.Path.Replace('\\', '/');

                                            if (tfsMergedFiles.Remove(filename))
                                            {
                                                WriteDebug("File {0} removed {1} left", filename, tfsMergedFiles.Count);

                                                if (tfsMergedFiles.Count == 0)
                                                {
                                                    break;
                                                }
                                            }
                                            if (change.Status == ChangeKind.Renamed)
                                            {
                                                filename = change.OldPath.Replace('\\', '/');

                                                if (tfsMergedFiles.Remove(filename))
                                                {
                                                    WriteDebug("File {0} removed {1} left", filename, tfsMergedFiles.Count);

                                                    if (tfsMergedFiles.Count == 0)
                                                    {
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    if (tfsMergedFiles.Count == 0)
                                    {
                                        break;
                                    }
                                }

                                if (tfsMergedFiles.Count == 0)
                                {
                                    WriteInfo("Merging commits");

                                    parents.AddRange(parentChangesetEntry.Commits);
                                }
                                else
                                {
                                    WriteInfo("Cherry picking commits");

                                    WriteDebug("CS{0} merge unmatched files begin, {1} files", changeset.ChangesetId, tfsMergedFiles.Count);

                                    foreach (var change in tfsMergedFiles.OrderBy(p => p.Key))
                                    {
                                        WriteDebug("\t{0} : {1}", change.Value.FullName, change.Value.ChangeType);
                                    }

                                    WriteDebug("CS{0} merge unmatched files end", changeset.ChangesetId);

                                }
                                WriteDebug("Merge check of CS{0} end", changeset.ChangesetId);
                            }
                             
                            commitEntry.Commit = odb.CreateCommit(
                                    log.ToString(),
                                    new Signature(logEntry.AuthorName, logEntry.AuthorEmail, logEntry.Date),
                                    new Signature(logEntry.CommitterName, logEntry.CommitterEmail, logEntry.Date),
                                    commitTree,
                                    parents);

                            commitEntry.Branch.LastCommit = commitEntry.Commit;

                            WriteLine("C{0} = {1}", changeset.ChangesetId, commitEntry.Commit.Id.Sha);

                            WriteInfo("\tUpdating index end");

                            int changes = added + deleted + edited + renamed;
                            WriteInfo("");
                            WriteInfo("\tCommitted CS{0} into {1} took {2} ms", changeset.ChangesetId, commitEntry.Commit.Id, sw.ElapsedMilliseconds);
                            WriteInfo("\t{0} changes => {1} added, {2} deleted, {3} edited, {4} renamed, {5} failed", changes, added, deleted, edited, renamed, failed);
                            if (parentChangesetEntry != null)
                            {
                                WriteInfo("\tMerged parent: CS{0} @ {1}", parentChangesetEntry.ChangesetId, string.Join(",", parentChangesetEntry.CommitEntries.Select(ce => ce.Branch.Path)));
                            }
                            WriteInfo("\tMessage: {0}", log.ToString());

                            if (branchesRootChangesetIds.Contains(changeset.ChangesetId))
                            {
                                int changesetId = changeset.ChangesetId;
                                CommitEntry ce = commitEntry;
                                foreach (BranchEntry branchEntry in branches.Where(b => b.RootChangesetId == changesetId && b.ParentPath == ce.Branch.Path))
                                {
                                    WriteInfo("Creating branch {0} from branch {1}", branchEntry.GitBranchName, commitEntry.Branch.Path);
                                    branchEntry.LastCommit = commitEntry.Commit;

                                    // Since branches in TFS may be partial, we need to know which files were in the branch's parent
                                    // in order to know which files to omit.
                                    // This is where we take a snapshot of the full tree in the scope of the parent branch
                                    branchEntry.ParentFullTree = 
                                        commitEntry.TfsChangeset.GetFullTree(branchEntry.ParentPath).
                                        Where(e => e.Item.ItemType == TfsItemType.File).
                                        ToArray();
                                    branchFirstCommit.Add(branchEntry);
                                }
                            }

                            // Tag every commit so if we in the middle we can still see something, becaue the branches are not created until the end of the process

                            string newTagName = string.Format("{0}@CS{1}", commitEntry.Branch.GitBranchName, changeset.ChangesetId);
                            repository.ApplyTag(newTagName, commitEntry.Commit.Id.Sha);

                            if (commitEntry.Branch.LastTag != null)
                            {
                                repository.Tags.Remove(commitEntry.Branch.LastTag);
                            }
                            commitEntry.Branch.LastTag = newTagName;
                        }

                        committedChangesets[changeset.ChangesetId] = changeset;

                        ++gcCounter;
                        if (gcCounter == gcCommitsPerGC)
                        {
                            gcCounter = 0;
                            WriteInfo("Running GC begin");
                            gitRepository.CommandOutputPipe(r => WriteDebug("GC: {0}", r.ReadToEnd()), "gc", "--auto");
                            WriteInfo("Running GC end");
                        }
                    }

                    gitRepository.CommandOutputPipe(r => WriteDebug("GC: {0}", r.ReadToEnd()), "gc", "--auto");
                    foreach (BranchEntry branchEntry in branches)
                    {
                        if (branchEntry.LastCommit != null)
                        {
                            repository.CreateBranch(branchEntry.GitBranchName, branchEntry.LastCommit);
                        }
                    }
                    WriteInfo("Running final GC begin");
                    gitRepository.CommandOutputPipe(r => WriteDebug("GC: {0}", r.ReadToEnd()), "gc", "--auto");
                    WriteInfo("Running final GC end");

                    foreach (Tag tag in repository.Tags.ToArray())
                    {
                        repository.Tags.Remove(tag.Name);
                    }
                }
            }

            //WriteLine("  -> Open 'Source Control Explorer' and for each folder corresponding to a branch, right click on the folder and select 'Branching and Merging' > 'Convert to branch'.");
            WriteInfo("Operation took {0}", duration.Elapsed);
            return GitTfsExitCodes.OK;
        }

        private TfsTreeEntry[] GetTfsChangesTree(CommitEntry commitEntry)
        {
            TfsTreeEntry[] changesTree = commitEntry.TfsChangeset.GetChangesTree(commitEntry.Branch.Path).ToArray();
            WriteInfo("\tGot {0} changes on branch {1}", changesTree.Count(), commitEntry.Branch.BranchObject.Path);

            if (Verbose == Verbosity.Debug)
            {
                WriteDebug("\tChanges begin");
                foreach (TfsTreeEntry tfsTreeEntry in changesTree.OrderBy(tte => tte.Item.ServerItem))
                {
                    WriteDebug("\tChange: {0} ({1}) : {2}", tfsTreeEntry.Item.ServerItem, tfsTreeEntry.FullName, tfsTreeEntry.ChangeType);
                }
                WriteDebug("\tChanges end");
            }
            return changesTree;
        }

        private TfsTreeEntry[] GenerateDeletesOnBranch(CommitEntry commitEntry, TfsTreeEntry[] changesTree)
        {            
            // When branching in TFS not all files are branched (option), this block deletes files that were not branched.
            int parentBranchCutLength = commitEntry.Branch.ParentPath.Length + 1;
            var parentBranchFiles =
                commitEntry.Branch.ParentFullTree.
                    Where(c => c.Item.ItemType == TfsItemType.File && c.Item.ServerItem.StartsWith(commitEntry.Branch.ParentPathSlash, StringComparison.InvariantCultureIgnoreCase)).
                    ToDictionary(c => c.Item.ServerItem.Substring(parentBranchCutLength), c => c, StringComparer.InvariantCultureIgnoreCase);

            int branchCutLength = commitEntry.Branch.Path.Length + 1;

            foreach (var tfsTreeChange in changesTree.Where(c => c.Item.ItemType == TfsItemType.File))
            {
                string filename = tfsTreeChange.Item.ServerItem.Substring(branchCutLength);
                parentBranchFiles.Remove(filename);
            }

            if (parentBranchFiles.Count > 0)
            {
                var newChangesTree = new List<TfsTreeEntry>(changesTree);

                WriteDebug("\tDuring branch {0} files were not branched", parentBranchFiles.Count);

                foreach (var tfsTreeChange in parentBranchFiles.Values.OrderBy(tte => tte.Item.ServerItem))
                {
                    TfsTreeEntry tfsDeletedTreeEntry = new TfsTreeEntry(tfsTreeChange.FullName, tfsTreeChange.Item, TfsChangeType.Delete);
                    newChangesTree.Add(tfsDeletedTreeEntry);
                    WriteDebug("\tDeleted: {0} ({1}) : {2}", tfsDeletedTreeEntry.Item.ServerItem, tfsDeletedTreeEntry.FullName, tfsDeletedTreeEntry.ChangeType);
                }
                changesTree = newChangesTree.ToArray();
            }

            commitEntry.Branch.ParentFullTree = null;

            return changesTree;
        }

        private BranchEntry[] GetBranches(string tfsRepositoryPath)
        {
            List<BranchEntry> branches = new List<BranchEntry>();

            var allBranches = tfsHelper.GetBranches().
                ToArray();

            List<IBranchObject> rootBranches = allBranches.Where(b => b.IsRoot).ToList();

            foreach (IBranchObject rootBranch in rootBranches)
            {
                branches.Add(new BranchEntry(rootBranch, null));
            }

            Queue<IBranchObject> initialBranches = new Queue<IBranchObject>(allBranches.Where(b => !b.IsRoot));
            Queue<IBranchObject> orphanBranches = new Queue<IBranchObject>();

            bool advanced = true;
            while (advanced && (initialBranches.Count > 0 || orphanBranches.Count > 0))
            {
                advanced = false;
                if (initialBranches.Count == 0)
                {
                    initialBranches = orphanBranches;
                    orphanBranches = new Queue<IBranchObject>();
                }

                IBranchObject branch = initialBranches.Dequeue();

                BranchEntry parentBranch = branches.FirstOrDefault(b => b.Path == branch.ParentPath);
                if (parentBranch != null)
                {
                    branches.Add(new BranchEntry(branch, parentBranch));
                    advanced = true;
                }
                else
                {
                    orphanBranches.Enqueue(branch);
                }
            }

            BranchEntry tfsRootBranch = branches.FirstOrDefault(b => b.Path == tfsRepositoryPath);

            branches = branches.Where(b => b.Root == tfsRootBranch).ToList();

            Parallel.ForEach(branches.Where(b => b.Root == tfsRootBranch), 
                //new ParallelOptions(){MaxDegreeOfParallelism = 1,},
                branch =>
            {
                WriteInfo("Checking branch {0}", branch.Path);
                if (branch.IsRoot)
                {
                    WriteInfo("Branch {0} is root", branch.Path);
                }
                else
                {
                    BranchingChangesets branchingChangesets = tfsHelper.GetRootChangesetForBranch(branch.Path);
                    branch.RootChangesetId = branchingChangesets.SourceChangesetId;
                    branch.FirstChangesetId = branchingChangesets.TargetChangesetId;
                    WriteInfo("Branch {0} branched {1} from CS{2} to CS{3}", branch.Path, branch.ParentPath, branch.RootChangesetId, branch.FirstChangesetId);
                }

                WriteInfo("Loading branch {0} merge points", branch.Path);
                var merges = tfsHelper.GetBranchMerges(branch.Path);
                branch.SetMerges(merges);
                WriteInfo("Branch {0} has {1} merge points", branch.Path, merges.Count);
            });
            return branches.ToArray();
        }

        private IEnumerable<ChangesetEntry> EnumerateBranchesChangesets(BranchEntry[] branches)
        {
            IGitTfsRemote gitTfsRemote = globals.Repository.ReadTfsRemote(globals.RemoteId);

            List<CommitEntry> commits = new List<CommitEntry>();

            List<BranchEntry> branchesEntries = new List<BranchEntry>(branches);
            List<ITfsChangeset> changesets = new List<ITfsChangeset>(new ITfsChangeset[branches.Length]);
            List<IEnumerator<ITfsChangeset>> enumerators =
                branches.Select(b => tfsHelper.EnumerateChangesets(b.Path, gitTfsRemote).GetEnumerator()).ToList();

            Parallel.For(0, enumerators.Count, i =>
                {
                    WriteInfo("Enumerating {0}", branches[i].Path);
                    if (enumerators[i].MoveNext())
                    {
                        changesets[i] = enumerators[i].Current;

                        if (changesets[i].Summary.ChangesetId < branches[i].FirstChangesetId)
                        {
                            WriteDebug("Branch {0} starts at CS{1}, (Current CS{2})", branches[i].Path, branches[i].FirstChangesetId, changesets[i].Summary.ChangesetId);
                        }

                        while (changesets[i].Summary.ChangesetId < branches[i].FirstChangesetId && enumerators[i].MoveNext())
                        {
                            changesets[i] = enumerators[i].Current;
                            WriteDebug("Moving {0} forward to CS{1}", branches[i].Path, changesets[i].Summary.ChangesetId);
                        }
                        WriteInfo("Branch {0} starts at CS{1}", branches[i].Path, changesets[i].Summary.ChangesetId);
                    }
                    else
                    {
                        WriteInfo("Branch {0} has no relevant changesets", branches[i].Path);
                    }
                });

            for (int i = enumerators.Count - 1; i >= 0; i--)
            {
                if (changesets[i] == null)
                {
                    changesets.RemoveAt(i);
                    enumerators.RemoveAt(i);
                    branchesEntries.RemoveAt(i);
                }
            }

            while (changesets.Count > 0)
            {
                int minChangesetId = (int)changesets[0].Summary.ChangesetId;

                for (int i = 1; i < changesets.Count; i++)
                {
                    long candidateChangesetId = changesets[i].Summary.ChangesetId;
                    if (minChangesetId > candidateChangesetId)
                    {
                        minChangesetId = (int)candidateChangesetId;
                    }
                }

                commits.Clear();

                for (int i = 0; i < changesets.Count; i++)
                {
                    if (minChangesetId == changesets[i].Summary.ChangesetId)
                    {
                        commits.Add(new CommitEntry(branchesEntries[i], (TfsChangeset)changesets[i]));
                    }
                }

                ChangesetEntry changesetEntry = new ChangesetEntry(commits.ToArray());

                yield return changesetEntry;

                for (int i = changesets.Count - 1; i >= 0; i--)
                {
                    if (minChangesetId == changesets[i].Summary.ChangesetId)
                    {
                        if (enumerators[i].MoveNext())
                        {
                            changesets[i] = enumerators[i].Current;
                        }
                        else
                        {
                            changesets.RemoveAt(i);
                            enumerators.RemoveAt(i);
                            branchesEntries.RemoveAt(i);
                        }
                    }
                }

            }
        }

        private void WriteError(string text)
        {
            WriteLine(text);
        }

        private void WriteWarn(string text)
        {
            WriteLine(text);
        }

        private void WriteInfo(string text)
        {
            if (Verbose >= Verbosity.Info)
            {
                WriteLine(text);
            }
        }

        private void WriteDebug(string text)
        {
            if (Verbose >= Verbosity.Debug)
            {
                WriteLine(text);
            }
        }

        private void WriteError(string format, params object[] args)
        {
            WriteError(string.Format(format, args));
        }

        private void WriteWarn(string format, params object[] args)
        {
            WriteWarn(string.Format(format, args));
        }

        private void WriteInfo(string format, params object[] args)
        {
            WriteInfo(string.Format(format, args));
        }

        private void WriteDebug(string format, params object[] args)
        {
            WriteDebug(string.Format(format, args));
        }

        private void WriteInfoExact(char ch)
        {
            if (Verbose == Verbosity.Info)
            {
                stdout.Write(ch);
            }
        }

        private void WriteLine(string text)
        {
            if (Verbose == Verbosity.Debug)
            {
                stdout.WriteLine(DateTime.Now.ToString("HH:mm:ss") + " : " + text);
            }
            else
            {
                stdout.WriteLine(text);
            }
        }

        private void WriteLine(string format, params object[] args)
        {
            WriteLine(string.Format(format, args));
        }
    }
}
