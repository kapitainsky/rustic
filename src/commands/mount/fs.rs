#[cfg(not(windows))]
use std::os::unix::prelude::OsStrExt;
use std::{
    collections::BTreeMap,
    ffi::{CString, OsStr},
    path::Path,
    sync::RwLock,
    time::{Duration, SystemTime},
};

use fuse_mt::{
    CallbackResult, DirectoryEntry, FileAttr, FileType, FilesystemMT, RequestInfo, ResultData,
    ResultEmpty, ResultEntry, ResultOpen, ResultReaddir, ResultSlice, ResultXattr, Xattr,
};
use itertools::Itertools;
use rustic_core::{
    repofile::{Node, NodeType},
    OpenFile,
};
use rustic_core::{Id, IndexedFull, Repository};

pub(super) struct RusticFS<P, S> {
    repo: Repository<P, S>,
    root: Id,
    open_files: RwLock<BTreeMap<u64, OpenFile>>,
    now: SystemTime,
}

impl<P, S: IndexedFull> RusticFS<P, S> {
    pub(crate) fn from_node(repo: Repository<P, S>, node: Node) -> anyhow::Result<Self> {
        let open_files = RwLock::new(BTreeMap::new());

        Ok(Self {
            repo,
            root: node.subtree.unwrap(),
            open_files,
            now: SystemTime::now(),
        })
    }

    fn node_from_path(&self, path: &Path) -> Result<Node, i32> {
        Ok(self
            .repo
            .node_from_path(self.root, path)
            .map_err(|_| libc::ENOENT)?)
    }
}

fn node_to_filetype(node: &Node) -> FileType {
    match node.node_type {
        NodeType::File => FileType::RegularFile,
        NodeType::Dir => FileType::Directory,
        NodeType::Symlink { .. } => FileType::Symlink,
        NodeType::Chardev { .. } => FileType::CharDevice,
        NodeType::Dev { .. } => FileType::BlockDevice,
        NodeType::Fifo => FileType::NamedPipe,
        NodeType::Socket => FileType::Socket,
    }
}

fn node_type_to_rdev(tpe: &NodeType) -> u32 {
    u32::try_from(match tpe {
        NodeType::Dev { device } => *device,
        NodeType::Chardev { device } => *device,
        _ => 0,
    })
    .unwrap()
}

impl<P, S: IndexedFull> FilesystemMT for RusticFS<P, S> {
    fn getattr(&self, _req: RequestInfo, path: &Path, _fh: Option<u64>) -> ResultEntry {
        let node = self.node_from_path(path)?;
        Ok((
            Duration::from_secs(1),
            FileAttr {
                /// Size in bytes
                size: node.meta.size,
                /// Size in blocks
                blocks: 0,
                // Time of last access
                atime: node.meta.atime.map(SystemTime::from).unwrap_or(self.now),
                /// Time of last modification
                mtime: node.meta.mtime.map(SystemTime::from).unwrap_or(self.now),
                /// Time of last metadata change
                ctime: node.meta.ctime.map(SystemTime::from).unwrap_or(self.now),
                /// Time of creation (macOS only)
                crtime: self.now,
                /// Kind of file (directory, file, pipe, etc.)
                kind: node_to_filetype(&node),
                /// Permissions
                perm: node.meta.mode.unwrap_or(0) as u16,
                /// Number of hard links
                nlink: node.meta.links.try_into().unwrap_or(1),
                /// User ID
                uid: node.meta.uid.unwrap_or(0),
                /// Group ID
                gid: node.meta.gid.unwrap_or(0),
                /// Device ID (if special file)
                rdev: node_type_to_rdev(&node.node_type),
                /// Flags (macOS only; see chflags(2))
                flags: 0,
            },
        ))
    }

    #[cfg(not(windows))]
    fn readlink(&self, _req: RequestInfo, path: &Path) -> ResultData {
        let node = self.node_from_path(path)?;
        if node.is_symlink() {
            let target = node.node_type.to_link().as_os_str().as_bytes();
            Ok(target.to_vec())
        } else {
            Err(libc::ENOSYS)
        }
    }

    fn open(&self, _req: RequestInfo, path: &Path, _flags: u32) -> ResultOpen {
        let node = self.node_from_path(path)?;
        let open = self.repo.open_file(&node).map_err(|_| libc::ENOSYS)?;
        let mut open_files = self.open_files.write().unwrap();
        let fh = open_files.first_key_value().map(|(fh, _)| *fh).unwrap_or(0);
        _ = open_files.insert(fh, open);
        Ok((fh, 0))
    }

    fn release(
        &self,
        _req: RequestInfo,
        _path: &Path,
        fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> ResultEmpty {
        _ = self.open_files.write().unwrap().remove(&fh);
        Ok(())
    }

    fn read(
        &self,
        _req: RequestInfo,
        _path: &Path,
        fh: u64,
        offset: u64,
        size: u32,

        callback: impl FnOnce(ResultSlice<'_>) -> CallbackResult,
    ) -> CallbackResult {
        if let Some(open_file) = self.open_files.read().unwrap().get(&fh) {
            if let Ok(data) =
                self.repo
                    .read_file_at(open_file, offset.try_into().unwrap(), size as usize)
            {
                return callback(Ok(&data));
            }
        }
        callback(Err(libc::ENOSYS))
    }

    fn opendir(&self, _req: RequestInfo, _path: &Path, _flags: u32) -> ResultOpen {
        Ok((0, 0))
    }

    fn readdir(&self, _req: RequestInfo, path: &Path, _fh: u64) -> ResultReaddir {
        let node = self.node_from_path(path)?;

        let tree = self
            .repo
            .get_tree(&node.subtree.unwrap())
            .map_err(|_| libc::ENOSYS)?;

        let result = tree
            .nodes
            .into_iter()
            .map(|node| DirectoryEntry {
                name: node.name(),
                kind: node_to_filetype(&node),
            })
            .collect();
        Ok(result)
    }

    fn releasedir(&self, _req: RequestInfo, _path: &Path, _fh: u64, _flags: u32) -> ResultEmpty {
        Ok(())
    }

    fn listxattr(&self, _req: RequestInfo, path: &Path, size: u32) -> ResultXattr {
        let node = self.node_from_path(path)?;
        let xattrs = node
            .meta
            .extended_attributes
            .into_iter()
            // convert into null-terminated [u8]
            .map(|a| CString::new(a.name).unwrap().into_bytes_with_nul())
            .concat();

        if size == 0 {
            Ok(Xattr::Size(u32::try_from(xattrs.len()).unwrap()))
        } else {
            Ok(Xattr::Data(xattrs))
        }
    }

    fn getxattr(&self, _req: RequestInfo, path: &Path, name: &OsStr, size: u32) -> ResultXattr {
        let node = self.node_from_path(path)?;
        match node
            .meta
            .extended_attributes
            .into_iter()
            .find(|a| name == OsStr::new(&a.name))
        {
            None => Err(libc::ENOSYS),
            Some(attr) => {
                if size == 0 {
                    Ok(Xattr::Size(u32::try_from(attr.value.len()).unwrap()))
                } else {
                    Ok(Xattr::Data(attr.value))
                }
            }
        }
    }
}
