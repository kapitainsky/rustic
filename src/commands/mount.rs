//! `mount` subcommand
mod fs;
use std::{ffi::OsStr, path::PathBuf};

use fs::RusticFS;

use crate::{commands::open_repository, status_err, Application, RUSTIC_APP};

use abscissa_core::{Command, Runnable, Shutdown};
use anyhow::Result;
use fuse_mt::{mount, FuseMT};

/// `dump` subcommand
#[derive(clap::Parser, Command, Debug)]
pub(crate) struct MountCmd {
    /// file from snapshot to dump
    #[clap(value_name = "SNAPSHOT[:PATH]")]
    snap: String,

    mountpoint: PathBuf,
}

impl Runnable for MountCmd {
    fn run(&self) {
        if let Err(err) = self.inner_run() {
            status_err!("{}", err);
            RUSTIC_APP.shutdown(Shutdown::Crash);
        };
    }
}

impl MountCmd {
    fn inner_run(&self) -> Result<()> {
        let config = RUSTIC_APP.config();

        let repo = open_repository(&config)?.to_indexed()?;
        let node =
            repo.node_from_snapshot_path(&self.snap, |sn| config.snapshot_filter.matches(sn))?;

        let options = [OsStr::new("-o"), OsStr::new("fsname=rusticfs")];

        let fs = FuseMT::new(RusticFS::from_node(repo, node)?, 1);
        mount(fs, &self.mountpoint, &options)?;

        Ok(())
    }
}
