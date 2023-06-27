//! `prune` example
use rustic_core::{PruneOpts, Repository, RepositoryOptions};
use simplelog::{Config, LevelFilter, SimpleLogger};

fn main() {
    // Display info logs
    let _ = SimpleLogger::init(LevelFilter::Info, Config::default());

    // Open repository
    let mut repo_opts = RepositoryOptions::default();
    repo_opts.repository = Some("/tmp/repo".to_string());
    repo_opts.password = Some("test".to_string());
    let repo = Repository::new(&repo_opts).unwrap().open().unwrap();

    let prune_opts = PruneOpts::default();
    let prune_plan = repo.prune_plan(&prune_opts).unwrap();
    println!("{:?}", prune_plan.stats);
    println!("to repack: {:?}", prune_plan.repack_packs());
    // to run the plan uncomment this line:
    // prune_plan.do_prune(&repo, &prune_opts).unwrap();
}