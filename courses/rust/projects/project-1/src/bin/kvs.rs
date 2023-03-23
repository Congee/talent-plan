use clap;
use clap::Parser;
use std::process::exit;

use kvs;

#[derive(clap::Subcommand)]
enum Cmd {
    Get { key: String },
    Set { key: String, value: String },
    Rm { key: String },
}

#[derive(clap::Parser)]
#[command(version)]
struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

fn main() {
    let args = Args::parse();

    match args.cmd {
        Cmd::Get { key } => {
            eprintln!("unimplemented");
            exit(1);
        }
        Cmd::Set { key, value } => {
            eprintln!("unimplemented");
            exit(1);
        }
        Cmd::Rm { key } => {
            eprintln!("unimplemented");
            exit(1);
        }
    }
}
