use clap::Parser;
use plain_bitassets_app_cli_lib::Cli;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let res = cli.run().await?;
    #[allow(clippy::print_stdout)]
    {
        println!("{res}");
    }
    Ok(())
}
