use std::{
    ffi::{OsStr, OsString},
    path::PathBuf,
    sync::OnceLock,
};

use bip300301_enforcer_integration_tests::util::{
    AbortOnDrop, BinPaths as EnforcerBinPaths, OnceLockExt as _, VarError,
    spawn_command_with_args,
};
use plain_bitassets::types::Network;

#[derive(Clone, Debug, Default)]
pub struct BinPaths {
    bitassets: OnceLock<PathBuf>,
    pub others: EnforcerBinPaths,
}

impl BinPaths {
    pub fn bitassets(&self) -> Result<&PathBuf, VarError> {
        self.bitassets.get_or_try_init_from_env("BITASSETS_APP")
    }
}

#[derive(Clone, Debug)]
pub struct BitAssetsApp {
    pub path: PathBuf,
    pub data_dir: PathBuf,
    pub log_level: Option<tracing::Level>,
    pub mainchain_grpc_port: u16,
    /// Port to use for P2P networking
    pub net_port: u16,
    pub network: Network,
    /// Port to use for the RPC server
    pub rpc_port: u16,
    /// Port to use for ZMQ server
    pub zmq_port: u16,
}

impl BitAssetsApp {
    pub fn spawn_command_with_args<Env, Arg, Envs, Args, F>(
        &self,
        envs: Envs,
        args: Args,
        err_handler: F,
    ) -> AbortOnDrop<()>
    where
        Arg: AsRef<OsStr>,
        Env: AsRef<OsStr>,
        Envs: IntoIterator<Item = (Env, Env)>,
        Args: IntoIterator<Item = Arg>,
        F: FnOnce(anyhow::Error) + Send + 'static,
    {
        let mut default_args = vec![
            "--datadir".to_owned(),
            self.data_dir.display().to_string(),
            "--headless".to_owned(),
            "--mainchain-grpc-port".to_owned(),
            self.mainchain_grpc_port.to_string(),
            "--net-addr".to_owned(),
            format!("127.0.0.1:{}", self.net_port),
            format!("--network={}", self.network),
            "--rpc-port".to_owned(),
            self.rpc_port.to_string(),
            "--zmq-addr".to_owned(),
            format!("127.0.0.1:{}", self.zmq_port),
        ];
        if let Some(log_level) = self.log_level {
            default_args.push("--log-level".to_owned());
            default_args.push(log_level.as_str().to_owned());
        }
        let args = default_args
            .into_iter()
            .map(OsString::from)
            .chain(args.into_iter().map(|arg| arg.as_ref().to_owned()));
        spawn_command_with_args(
            &self.data_dir,
            self.path.clone(),
            envs,
            args,
            err_handler,
        )
    }
}
