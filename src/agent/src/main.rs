// Copyright (c) 2019 Ant Financial
//
// SPDX-License-Identifier: Apache-2.0
//

#![allow(non_camel_case_types)]
#![allow(unused_parens)]
#![allow(unused_unsafe)]
#![allow(dead_code)]
#![allow(non_snake_case)]
#[macro_use]
extern crate lazy_static;
extern crate oci;
extern crate prctl;
extern crate prometheus;
extern crate protocols;
extern crate regex;
extern crate rustjail;
extern crate scan_fmt;
extern crate serde_json;
extern crate signal_hook;

#[macro_use]
extern crate scopeguard;

#[macro_use]
extern crate slog;
extern crate netlink;

use crate::netlink::{RtnlHandle, NETLINK_ROUTE};
use anyhow::{anyhow, Context, Result};
use crossbeam_channel::bounded;
use crossbeam_channel::{select, unbounded, Receiver, Sender as cb_Sender};
use nix::fcntl::{self, OFlag};
use nix::fcntl::{FcntlArg, FdFlag};
use nix::libc::{STDERR_FILENO, STDIN_FILENO, STDOUT_FILENO};
use nix::pty;
use nix::sys::select::{select, FdSet};
use nix::sys::socket::{self, AddressFamily, SockAddr, SockFlag, SockType};
use nix::sys::wait;
use nix::unistd::{self, close, dup, dup2, fork, setsid, ForkResult};
use std::collections::HashMap;
use std::env;
use std::ffi::{CStr, CString, OsStr};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs as unixfs;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex, RwLock};
use std::{io, thread, thread::JoinHandle};
use unistd::Pid;

mod config;
mod device;
mod linux_abi;
mod metrics;
mod mount;
mod namespace;
mod network;
pub mod random;
mod sandbox;
mod signal;
#[cfg(test)]
mod test_utils;
mod uevent;
mod version;

use mount::{cgroups_mount, general_mount};
use sandbox::Sandbox;
use signal::setup_signal_handler;
use slog::Logger;
use uevent::watch_uevents;

mod rpc;

const NAME: &str = "kata-agent";
const KERNEL_CMDLINE_FILE: &str = "/proc/cmdline";
const CONSOLE_PATH: &str = "/dev/console";

const DEFAULT_BUF_SIZE: usize = 8 * 1024;

lazy_static! {
    static ref GLOBAL_DEVICE_WATCHER: Arc<Mutex<HashMap<String, Sender<String>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    static ref AGENT_CONFIG: Arc<RwLock<agentConfig>> =
        Arc::new(RwLock::new(config::agentConfig::new()));
}

fn announce(logger: &Logger, config: &agentConfig) {
    info!(logger, "announce";
    "agent-commit" => version::VERSION_COMMIT,

    // Avoid any possibility of confusion with the old agent
    "agent-type" => "rust",

    "agent-version" =>  version::AGENT_VERSION,
    "api-version" => version::API_VERSION,
    "config" => format!("{:?}", config),
    );
}

// Read from the specified file descriptor and send the data to the provided
// channel. Since this is part of the logger, any errors are sent to stderr to
// avoid recursion and since there is no alternative.
fn read_from_logger(read_fd: RawFd, ch: cb_Sender<Vec<u8>>) {
    if read_fd == -1 {
        eprintln!("ERROR: invalid log fd");
        return;
    }

    let mut reader = unsafe { File::from_raw_fd(read_fd) };

    loop {
        let mut buf: Vec<u8> = vec![0; 1024];

        let result = reader.read(&mut buf);

        match result {
            Ok(0) => break,
            Ok(bytes) => {
                match ch.send(buf[0..bytes].to_vec()) {
                    Ok(_) => (),
                    Err(e) => {
                        eprintln!("ERROR: failed to send logger data: {:?}", e);
                        break;
                    }
                };
            }

            Err(e) => {
                eprintln!("ERROR: failed to read logger: {:?}", e);
                break;
            }
        };
    }
}

fn start_logger(
    shutdown: Receiver<bool>,
    log_vport: u32,
    read_fd: RawFd,
) -> Result<JoinHandle<Result<()>>> {
    let builder = thread::Builder::new().name("logger".into());

    let handle = builder.spawn(move || -> Result<()> {
        let mut listenfd: RawFd = -1;
        let write_fd: RawFd;
        let drop_writer: bool;

        if log_vport > 0 {
            listenfd = socket::socket(
                AddressFamily::Vsock,
                SockType::Stream,
                SockFlag::SOCK_CLOEXEC,
                None,
            )?;

            let addr = SockAddr::new_vsock(libc::VMADDR_CID_ANY, log_vport);
            socket::bind(listenfd, &addr)?;
            socket::listen(listenfd, 1)?;
            write_fd = socket::accept4(listenfd, SockFlag::SOCK_CLOEXEC)?;

            drop_writer = true;

        // copy data to VSOCK
        } else {
            // copy log to stdout
            write_fd = io::stdout().as_raw_fd();
            drop_writer = false;
        }

        let (tx, rx) = unbounded::<Vec<u8>>();

        let builder2 = thread::Builder::new().name("logger-read-from-logger".into());

        let _log_reader_thread = builder2.spawn(move || read_from_logger(read_fd, tx));

        unsafe {
            let writer = &mut File::from_raw_fd(write_fd);

            loop {
                select! {
                    recv(rx) -> data => {
                            if data.is_err() {

                                // Synchronous calls don't allow the status to
                                // be queried, so make a non-blocking call to
                                // get the error condition.
                                let status = rx.try_recv();

                                if status.is_err() && status.err().unwrap() == crossbeam_channel::TryRecvError::Disconnected {
                                    // Logger thread was shut down
                                    continue;
                                }

                                eprintln!("ERROR: failed to read log data: {:?}", data.err());
                                continue;
                            }

                            let msg = data.unwrap();

                            let result = writer.write_all(&msg);
                            if result.is_err() {
                                eprintln!("ERROR: failed to write log data: {:?}", result.err());
                            }
                    },
                    recv(shutdown) -> _ => {
                        break;
                    },
                };
            }

            if listenfd != -1 {
                let _ = unistd::close(listenfd);
            }

            if drop_writer && write_fd != -1 {
                drop(writer);
            }
        }

        Ok(())
    })?;

    Ok(handle)
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    if args.len() == 2 && args[1] == "--version" {
        println!(
            "{} version {} (api version: {}, commit version: {}, type: rust)",
            NAME,
            version::AGENT_VERSION,
            version::API_VERSION,
            version::VERSION_COMMIT,
        );

        exit(0);
    }

    if args.len() == 2 && args[1] == "init" {
        rustjail::container::init_child();
        exit(0);
    }

    env::set_var("RUST_BACKTRACE", "full");

    lazy_static::initialize(&SHELLS);

    lazy_static::initialize(&AGENT_CONFIG);

    // support vsock log
    let (rfd, wfd) = unistd::pipe2(OFlag::O_CLOEXEC)?;

    let agentConfig = AGENT_CONFIG.clone();

    let init_mode = unistd::getpid() == Pid::from_raw(1);
    if init_mode {
        // dup a new file descriptor for this temporary logger writer,
        // since this logger would be dropped and it's writer would
        // be closed out of this code block.
        let newwfd = dup(wfd)?;
        let writer = unsafe { File::from_raw_fd(newwfd) };

        // Init a temporary logger used by init agent as init process
        // since before do the base mount, it wouldn't access "/proc/cmdline"
        // to get the customzied debug level.
        let (logger, logger_async_guard) =
            logging::create_logger(NAME, "agent", slog::Level::Debug, writer);

        // Must mount proc fs before parsing kernel command line
        general_mount(&logger).map_err(|e| {
            error!(logger, "fail general mount: {}", e);
            e
        })?;

        let mut config = agentConfig.write().unwrap();
        config.parse_cmdline(KERNEL_CMDLINE_FILE)?;

        init_agent_as_init(&logger, config.unified_cgroup_hierarchy)?;

        drop(logger_async_guard);
    } else {
        // once parsed cmdline and set the config, release the write lock
        // as soon as possible in case other thread would get read lock on
        // it.
        let mut config = agentConfig.write().unwrap();
        config.parse_cmdline(KERNEL_CMDLINE_FILE)?;
    }

    // Create a channel to inform threads to shut themselves down
    let (shutdown_tx, shutdown_rx) = bounded::<bool>(1);

    let config = agentConfig.read().unwrap();

    let log_vport = config.log_vport as u32;

    let _log_handle = start_logger(shutdown_rx.clone(), log_vport, rfd)?;

    let builder = thread::Builder::new().name("log-handle".into());

    let log_handle = builder.spawn(move || -> Result<()> {
        let mut reader = unsafe { File::from_raw_fd(rfd) };
        if log_vport > 0 {
            let listenfd = socket::socket(
                AddressFamily::Vsock,
                SockType::Stream,
                SockFlag::SOCK_CLOEXEC,
                None,
            )?;
            let addr = SockAddr::new_vsock(libc::VMADDR_CID_ANY, log_vport);
            socket::bind(listenfd, &addr)?;
            socket::listen(listenfd, 1)?;
            let datafd = socket::accept4(listenfd, SockFlag::SOCK_CLOEXEC)?;
            let mut log_writer = unsafe { File::from_raw_fd(datafd) };
            let _ = io::copy(&mut reader, &mut log_writer)?;
            let _ = unistd::close(listenfd);
            let _ = unistd::close(datafd);
        }
        // copy log to stdout
        let mut stdout_writer = io::stdout();
        let _ = io::copy(&mut reader, &mut stdout_writer)?;
        Ok(())
    })?;

    let writer = unsafe { File::from_raw_fd(wfd) };
    // Recreate a logger with the log level get from "/proc/cmdline".
    let (logger, logger_async_guard) =
        logging::create_logger(NAME, "agent", config.log_level, writer);

    announce(&logger, &config);

    // This variable is required as it enables the global (and crucially static) logger,
    // which is required to satisfy the the lifetime constraints of the auto-generated gRPC code.
    let global_logger_guard = slog_scope::set_global_logger(logger.new(o!("subsystem" => "rpc")));
    global_logger_guard.cancel_reset();

    let mut ttrpc_log_guard: Result<(), log::SetLoggerError> = Ok(());

    if config.log_level == slog::Level::Trace {
        // Redirect ttrpc log calls to slog iff full debug requested
        ttrpc_log_guard = Ok(slog_stdlog::init().map_err(|e| e)?);
    }

    start_sandbox(&logger, &config, init_mode)?;

    // Install a NOP logger for the remainder of the shutdown sequence
    // to ensure any log calls made by local crates using the scope logger
    // don't fail.
    let global_logger_guard2 =
        slog_scope::set_global_logger(slog::Logger::root(slog::Discard, o!()));
    global_logger_guard2.cancel_reset();

    //drop(global_logger_guard);
    drop(logger_async_guard);

    // Force uninterruptible logger thread to detect an error and end.
    let _ = unistd::close(wfd);

    let _ = unistd::close(rfd);

    drop(ttrpc_log_guard);

    // Shutdown logger thread
    shutdown_tx.send(true)?;

    let _ = log_handle.join().map_err(|e| anyhow!("{:?}", e))?;

    if config.log_level >= slog::Level::Debug {
        eprintln!("{} shutdown complete", NAME);
    }

    Ok(())
}

fn start_sandbox(logger: &Logger, config: &agentConfig, init_mode: bool) -> Result<()> {
    let shells = SHELLS.clone();
    let debug_console_vport = config.debug_console_vport as u32;

    let mut shell_handle: Option<JoinHandle<()>> = None;
    if config.debug_console {
        let thread_logger = logger.clone();

        let builder = thread::Builder::new().name("start-sandbox".into());

        let handle = builder.spawn(move || {
            let shells = shells.lock().unwrap();
            let result = setup_debug_console(&thread_logger, shells.to_vec(), debug_console_vport);
            if result.is_err() {
                // Report error, but don't fail
                warn!(thread_logger, "failed to setup debug console";
                    "error" => format!("{}", result.unwrap_err()));
            }
        })?;

        shell_handle = Some(handle);
    }

    // Initialize unique sandbox structure.
    let mut s = Sandbox::new(&logger).context("Failed to create sandbox")?;

    if init_mode {
        let mut rtnl = RtnlHandle::new(NETLINK_ROUTE, 0).unwrap();
        rtnl.handle_localhost()?;

        s.rtnl = Some(rtnl);
    }

    let sandbox = Arc::new(Mutex::new(s));

    // This is awkward but crossbeam channels do not support pubsub, meaning
    // the message needs to be sent to every thread which is listening on the
    // channel.
    //
    // If new threads are created, this number *must* be increased to take
    // account of them. Otherwise, the agent will not shutdown.
    let threads_to_stop = 2;

    // Create a channel to inform threads to shut themselves down
    let (shutdown_tx, shutdown_rx) = bounded::<bool>(threads_to_stop);

    let signal_thread_handler =
        setup_signal_handler(&logger, shutdown_rx.clone(), sandbox.clone()).map_err(|e| e)?;

    let uevents_thread_handler = watch_uevents(shutdown_rx.clone(), sandbox.clone());

    let (tx, rx) = mpsc::channel::<i32>();
    sandbox.lock().unwrap().sender = Some(tx);

    let mut server = rpc::start(sandbox, config.server_addr.as_str());

    let _ = server.start().unwrap();

    let _ = rx.recv()?;

    server.shutdown();

    // Request helper threads stop
    for i in 0..threads_to_stop {
        shutdown_tx.send(true).context(format!(
            "failed to request shutdown of helper thread {} of {}",
            i, threads_to_stop
        ))?;
    }

    // Wait for the helper threads to end
    uevents_thread_handler
        .join()
        .map_err(|e| anyhow!("{:?}", e))?;

    signal_thread_handler
        .join()
        .map_err(|e| anyhow!("{:?}", e))?;

    if let Some(handle) = shell_handle {
        handle.join().map_err(|e| anyhow!("{:?}", e))?;
    }

    Ok(())
}

// init_agent_as_init will do the initializations such as setting up the rootfs
// when this agent has been run as the init process.
fn init_agent_as_init(logger: &Logger, unified_cgroup_hierarchy: bool) -> Result<()> {
    cgroups_mount(logger, unified_cgroup_hierarchy).map_err(|e| {
        error!(
            logger,
            "fail cgroups mount, unified_cgroup_hierarchy {}: {}", unified_cgroup_hierarchy, e
        );
        e
    })?;

    fs::remove_file(Path::new("/dev/ptmx"))?;
    unixfs::symlink(Path::new("/dev/pts/ptmx"), Path::new("/dev/ptmx"))?;

    unistd::setsid()?;

    unsafe {
        libc::ioctl(io::stdin().as_raw_fd(), libc::TIOCSCTTY, 1);
    }

    env::set_var("PATH", "/bin:/sbin/:/usr/bin/:/usr/sbin/");

    let contents =
        std::fs::read_to_string("/etc/hostname").unwrap_or_else(|_| String::from("localhost"));
    let contents_array: Vec<&str> = contents.split(' ').collect();
    let hostname = contents_array[0].trim();

    if sethostname(OsStr::new(hostname)).is_err() {
        warn!(logger, "failed to set hostname");
    }

    Ok(())
}

fn sethostname(hostname: &OsStr) -> Result<()> {
    let size = hostname.len() as usize;

    let result =
        unsafe { libc::sethostname(hostname.as_bytes().as_ptr() as *const libc::c_char, size) };

    if result != 0 {
        Err(anyhow!("failed to set hostname"))
    } else {
        Ok(())
    }
}

lazy_static! {
    static ref SHELLS: Arc<Mutex<Vec<String>>> = {
        let mut v = Vec::new();

        if !cfg!(test) {
            v.push("/bin/bash".to_string());
            v.push("/bin/sh".to_string());
        }

        Arc::new(Mutex::new(v))
    };
}

// pub static mut LOG_LEVEL: ;
// pub static mut TRACE_MODE: ;

use crate::config::agentConfig;
use nix::sys::stat::Mode;
use std::os::unix::io::{FromRawFd, RawFd};
use std::path::PathBuf;
use std::process::exit;

fn setup_debug_console(logger: &Logger, shells: Vec<String>, port: u32) -> Result<()> {
    let mut shell: &str = "";
    for sh in shells.iter() {
        let binary = PathBuf::from(sh);
        if binary.exists() {
            shell = sh;
            break;
        }
    }

    if shell == "" {
        return Err(anyhow!("no shell found to launch debug console"));
    }

    if port > 0 {
        let listenfd = socket::socket(
            AddressFamily::Vsock,
            SockType::Stream,
            SockFlag::SOCK_CLOEXEC,
            None,
        )?;
        let addr = SockAddr::new_vsock(libc::VMADDR_CID_ANY, port);
        socket::bind(listenfd, &addr)?;
        socket::listen(listenfd, 1)?;
        loop {
            let f: RawFd = socket::accept4(listenfd, SockFlag::SOCK_CLOEXEC)?;
            match run_debug_console_shell(logger, shell, f) {
                Ok(_) => {
                    info!(logger, "run_debug_console_shell session finished");
                }
                Err(err) => {
                    error!(logger, "run_debug_console_shell failed: {:?}", err);
                }
            }
        }
    } else {
        let mut flags = OFlag::empty();
        flags.insert(OFlag::O_RDWR);
        flags.insert(OFlag::O_CLOEXEC);
        loop {
            let f: RawFd = fcntl::open(CONSOLE_PATH, flags, Mode::empty())?;
            match run_debug_console_shell(logger, shell, f) {
                Ok(_) => {
                    info!(logger, "run_debug_console_shell session finished");
                }
                Err(err) => {
                    error!(logger, "run_debug_console_shell failed: {:?}", err);
                }
            }
        }
    };
}

fn io_copy<R: ?Sized, W: ?Sized>(reader: &mut R, writer: &mut W) -> io::Result<u64>
where
    R: Read,
    W: Write,
{
    let mut buf = [0; DEFAULT_BUF_SIZE];
    let buf_len;

    match reader.read(&mut buf) {
        Ok(0) => return Ok(0),
        Ok(len) => buf_len = len,
        Err(err) => return Err(err),
    };

    // write and return
    match writer.write_all(&buf[..buf_len]) {
        Ok(_) => Ok(buf_len as u64),
        Err(err) => Err(err),
    }
}

fn run_debug_console_shell(logger: &Logger, shell: &str, socket_fd: RawFd) -> Result<()> {
    let pseduo = pty::openpty(None, None)?;
    let _ = fcntl::fcntl(pseduo.master, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC));
    let _ = fcntl::fcntl(pseduo.slave, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC));

    let slave_fd = pseduo.slave;

    match fork() {
        Ok(ForkResult::Child) => {
            // create new session with child as session leader
            setsid()?;

            // dup stdin, stdout, stderr to let child act as a terminal
            dup2(slave_fd, STDIN_FILENO)?;
            dup2(slave_fd, STDOUT_FILENO)?;
            dup2(slave_fd, STDERR_FILENO)?;

            // set tty
            unsafe {
                libc::ioctl(0, libc::TIOCSCTTY);
            }

            let cmd = CString::new(shell).unwrap();
            let args: Vec<&CStr> = vec![];

            // run shell
            let _ = unistd::execvp(cmd.as_c_str(), args.as_slice()).map_err(|e| match e {
                nix::Error::Sys(errno) => {
                    std::process::exit(errno as i32);
                }
                _ => std::process::exit(-2),
            });
        }

        Ok(ForkResult::Parent { child: child_pid }) => {
            info!(logger, "get debug shell pid {:?}", child_pid);

            let (rfd, wfd) = unistd::pipe2(OFlag::O_CLOEXEC)?;
            let master_fd = pseduo.master;
            let debug_shell_logger = logger.clone();

            // channel that used to sync between thread and main process
            let (tx, rx) = mpsc::channel::<i32>();

            let builder = thread::Builder::new().name("console-shell".into());

            // start a thread to do IO copy between socket and pseduo.master
            builder.spawn(move || {
                let mut master_reader = unsafe { File::from_raw_fd(master_fd) };
                let mut master_writer = unsafe { File::from_raw_fd(master_fd) };
                let mut socket_reader = unsafe { File::from_raw_fd(socket_fd) };
                let mut socket_writer = unsafe { File::from_raw_fd(socket_fd) };

                loop {
                    let mut fd_set = FdSet::new();
                    fd_set.insert(rfd);
                    fd_set.insert(master_fd);
                    fd_set.insert(socket_fd);

                    match select(
                        Some(fd_set.highest().unwrap() + 1),
                        &mut fd_set,
                        None,
                        None,
                        None,
                    ) {
                        Ok(_) => (),
                        Err(e) => {
                            if e == nix::Error::from(nix::errno::Errno::EINTR) {
                                continue;
                            } else {
                                error!(debug_shell_logger, "select error {:?}", e);
                                tx.send(1).unwrap();
                                break;
                            }
                        }
                    }

                    if fd_set.contains(rfd) {
                        info!(
                            debug_shell_logger,
                            "debug shell process {} exited", child_pid
                        );
                        tx.send(1).unwrap();
                        break;
                    }

                    if fd_set.contains(master_fd) {
                        match io_copy(&mut master_reader, &mut socket_writer) {
                            Ok(0) => {
                                debug!(debug_shell_logger, "master fd closed");
                                tx.send(1).unwrap();
                                break;
                            }
                            Ok(_) => {}
                            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                            Err(e) => {
                                error!(debug_shell_logger, "read master fd error {:?}", e);
                                tx.send(1).unwrap();
                                break;
                            }
                        }
                    }

                    if fd_set.contains(socket_fd) {
                        match io_copy(&mut socket_reader, &mut master_writer) {
                            Ok(0) => {
                                debug!(debug_shell_logger, "socket fd closed");
                                tx.send(1).unwrap();
                                break;
                            }
                            Ok(_) => {}
                            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                            Err(e) => {
                                error!(debug_shell_logger, "read socket fd error {:?}", e);
                                tx.send(1).unwrap();
                                break;
                            }
                        }
                    }
                }
            })?;

            let wait_status = wait::waitpid(child_pid, None);
            info!(logger, "debug console process exit code: {:?}", wait_status);

            info!(logger, "notify debug monitor thread to exit");
            // close pipe to exit select loop
            let _ = close(wfd);

            // wait for thread exit.
            let _ = rx.recv().unwrap();
            info!(logger, "debug monitor thread has exited");

            // close files
            let _ = close(rfd);
            let _ = close(master_fd);
            let _ = close(slave_fd);
        }
        Err(err) => {
            return Err(anyhow!("fork error: {:?}", err));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_setup_debug_console_no_shells() {
        // Guarantee no shells have been added
        // (required to avoid racing with
        // test_setup_debug_console_invalid_shell()).
        let shells_ref = SHELLS.clone();
        let mut shells = shells_ref.lock().unwrap();
        shells.clear();
        let logger = slog_scope::logger();

        let result = setup_debug_console(&logger, shells.to_vec(), 0);

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "no shell found to launch debug console"
        );
    }

    #[test]
    fn test_setup_debug_console_invalid_shell() {
        let shells_ref = SHELLS.clone();
        let mut shells = shells_ref.lock().unwrap();

        let dir = tempdir().expect("failed to create tmpdir");

        // Add an invalid shell
        let shell = dir
            .path()
            .join("enoent")
            .to_str()
            .expect("failed to construct shell path")
            .to_string();

        shells.push(shell);
        let logger = slog_scope::logger();

        let result = setup_debug_console(&logger, shells.to_vec(), 0);

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "no shell found to launch debug console"
        );
    }
}
