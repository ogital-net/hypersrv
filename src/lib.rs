use std::error::Error as StdError;
use std::{
    net::SocketAddr,
    sync::atomic::{AtomicUsize, Ordering},
    thread,
};

use hyper::service::service_fn;
use hyper::{
    Request, Response,
    body::{Body, Incoming},
    service::Service,
};
use log::{debug, trace};
#[cfg(not(feature = "http2"))]
use log::error;
use tokio::net::TcpStream;
use tokio::task::JoinSet;
use tokio::{net::TcpListener, sync};

#[cfg(feature = "http2")]
#[derive(Clone, Copy, Debug)]
struct LocalExec;

#[cfg(feature = "http2")]
impl<F> hyper::rt::Executor<F> for LocalExec
where
    F: std::future::Future + 'static, // not requiring `Send`
{
    fn execute(&self, fut: F) {
        // This will spawn into the currently running `LocalSet`.
        tokio::task::spawn_local(fut);
    }
}

#[derive(Debug)]
pub struct AlpnError {
    protocol: Vec<u8>,
}

impl std::fmt::Display for AlpnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "unknown ALPN protocol: {}",
            String::from_utf8_lossy(&self.protocol)
        )
    }
}

impl std::error::Error for AlpnError {}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum Alpn {
    Http10,
    Http11,
    Http2,
    Http3,
}

impl TryFrom<&[u8]> for Alpn {
    type Error = AlpnError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        match value {
            b"http/1.0" => Ok(Alpn::Http10),
            b"http/1.1" => Ok(Alpn::Http11),
            b"h2" => Ok(Alpn::Http2),
            b"h3" => Ok(Alpn::Http3),
            unknown => Err(AlpnError {
                protocol: unknown.to_vec(),
            }),
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct ConnectionInfo {
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
    alpn: Option<Alpn>,
}

impl ConnectionInfo {
    pub fn new(local_addr: SocketAddr, remote_addr: SocketAddr, alpn: Option<&[u8]>) -> Self {
        Self {
            local_addr,
            remote_addr,
            alpn: alpn.and_then(|p| Alpn::try_from(p).ok()),
        }
    }

    /// Returns the local address of the connection.
    pub fn local_addr(&self) -> &SocketAddr {
        &self.local_addr
    }

    /// Returns the remote address of the connection.
    pub fn remote_addr(&self) -> &SocketAddr {
        &self.remote_addr
    }

    /// Returns the ALPN protocol used for the connection, if any.
    pub fn alpn(&self) -> Option<&Alpn> {
        self.alpn.as_ref()
    }
}

async fn run<S, B, IOF, F, I>(
    ls: &tokio::task::LocalSet,
    sa: SocketAddr,
    service: S,
    io_fn: IOF,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + 'static,
    S::Error: Into<Box<dyn StdError + Send + Sync>>,
    B: Body + 'static,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
    IOF: Fn(TcpStream, SocketAddr) -> F + Clone + 'static,
    F: Future<Output = std::io::Result<(I, ConnectionInfo)>>,
    I: hyper::rt::Read + hyper::rt::Write + Unpin + 'static,
{
    trace!(
        "run() thread {}",
        thread::current().name().unwrap_or_default()
    );
    trace!("bind() {}", sa);
    let listener = bind(sa).await?;

    // accept loop
    loop {
        trace!(
            "thread {} accept()",
            thread::current().name().unwrap_or_default()
        );
        let (stream, remote_addr) = listener.accept().await?;
        trace!(
            "thread {} accept() yeilded new connection from {}",
            thread::current().name().unwrap_or_default(),
            remote_addr
        );

        let service = service.clone();
        let io_fn = io_fn.clone();
        ls.spawn_local(async move {
            match io_fn(stream, remote_addr).await {
                Ok((io, conn_info)) => {
                    let s = service_fn(|mut req| {
                        let service = service.clone();
                        async move {
                            trace!(
                                "thread {} serving conn from {}",
                                thread::current().name().unwrap_or_default(),
                                conn_info.remote_addr()
                            );
                            req.extensions_mut().insert(conn_info);
                            service.call(req).await
                        }
                    });
                    let is_h2 = conn_info.alpn().is_some_and(|v| *v == Alpn::Http2);

                    if is_h2 {
                        #[cfg(feature = "http2")]
                        let _ = hyper::server::conn::http2::Builder::new(LocalExec)
                            .serve_connection(io, s)
                            .await;

                        #[cfg(not(feature = "http2"))]
                        error!("HTTP2 support not enabled");
                    } else {
                        let _ = hyper::server::conn::http1::Builder::new()
                            .serve_connection(io, s)
                            .await;
                    }
                    trace!("conn from {} closed", remote_addr);
                }
                Err(e) => {
                    debug!("connection error: {e}");
                }
            };
        });
    }
}

pub async fn serve<SF, IOF, S, B, IF, F, I>(
    num_threads: usize,
    listen_addr: SocketAddr,
    service_factory: SF,
    io_fn_factory: IOF,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    SF: Fn() -> S + Clone + Send + 'static,
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + 'static,
    S::Error: Into<Box<dyn StdError + Send + Sync>>,
    B: Body + 'static,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
    IOF: Fn() -> IF + Clone + Send + 'static,
    IF: Fn(TcpStream, SocketAddr) -> F + Clone + 'static,
    F: Future<Output = std::io::Result<(I, ConnectionInfo)>>,
    I: hyper::rt::Read + hyper::rt::Write + Unpin + 'static,
{
    let mut set = JoinSet::new();

    for _ in 0..num_threads {
        let io_f = io_fn_factory.clone();
        let f = service_factory.clone();
        set.spawn(spawn_thread(gen_thread_name(), move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build runtime");
            let ls = tokio::task::LocalSet::new();
            let service = f();
            let io_fn = io_f();
            ls.block_on(&rt, run(&ls, listen_addr, service, io_fn))
        }));
    }

    while let Some(res) = set.join_next().await {
        match res {
            Ok(r) => r?,
            Err(e) => return Err(Box::new(e)),
        }
    }
    Ok(())
}

async fn spawn_thread<F, T>(name: String, f: F) -> T
where
    F: FnOnce() -> T,
    F: Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = sync::oneshot::channel();

    thread::Builder::new()
        .name(name)
        .spawn(|| tx.send(f()))
        .expect("spawn");

    rx.await.unwrap()
}

async fn bind(addr: SocketAddr) -> std::io::Result<TcpListener> {
    let socket = match addr {
        SocketAddr::V4(_) => tokio::net::TcpSocket::new_v4()?,
        SocketAddr::V6(_) => tokio::net::TcpSocket::new_v6()?,
    };
    socket.set_reuseaddr(true)?;
    #[cfg(target_os = "linux")]
    socket.set_reuseport(true)?;
    #[cfg(target_os = "freebsd")]
    socket.set_reuse_port_lb(true)?;
    socket.set_nodelay(true)?;
    socket.bind(addr)?;
    socket.listen(256)
}

fn gen_thread_name() -> String {
    static ATOMIC_ID: AtomicUsize = AtomicUsize::new(1);
    let n = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
    format!("http-worker-{}", n)
}

#[cfg(test)]
mod tests {
    use std::{convert::Infallible, rc::Rc};

    use hyper_util::rt::TokioIo;

    use super::*;

    #[test]
    fn test_serve() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build runtime");
        let ls = tokio::task::LocalSet::new();
        let io_fn = async |stream: TcpStream, remote_addr| {
            let local_addr = stream.local_addr()?;
            Ok((
                TokioIo::<TcpStream>::new(stream),
                ConnectionInfo::new(local_addr, remote_addr, None),
            ))
        };
        let service = service_fn(|req: Request<Incoming>| async move {
            Ok::<hyper::Response<String>, Infallible>(Response::new(format!(
                "Hello World! {} {}",
                req.extensions().get::<SocketAddr>().unwrap(),
                thread::current().name().unwrap_or_default()
            )))
        });

        ls.block_on(
            &rt,
            serve(
                4,
                "127.0.0.1:0".parse().unwrap(),
                move || Rc::new(service),
                move || io_fn,
            ),
        )
        .unwrap();
    }
}
