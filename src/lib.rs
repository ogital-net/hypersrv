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
use log::trace;
use tokio::net::TcpStream;
use tokio::task::JoinSet;
use tokio::{net::TcpListener, sync};

async fn run<S, B, F, I>(
    ls: &tokio::task::LocalSet,
    sa: SocketAddr,
    service: S,
    io_fn: F,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + 'static,
    S::Error: Into<Box<dyn StdError + Send + Sync>>,
    B: Body + 'static,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
    F: Fn(TcpStream, SocketAddr) -> (I, SocketAddr),
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

        let (io, _) = io_fn(stream, remote_addr);
        let service = service.clone();

        ls.spawn_local(async move {
            let s = service_fn(|mut req| async {
                trace!(
                    "thread {} serving conn from {}",
                    thread::current().name().unwrap_or_default(),
                    remote_addr
                );
                req.extensions_mut().insert(remote_addr);
                service.call(req).await
            });
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, s)
                .await;
            trace!("conn from {} closed", remote_addr);
        });
    }
}

pub async fn serve<SF, IOF, S, B, IF, I>(
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
    IF: Fn(TcpStream, SocketAddr) -> (I, SocketAddr),
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
        let io_fn = |stream, remote_addr| (TokioIo::new(stream), remote_addr);
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
                "127.0.0.1:8000".parse().unwrap(),
                move || Rc::new(service),
                move || io_fn,
            ),
        )
        .unwrap();
    }
}
