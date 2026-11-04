use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};
#[cfg(any(feature = "runtime-smol", feature = "runtime-async-std"))]
use std::{io, sync::Arc, task::ready};

#[cfg(any(feature = "runtime-smol", feature = "runtime-async-std"))]
use async_io::Async;
use async_io::Timer;

use super::AsyncTimer;
#[cfg(any(feature = "runtime-smol", feature = "runtime-async-std"))]
use super::{AsyncUdpSocket, Runtime, UdpPollHelper};

#[cfg(feature = "runtime-smol")]
// Due to MSRV, we must specify `self::` where there's crate/module ambiguity
pub use self::smol::SmolRuntime;

#[cfg(feature = "runtime-smol")]
mod smol {
    use super::*;

    /// A Quinn runtime for smol
    #[derive(Debug)]
    pub struct SmolRuntime;

    impl Runtime for SmolRuntime {
        fn new_timer(&self, t: Instant) -> Pin<Box<dyn AsyncTimer>> {
            Box::pin(Timer::at(t))
        }

        fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) {
            ::smol::spawn(future).detach();
        }

        fn wrap_udp_socket(
            &self,
            sock: std::net::UdpSocket,
        ) -> io::Result<Arc<dyn AsyncUdpSocket>> {
            Ok(Arc::new(UdpSocket::new(sock)?))
        }
    }
}

#[cfg(feature = "runtime-async-std")]
// Due to MSRV, we must specify `self::` where there's crate/module ambiguity
pub use self::async_std::AsyncStdRuntime;

#[cfg(feature = "runtime-async-std")]
mod async_std {
    use super::*;

    /// A Quinn runtime for async-std
    #[derive(Debug)]
    pub struct AsyncStdRuntime;

    impl Runtime for AsyncStdRuntime {
        fn new_timer(&self, t: Instant) -> Pin<Box<dyn AsyncTimer>> {
            Box::pin(Timer::at(t))
        }

        fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) {
            ::async_std::task::spawn(future);
        }

        fn wrap_udp_socket(
            &self,
            sock: std::net::UdpSocket,
        ) -> io::Result<Arc<dyn AsyncUdpSocket>> {
            Ok(Arc::new(UdpSocket::new(sock)?))
        }
    }
}

impl AsyncTimer for Timer {
    fn reset(mut self: Pin<&mut Self>, t: Instant) {
        self.set_at(t)
    }

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<()> {
        Future::poll(self, cx).map(|_| ())
    }
}

#[cfg(any(feature = "runtime-smol", feature = "runtime-async-std"))]
#[derive(Debug)]
struct UdpSocket {
    io: Async<std::net::UdpSocket>,
    inner: udp::UdpSocketState,
}

#[cfg(any(feature = "runtime-smol", feature = "runtime-async-std"))]
impl UdpSocket {
    fn new(sock: std::net::UdpSocket) -> io::Result<Self> {
        Ok(Self {
            inner: udp::UdpSocketState::new((&sock).into())?,
            io: Async::new_nonblocking(sock)?,
        })
    }

    fn poll_recv_with(
        &self,
        cx: &mut Context,
        mut recv: impl FnMut() -> io::Result<usize>,
    ) -> Poll<io::Result<usize>> {
        loop {
            ready!(self.io.poll_readable(cx))?;
            match recv() {
                Ok(res) => return Poll::Ready(Ok(res)),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }
}

#[cfg(any(feature = "runtime-smol", feature = "runtime-async-std"))]
impl AsyncUdpSocket for UdpSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn super::UdpPoller>> {
        Box::pin(UdpPollHelper::new(move || {
            let socket = self.clone();
            async move { socket.io.writable().await }
        }))
    }

    fn try_send(&self, transmit: &udp::Transmit) -> io::Result<()> {
        self.inner.send((&self.io).into(), transmit)
    }

    fn poll_recv(
        &self,
        cx: &mut Context,
        bufs: &mut [io::IoSliceMut<'_>],
        meta: &mut [udp::RecvMeta],
    ) -> Poll<io::Result<usize>> {
        self.poll_recv_with(cx, || self.inner.recv((&self.io).into(), bufs, meta))
    }

    fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.io.as_ref().local_addr()
    }

    fn may_fragment(&self) -> bool {
        self.inner.may_fragment()
    }

    fn max_transmit_segments(&self) -> usize {
        self.inner.max_gso_segments()
    }

    fn max_receive_segments(&self) -> usize {
        self.inner.gro_segments()
    }
}

#[cfg(all(test, any(feature = "runtime-smol", feature = "runtime-async-std")))]
mod tests {
    use std::{future::poll_fn, io::IoSliceMut, net::UdpSocket as StdUdpSocket, time::Duration};

    use super::*;

    fn socket() -> UdpSocket {
        let sock = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        sock.set_nonblocking(true).unwrap();
        UdpSocket::new(sock).unwrap()
    }

    #[tokio::test]
    async fn receive_errors_are_returned_without_retrying() {
        for kind in [
            io::ErrorKind::NotConnected,
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::OutOfMemory,
        ] {
            let socket = socket();
            let sender = StdUdpSocket::bind("127.0.0.1:0").unwrap();
            sender
                .send_to(b"ready", socket.local_addr().unwrap())
                .unwrap();
            let mut data = [0; 64];
            let mut bufs = [IoSliceMut::new(&mut data)];
            let mut meta = [udp::RecvMeta::default()];
            let mut calls = 0;
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                poll_fn(|cx| {
                    socket.poll_recv_with(cx, || {
                        calls += 1;
                        if calls == 1 {
                            Err(io::Error::from(kind))
                        } else {
                            // If an error is swallowed, finish the old loop rather than hang the test.
                            socket.inner.recv((&socket.io).into(), &mut bufs, &mut meta)
                        }
                    })
                }),
            )
            .await
            .unwrap();
            assert_eq!(result.unwrap_err().kind(), kind);
            assert_eq!(
                calls, 1,
                "receive errors must reach the endpoint immediately"
            );
            // A returned error must not consume the queued datagram.
            let n = poll_fn(|cx| socket.poll_recv(cx, &mut bufs, &mut meta))
                .await
                .unwrap();
            assert_eq!(n, 1);
            assert_eq!(meta[0].len, 5);
            assert_eq!(&data[..5], b"ready");
        }
    }

    #[tokio::test]
    async fn receive_would_block_waits_for_readiness() {
        let socket = socket();
        let sender = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = socket.local_addr().unwrap();
        let mut data = [0; 64];
        let mut bufs = [IoSliceMut::new(&mut data)];
        let mut meta = [udp::RecvMeta::default()];
        // With no input, the normal production receive path registers its waiter.
        poll_fn(|cx| {
            assert!(socket.poll_recv(cx, &mut bufs, &mut meta).is_pending());
            Poll::Ready(())
        })
        .await;
        sender.send_to(b"old", addr).unwrap();
        let sender = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            sender.send_to(b"new", addr).unwrap();
        });
        let mut first = true;
        let n = tokio::time::timeout(
            Duration::from_secs(5),
            poll_fn(|cx| {
                socket.poll_recv_with(cx, || {
                    let result = socket.inner.recv((&socket.io).into(), &mut bufs, &mut meta);
                    if first && result.is_ok() {
                        first = false;
                        // Model readiness consumed by another reader before this receive.
                        Err(io::ErrorKind::WouldBlock.into())
                    } else {
                        result
                    }
                })
            }),
        )
        .await
        .unwrap()
        .unwrap();
        sender.await.unwrap();
        assert!(!first);
        assert_eq!(n, 1);
        assert_eq!(meta[0].len, 3);
        assert_eq!(&data[..3], b"new");
    }
}
