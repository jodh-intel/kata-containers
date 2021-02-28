// Copyright (c) 2021 Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0
//

use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::watch::Receiver;

// Size of I/O read buffer
const BUF_SIZE: usize = 8192;

// Interruptable I/O copy using readers and writers
// (an interruptable version of "io::copy()").
pub async fn interruptable_io_copier<R: Sized, W: Sized>(
    mut reader: R,
    mut writer: W,
    mut shutdown: Receiver<bool>,
) -> io::Result<u64>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut total_bytes: u64 = 0;

    let mut buf: [u8; BUF_SIZE] = [0; BUF_SIZE];

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                eprintln!("INFO: interruptable_io_copier: got shutdown request");
                break;
            },

            result = reader.read(&mut buf) => {
                if result.is_err() {
                    eprintln!("ERROR: failed to read data: {:?}", result.err());
                    continue;
                }

                let bytes : usize = result.unwrap();

                if bytes == 0 {
                    continue;
                }

                total_bytes = total_bytes + bytes as u64;

                // Actually copy the data ;)
                writer.write_all(&buf[..bytes]).await?;
            },
        };
    }

    Ok(total_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;
    use std::io::Cursor;
    use std::io::Write;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Poll::Pending, Poll::Ready};
    use tokio::pin;
    use tokio::select;
    use tokio::sync::watch::channel;
    use tokio::task::{JoinError, JoinHandle};
    use tokio::time::Duration;

    #[derive(Debug, Default, Clone)]
    struct BufWriter {
        data: Arc<Mutex<Vec<u8>>>,
        slow_write: bool,
        write_delay: Duration,
    }

    impl BufWriter {
        fn new_with_options(slow_write: bool, write_delay: Duration) -> Self {
            BufWriter {
                data: Arc::new(Mutex::new(Vec::<u8>::new())),
                slow_write: slow_write,
                write_delay: write_delay,
            }
        }

        fn new() -> Self {
            BufWriter {
                data: Arc::new(Mutex::new(Vec::<u8>::new())),
                slow_write: false,
                write_delay: Duration::new(0, 0),
            }
        }

        fn clear(&mut self) {
            self.data.lock().unwrap().clear();
        }

        fn len(&mut self) -> usize {
            self.data.lock().unwrap().len()
        }

        fn capacity(&mut self) -> usize {
            self.data.lock().unwrap().capacity()
        }

        fn to_vec(&mut self) -> Vec<u8> {
            self.data.clone().lock().as_ref().unwrap().to_vec()
        }

        fn to_string(&mut self) -> String {
            String::from_utf8(self.to_vec()).unwrap()
        }

        fn write_vec(&mut self, buf: &[u8]) -> io::Result<usize> {
            let vec_ref = self.data.clone();

            let mut vec_locked = vec_ref.lock();

            let mut v = vec_locked.as_deref_mut().unwrap();

            if self.write_delay.as_nanos() > 0 {
                std::thread::sleep(self.write_delay);
            }

            let result = std::io::Write::write(&mut v, buf);

            result
        }
    }

    impl Write for BufWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.write_vec(buf)
        }

        fn flush(&mut self) -> io::Result<()> {
            let vec_ref = self.data.clone();

            let mut vec_locked = vec_ref.lock();

            let v = vec_locked.as_deref_mut().unwrap();

            std::io::Write::flush(v)
        }
    }

    impl tokio::io::AsyncWrite for BufWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, io::Error>> {
            let result = self.write_vec(buf);

            Ready(result)
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            // NOP
            Ready(Ok(()))
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            // NOP
            Ready(Ok(()))
        }
    }

    impl ToString for BufWriter {
        fn to_string(&self) -> String {
            let data_ref = self.data.clone();
            let output = data_ref.lock().unwrap();
            let s = (*output).clone();
            let output = String::from_utf8(s).unwrap();

            output
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_interruptable_io_copier() {
        #[derive(Debug)]
        struct TestData {
            reader_value: String,
            result: io::Result<u64>,
        }

        let tests = &[
            TestData {
                reader_value: "".into(),
                result: Ok(0),
            },
            TestData {
                reader_value: "a".into(),
                result: Ok(1),
            },
            TestData {
                reader_value: "foo".into(),
                result: Ok(3),
            },
            TestData {
                reader_value: "b".repeat(BUF_SIZE - 1),
                result: Ok((BUF_SIZE - 1) as u64),
            },
            TestData {
                reader_value: "c".repeat(BUF_SIZE),
                result: Ok((BUF_SIZE) as u64),
            },
            TestData {
                reader_value: "d".repeat(BUF_SIZE + 1),
                result: Ok((BUF_SIZE + 1) as u64),
            },
            TestData {
                reader_value: "e".repeat((2 * BUF_SIZE) - 1),
                result: Ok(((2 * BUF_SIZE) - 1) as u64),
            },
            TestData {
                reader_value: "f".repeat(2 * BUF_SIZE),
                result: Ok((2 * BUF_SIZE) as u64),
            },
            TestData {
                reader_value: "g".repeat((2 * BUF_SIZE) + 1),
                result: Ok(((2 * BUF_SIZE) + 1) as u64),
            },
        ];

        for (i, d) in tests.iter().enumerate() {
            // Create a string containing details of the test
            let msg = format!("test[{}]: {:?}", i, d);

            let (tx, rx) = channel(true);
            let reader = Cursor::new(d.reader_value.clone());
            let writer = BufWriter::new();

            // XXX: Pass a copy of the writer to the copier to allow the
            // result of the write operation to be checked below.
            let handle = tokio::spawn(interruptable_io_copier(reader, writer.clone(), rx));

            // Allow time for the thread to be spawned.
            // FIXME: testing
            tokio::time::sleep(Duration::from_secs(1)).await;

            let timeout = tokio::time::sleep(Duration::from_secs(1));
            pin!(timeout);

            tx.send(true).expect("failed to request shutdown");

            let spawn_result: std::result::Result<
                std::result::Result<u64, std::io::Error>,
                JoinError,
            >;

            let result: std::result::Result<u64, std::io::Error>;

            select! {
                res = handle => spawn_result = res,
                _ = &mut timeout => panic!(format!("{:?} timed out", msg)),
            }

            assert!(spawn_result.is_ok());

            result = spawn_result.unwrap();

            match result {
                Ok(actual_byte_count) => {
                    assert!(d.result.is_ok());

                    let expected_byte_count = *d.result.as_ref().unwrap();

                    assert_eq!(expected_byte_count, actual_byte_count, "{}", msg);

                    let expected_value = d.reader_value.clone();
                    let actual_value = writer.to_string();

                    assert_eq!(actual_value, expected_value, "{}", msg);
                }

                Err(e) => {
                    assert!(d.result.is_err(), "{}: error: {:?}", msg, e);
                }
            }
        }
    }
}
