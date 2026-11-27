//! Compression of a volume on its way between two agents.
//!
//! rsync sends files as they are, and TLS compresses nothing. The agent that holds the data
//! therefore packs what its rsync writes (zstd, one frame for a pass over the volume), and the
//! receiving agent unpacks it for its own rsync. Only that direction is packed: the other one
//! carries a few bytes of requests.
//!
//! The stream of rsync is a dialogue, so nothing may wait inside the packer for more input:
//! whatever was read is on the wire before the next read blocks.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

use futures_util::FutureExt;
use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use zstd::stream::raw::{Decoder, Encoder, InBuffer, Operation, OutBuffer};

/// The name of zstd in the conversation of two agents.
pub const ZSTD: &str = "zstd";

/// Levels below zero trade ratio for speed; the ones above 19 take memory no agent should.
pub const ZSTD_LEVELS: std::ops::RangeInclusive<i32> = -7..=19;

/// How the node that holds the data packs what it sends.
#[derive(Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    None,
    #[default]
    Zstd,
}

/// What came through for a volume, counted on the receiving node.
#[derive(Default, Debug)]
pub struct Traffic {
    /// Bytes of the stream of rsync: what the copy would have taken of the network as it is.
    pub uncompressed: AtomicU64,
    /// Bytes that crossed the network.
    pub compressed: AtomicU64,
}

impl Traffic {
    fn add(&self, uncompressed: usize, compressed: usize) {
        self.uncompressed.fetch_add(uncompressed as u64, Ordering::Relaxed);
        self.compressed.fetch_add(compressed as u64, Ordering::Relaxed);
    }

    /// Uncompressed and compressed bytes so far.
    pub fn totals(&self) -> (u64, u64) {
        (self.uncompressed.load(Ordering::Relaxed), self.compressed.load(Ordering::Relaxed))
    }
}

/// Bytes handled in one step: large enough for the packer to see repetitions across writes of
/// rsync, small enough to be nothing next to the memory of an agent.
const CHUNK: usize = 256 << 10;

/// Waits for bytes, then takes the ones that are there already as well. Zero is the end.
async fn gather<R: AsyncRead + Unpin>(reader: &mut R, buffer: &mut [u8]) -> io::Result<usize> {
    let mut filled = reader.read(buffer).await?;
    while filled > 0 && filled < buffer.len() {
        match reader.read(&mut buffer[filled..]).now_or_never() {
            Some(Ok(more)) if more > 0 => filled += more,
            Some(Err(e)) => return Err(e),
            // Nothing more for now, or the end: the next call reports it.
            _ => break,
        }
    }
    Ok(filled)
}

/// Sends `reader` to `writer` packed with zstd at `level`, to the end of `reader`.
pub async fn pack<R, W>(reader: &mut R, writer: &mut W, level: i32, traffic: &Traffic) -> io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut encoder = Encoder::new(level)?;
    let mut input = vec![0u8; CHUNK];
    let mut output = vec![0u8; CHUNK];
    loop {
        let filled = gather(reader, &mut input).await?;
        let mut source = InBuffer::around(&input[..filled]);
        loop {
            let mut target = OutBuffer::around(&mut output[..]);
            // What the encoder still holds back after this step.
            let held = if source.pos < filled {
                encoder.run(&mut source, &mut target)?;
                // Fed: what it kept is asked for by the flush that follows.
                1
            } else if filled == 0 {
                encoder.finish(&mut target, true)?
            } else {
                encoder.flush(&mut target)?
            };
            let packed = target.pos();
            writer.write_all(&output[..packed]).await?;
            traffic.add(0, packed);
            if held == 0 {
                break;
            }
        }
        traffic.add(filled, 0);
        writer.flush().await?;
        if filled == 0 {
            return Ok(());
        }
    }
}

/// Sends what [`pack`] wrote into `reader` to `writer` as it was, to the end of `reader`. A
/// stream that ends before the packer has finished it is an error.
pub async fn unpack<R, W>(reader: &mut R, writer: &mut W, traffic: &Traffic) -> io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut decoder = Decoder::new()?;
    let mut input = vec![0u8; CHUNK];
    let mut output = vec![0u8; CHUNK];
    // Whether the stream may end here: nothing has come yet, or its frame is complete.
    let mut whole = true;
    loop {
        let filled = gather(reader, &mut input).await?;
        if filled == 0 {
            break;
        }
        traffic.add(0, filled);
        let mut source = InBuffer::around(&input[..filled]);
        loop {
            let mut target = OutBuffer::around(&mut output[..]);
            let consumed = source.pos;
            let expects = decoder.run(&mut source, &mut target)?;
            let unpacked = target.pos();
            // A step that had nothing to do says how much the next frame would need, not
            // whether this one is complete.
            if source.pos > consumed || unpacked > 0 {
                whole = expects == 0;
            }
            writer.write_all(&output[..unpacked]).await?;
            traffic.add(unpacked, 0);
            // A buffer filled to the brim may have left more inside the decoder.
            if source.pos == filled && unpacked < output.len() {
                break;
            }
        }
        writer.flush().await?;
    }
    if whole { Ok(()) } else { Err(io::Error::new(io::ErrorKind::UnexpectedEof, "the packed stream was cut short")) }
}

/// Sends `reader` to `writer` as it is, to the end of `reader`.
pub async fn relay<R, W>(reader: &mut R, writer: &mut W, traffic: &Traffic) -> io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buffer = vec![0u8; CHUNK];
    loop {
        let filled = gather(reader, &mut buffer).await?;
        if filled == 0 {
            return Ok(());
        }
        writer.write_all(&buffer[..filled]).await?;
        writer.flush().await?;
        traffic.add(filled, filled);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;

    /// Bytes no packer makes shorter.
    fn noise(length: usize) -> Vec<u8> {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        (0..length)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 32) as u8
            })
            .collect()
    }

    /// Packs `data` on one side of a connection and unpacks it on the other. Returns what came
    /// out, and what each side counted.
    async fn through(data: Vec<u8>, level: i32) -> (Vec<u8>, (u64, u64), (u64, u64)) {
        let (mut wire_in, mut wire_out) = tokio::io::duplex(64 << 10);
        let (sent, received) = (Traffic::default(), Traffic::default());
        let sender = async {
            pack(&mut data.as_slice(), &mut wire_in, level, &sent).await.unwrap();
            wire_in.shutdown().await.unwrap();
        };
        let mut arrived = Vec::new();
        let receiver = async { unpack(&mut wire_out, &mut arrived, &received).await.unwrap() };
        tokio::join!(sender, receiver);
        (arrived, sent.totals(), received.totals())
    }

    #[tokio::test]
    async fn what_is_packed_comes_out_as_it_went_in() {
        // Text that repeats, then noise, then a long run of one byte: larger than one step.
        let mut data = b"the quick brown fox jumps over the lazy dog\n".repeat(20_000);
        data.extend(noise(700_000));
        data.extend(vec![0u8; 1 << 20]);
        for level in [-7, 1, 19] {
            let (arrived, sent, received) = through(data.clone(), level).await;
            assert!(arrived == data, "level {level}: the data differs");
            assert_eq!(sent.0, data.len() as u64, "level {level}: the sender counts what it read");
            assert_eq!(received.0, data.len() as u64, "level {level}: the receiver counts what it handed on");
            assert_eq!(sent.1, received.1, "level {level}: both sides saw the same bytes on the wire");
        }
    }

    #[tokio::test]
    async fn data_that_repeats_takes_less_of_the_network_and_noise_takes_about_its_size() {
        let text = b"the quick brown fox jumps over the lazy dog\n".repeat(50_000);
        let (_, _, (uncompressed, compressed)) = through(text.clone(), 1).await;
        assert_eq!(uncompressed, text.len() as u64);
        assert!(compressed * 20 < uncompressed, "{compressed} of {uncompressed}");

        let (_, _, (uncompressed, compressed)) = through(noise(2 << 20), 1).await;
        assert!(compressed >= uncompressed, "noise does not shrink: {compressed} of {uncompressed}");
        assert!(compressed < uncompressed + uncompressed / 100, "and costs under 1%: {compressed} of {uncompressed}");
    }

    #[tokio::test]
    async fn nothing_packed_is_still_a_stream_that_ends_well() {
        let (arrived, sent, received) = through(Vec::new(), 1).await;
        assert!(arrived.is_empty());
        assert_eq!((sent.0, received.0), (0, 0));
    }

    /// rsync waits for the answer to what it has just said: bytes must not sit in the packer.
    #[tokio::test]
    async fn every_message_arrives_while_the_stream_stays_open() {
        let (mut plain_in, mut plain_out) = tokio::io::duplex(1024);
        let (mut wire_in, mut wire_out) = tokio::io::duplex(1024);
        let (mut arrived_in, mut arrived_out) = tokio::io::duplex(1024);
        let traffic = Arc::new(Traffic::default());
        let packing = tokio::spawn({
            let traffic = traffic.clone();
            async move { pack(&mut plain_out, &mut wire_in, 1, &traffic).await }
        });
        let unpacking = tokio::spawn({
            let traffic = traffic.clone();
            async move { unpack(&mut wire_out, &mut arrived_in, &traffic).await }
        });
        for message in [&b"hello"[..], b"again", b"!"] {
            plain_in.write_all(message).await.unwrap();
            let mut arrived = vec![0u8; message.len()];
            tokio::time::timeout(Duration::from_secs(5), arrived_out.read_exact(&mut arrived))
                .await
                .expect("the message is held back")
                .unwrap();
            assert_eq!(arrived, message);
        }
        packing.abort();
        unpacking.abort();
    }

    /// The end of a stream is told from a cut by asking the decoder; it must be asked the right
    /// moment also when the last bytes fill a step to the brim.
    #[tokio::test]
    async fn stream_whose_end_falls_on_the_edge_of_a_step_ends_well() {
        for length in [CHUNK / 2, CHUNK, 2 * CHUNK, 3 * CHUNK] {
            for data in [noise(length), vec![0u8; length]] {
                let mut packed = Vec::new();
                pack(&mut data.as_slice(), &mut packed, 1, &Traffic::default()).await.unwrap();
                let (mut arrived, traffic) = (Vec::new(), Traffic::default());
                let unpacked = unpack(&mut packed.as_slice(), &mut arrived, &traffic).await;
                assert!(unpacked.is_ok(), "{length} bytes: {unpacked:?}");
                assert!(arrived == data, "{length} bytes: the data differs");
                assert_eq!(traffic.totals(), (length as u64, packed.len() as u64));
            }
        }
    }

    #[tokio::test]
    async fn stream_cut_short_is_an_error() {
        let data = b"the quick brown fox jumps over the lazy dog\n".repeat(20_000);
        let mut packed = Vec::new();
        pack(&mut data.as_slice(), &mut packed, 1, &Traffic::default()).await.unwrap();
        let cut = &packed[..packed.len() / 2];
        let error = unpack(&mut &cut[..], &mut Vec::new(), &Traffic::default()).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        // What is not a packed stream at all.
        assert!(unpack(&mut &b"rsync speaks here"[..], &mut Vec::new(), &Traffic::default()).await.is_err());
    }

    #[tokio::test]
    async fn stream_sent_as_it_is_counts_the_same_on_both_scales() {
        let data = noise(600_000);
        let (mut arrived, traffic) = (Vec::new(), Traffic::default());
        relay(&mut data.as_slice(), &mut arrived, &traffic).await.unwrap();
        assert!(arrived == data);
        assert_eq!(traffic.totals(), (600_000, 600_000));
    }
}
