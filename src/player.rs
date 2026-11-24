use crate::opus::OpusStream;
use crate::ytm;
use std::collections::{HashMap, VecDeque};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering::Relaxed};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use symphonia::core::audio::SampleBuffer;
use symphonia::core::codecs::{Decoder, DecoderOptions};
use symphonia::core::errors::Error as DecodeError;
use symphonia::core::formats::{FormatOptions, FormatReader, SeekMode, SeekTo};
use symphonia::core::io::{MediaSource, MediaSourceStream};
use symphonia::core::meta::MetadataOptions;
use symphonia::core::probe::Hint;
use symphonia::core::units::{Time, TimeBase};

const RANGE_BYTES: u64 = 1024 * 1024;
const WINDOW_BYTES: u64 = 8 * 1024 * 1024;
const KEPT_BEHIND: u64 = 1024 * 1024;
const BUFFERED_AHEAD: u64 = WINDOW_BYTES - KEPT_BEHIND - RANGE_BYTES;
const FULL_POLL: Duration = Duration::from_millis(10);
const PAUSED_POLL: Duration = Duration::from_millis(250);
const WINDOW_POLL: Duration = Duration::from_millis(200);
const MAX_STALLS: u32 = 5;
const QUEUED_CHUNKS: usize = 24;
const PRE_ROLL_MS: u64 = 80;
const SILENCE_SAMPLES: usize = 512;
const URL_LIFETIME: Duration = Duration::from_secs(3 * 60 * 60);
const URL_CACHE_ENTRIES: usize = 64;
const SELFTEST_FAR_SEEK_MS: u64 = 20 * 60 * 1000;
const DEVICE_POLL: Duration = Duration::from_secs(2);
const LOAD_RETRIES: u32 = 3;
const STALL_AFTER: Duration = Duration::from_millis(700);
const RECOVERED_AFTER_MS: u64 = 5000;
const NO_OUTPUT: &str = "No audio output device was found";
const AUDIO_LOG: &str = "audio.log";
const AUDIO_LOG_BYTES: u64 = 512 * 1024;
const DIAGNOSTIC_EVENTS: usize = 256;
const NETWORK_WAIT_LOG_MS: u64 = 25;
const PACKET_WAIT_LOG_MS: u64 = 25;
const DECODE_WAIT_LOG_MS: u64 = 10;
const AUDIO_LATE_LOG_MS: u64 = 25;
const SCHEDULER_HEARTBEAT: Duration = Duration::from_millis(100);
const SCHEDULER_LATE_LOG_MS: u64 = 150;
const AUDIO_HEALTH_EVERY: Duration = Duration::from_secs(1);

#[derive(Clone)]
struct AudioDiagnostics {
    events: SyncSender<(u64, Diagnostic)>,
    started: Instant,
}

#[derive(Debug)]
enum Diagnostic {
    Session,
    TrackLoad {
        video_id: String,
    },
    TrackOpened {
        video_id: String,
    },
    AudioHealth {
        queue: u32,
        position_ms: u64,
        buffered: u32,
        rate: u32,
        channels: u16,
        paused: bool,
    },
    NetworkWait {
        elapsed_ms: u64,
    },
    PacketWait {
        elapsed_ms: u64,
        queue: u32,
    },
    DecodeWait {
        elapsed_ms: u64,
        queue: u32,
    },
    AudioLate {
        late_ms: u64,
        queue: u32,
    },
    QueueStarved {
        queue: u32,
    },
    QueueRecovered {
        queue: u32,
        elapsed_ms: u64,
    },
    DeviceError(String),
}

impl AudioDiagnostics {
    fn start() -> Option<Self> {
        let folder = eframe::storage_dir(crate::app_id())?;
        let file = folder.join(AUDIO_LOG);
        let grown = std::fs::metadata(&file).is_ok_and(|log| log.len() > AUDIO_LOG_BYTES);
        std::fs::create_dir_all(&folder).ok()?;
        let (events, inbox) = mpsc::sync_channel(DIAGNOSTIC_EVENTS);
        std::thread::spawn(move || {
            let Ok(log) = std::fs::OpenOptions::new()
                .create(true)
                .append(!grown)
                .write(true)
                .truncate(grown)
                .open(file)
            else {
                return;
            };
            let mut log = std::io::BufWriter::new(log);
            let mut lines = 0u64;
            let mut heartbeat = Instant::now();
            loop {
                let received = inbox.recv_timeout(SCHEDULER_HEARTBEAT);
                let heartbeat_ms = heartbeat.elapsed().as_millis() as u64;
                if heartbeat_ms >= SCHEDULER_LATE_LOG_MS {
                    let elapsed_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map_or(0, |elapsed| elapsed.as_millis());
                    let _ = writeln!(log, "unix_ms={elapsed_ms} scheduler_late elapsed_ms={heartbeat_ms}");
                    let _ = log.flush();
                    lines += 1;
                    if lines.is_multiple_of(128) && log.get_ref().metadata().is_ok_and(|metadata| metadata.len() > AUDIO_LOG_BYTES) {
                        let _ = log.flush();
                        let _ = log.get_mut().set_len(0);
                        let _ = log.get_mut().seek(SeekFrom::Start(0));
                    }
                }
                heartbeat = Instant::now();
                let (elapsed_ms, event) = match received {
                    Ok(event) => event,
                    Err(RecvTimeoutError::Timeout) => continue,
                    Err(RecvTimeoutError::Disconnected) => return,
                };
                let message = match event {
                    Diagnostic::Session => format!("session version={} platform={}", crate::VERSION, std::env::consts::OS),
                    Diagnostic::TrackLoad { video_id } => format!("track_load video_id={video_id}"),
                    Diagnostic::TrackOpened { video_id } => format!("track_opened video_id={video_id}"),
                    Diagnostic::AudioHealth {
                        queue,
                        position_ms,
                        buffered,
                        rate,
                        channels,
                        paused,
                    } => format!(
                        "audio_health queue={queue} position_ms={position_ms} buffered={buffered} rate={rate} channels={channels} paused={paused}"
                    ),
                    Diagnostic::NetworkWait { elapsed_ms } => format!("network_wait elapsed_ms={elapsed_ms}"),
                    Diagnostic::PacketWait { elapsed_ms, queue } => {
                        format!("packet_wait elapsed_ms={elapsed_ms} queue={queue}")
                    }
                    Diagnostic::DecodeWait { elapsed_ms, queue } => {
                        format!("decode_wait elapsed_ms={elapsed_ms} queue={queue}")
                    }
                    Diagnostic::AudioLate { late_ms, queue } => format!("audio_late late_ms={late_ms} queue={queue}"),
                    Diagnostic::QueueStarved { queue } => format!("queue_starved queue={queue}"),
                    Diagnostic::QueueRecovered {
                        queue,
                        elapsed_ms: stalled,
                    } => {
                        format!("queue_recovered stalled_ms={stalled} queue={queue}")
                    }
                    Diagnostic::DeviceError(message) => format!("device_error message={message:?}"),
                };
                let unix_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(0, |elapsed| elapsed.as_millis());
                if writeln!(log, "unix_ms={unix_ms} since_start_ms={elapsed_ms} {message}").is_ok() {
                    let _ = log.flush();
                }
                lines += 1;
                if lines.is_multiple_of(128) && log.get_ref().metadata().is_ok_and(|metadata| metadata.len() > AUDIO_LOG_BYTES) {
                    let _ = log.flush();
                    let _ = log.get_mut().set_len(0);
                    let _ = log.get_mut().seek(SeekFrom::Start(0));
                }
            }
        });
        let diagnostics = Self {
            events,
            started: Instant::now(),
        };
        diagnostics.record(Diagnostic::Session);
        Some(diagnostics)
    }

    fn record(&self, event: Diagnostic) {
        let elapsed_ms = self.started.elapsed().as_millis() as u64;
        let _ = self.events.try_send((elapsed_ms, event));
    }

    #[cfg(test)]
    fn channel() -> (Self, Receiver<(u64, Diagnostic)>) {
        let (events, inbox) = mpsc::sync_channel(DIAGNOSTIC_EVENTS);
        (
            Self {
                events,
                started: Instant::now(),
            },
            inbox,
        )
    }
}

fn retry_delay(retry: u32) -> Option<Duration> {
    (1..=LOAD_RETRIES).contains(&retry).then(|| Duration::from_secs(1 << (retry - 1)))
}

type Wake = Arc<dyn Fn() + Send + Sync>;
type Chunks = Arc<Mutex<Receiver<Chunk>>>;
type Urls = Arc<Mutex<HashMap<String, (Instant, ytm::Stream)>>>;

#[derive(Default)]
struct Shared {
    generation: AtomicU64,
    epoch: AtomicU64,
    position_ms: AtomicU64,
    duration_ms: AtomicU64,
    buffered: AtomicU32,
    queued: AtomicU32,
    pause_epoch: AtomicU64,
    loading: AtomicBool,
    buffering: AtomicBool,
    paused: AtomicBool,
    ended: AtomicBool,
    failed: AtomicBool,
    error: Mutex<Option<ytm::StreamError>>,
}

struct Chunk {
    samples: Vec<f32>,
    start_ms: u64,
    epoch: u64,
    rate: u32,
    channels: u16,
    end: bool,
}

enum Cmd {
    Load {
        video_id: String,
        generation: u64,
        epoch: u64,
        resume_ms: Option<u64>,
        retry: u32,
    },
    Opened(Track),
    Failed {
        generation: u64,
        message: ytm::StreamError,
    },
    Seek {
        ms: u64,
        epoch: u64,
    },
}

#[derive(Default)]
struct Window {
    start: u64,
    bytes: VecDeque<u8>,
    reader: u64,
}

impl Window {
    fn end(&self) -> u64 {
        self.start + self.bytes.len() as u64
    }
}

struct Fetch {
    window: Mutex<Window>,
    changed: Condvar,
    len: u64,
    failed: AtomicBool,
    cancelled: AtomicBool,
    generation: u64,
    shared: Arc<Shared>,
    diagnostics: Option<AudioDiagnostics>,
}

impl Fetch {
    fn start(
        yt: &ytm::Client,
        url: String,
        len: u64,
        generation: u64,
        shared: Arc<Shared>,
        diagnostics: Option<AudioDiagnostics>,
    ) -> Arc<Fetch> {
        let (agent, user_agent) = (yt.agent.clone(), yt.player_agent());
        let window = Window {
            bytes: VecDeque::with_capacity(len.min(WINDOW_BYTES) as usize),
            ..Window::default()
        };
        let fetch = Arc::new(Fetch {
            window: Mutex::new(window),
            changed: Condvar::new(),
            len,
            failed: AtomicBool::new(false),
            cancelled: AtomicBool::new(false),
            generation,
            shared,
            diagnostics,
        });
        let worker = fetch.clone();
        std::thread::spawn(move || {
            let complete = worker.download(&agent, &url, &user_agent);
            worker.failed.store(!complete, Relaxed);
            worker.changed.notify_all();
        });
        fetch
    }

    fn current(&self) -> bool {
        self.shared.generation.load(Relaxed) == self.generation && !self.cancelled.load(Relaxed)
    }

    fn next_range(&self) -> Option<u64> {
        let windowed = self.len > WINDOW_BYTES;
        let mut window = self.window.lock().unwrap();
        while self.current() {
            if windowed {
                if window.reader < window.start || window.reader > window.end() + RANGE_BYTES {
                    window.bytes.clear();
                    window.start = window.reader;
                }
                let played = window
                    .reader
                    .saturating_sub(window.start + KEPT_BEHIND)
                    .min(window.bytes.len() as u64);
                window.bytes.drain(..played as usize);
                window.start += played;
            }
            let end = window.end();
            if end < self.len && (!windowed || end < window.reader + BUFFERED_AHEAD) {
                return Some(end);
            }
            if !windowed {
                return None;
            }
            window = self.changed.wait_timeout(window, WINDOW_POLL).unwrap().0;
        }
        None
    }

    fn download(&self, agent: &ureq::Agent, url: &str, user_agent: &str) -> bool {
        let mut stalls = 0;
        let mut block = [0u8; 16 * 1024];
        while let Some(from) = self.next_range() {
            let mut at = from;
            let to = (from + RANGE_BYTES).min(self.len) - 1;
            let request = agent.get(&format!("{url}&range={from}-{to}")).set("User-Agent", user_agent);
            if let Ok(response) = request.call() {
                let mut body = response.into_reader();
                while let Ok(read) = body.read(&mut block) {
                    if read == 0 || !self.current() {
                        break;
                    }
                    self.window.lock().unwrap().bytes.extend(&block[..read]);
                    at += read as u64;
                    self.changed.notify_all();
                    self.shared.buffered.store((at * 1000 / self.len) as u32, Relaxed);
                }
            }
            if at > from {
                stalls = 0;
                continue;
            }
            stalls += 1;
            if stalls >= MAX_STALLS {
                return false;
            }
            std::thread::sleep(Duration::from_millis(400 * stalls as u64));
        }
        true
    }
}

struct Reader {
    fetch: Arc<Fetch>,
    at: u64,
    seekable: Arc<AtomicBool>,
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.fetch.cancelled.store(true, Relaxed);
        self.fetch.changed.notify_all();
    }
}

impl Read for Reader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let mut window = self.fetch.window.lock().unwrap();
        let mut waiting: Option<Instant> = None;
        loop {
            window.reader = self.at;
            if self.at >= window.start && self.at < window.end() {
                let offset = (self.at - window.start) as usize;
                let mut count = 0;
                for (slot, byte) in out.iter_mut().zip(window.bytes.range(offset..)) {
                    *slot = *byte;
                    count += 1;
                }
                self.at += count as u64;
                window.reader = self.at;
                drop(window);
                if let (Some(waiting), Some(diagnostics)) = (waiting, &self.fetch.diagnostics) {
                    let elapsed_ms = waiting.elapsed().as_millis() as u64;
                    if elapsed_ms >= NETWORK_WAIT_LOG_MS {
                        diagnostics.record(Diagnostic::NetworkWait { elapsed_ms });
                    }
                }
                self.fetch.changed.notify_all();
                return Ok(count);
            }
            if !self.fetch.current() {
                return Err(io::Error::other("cancelled"));
            }
            if self.at >= self.fetch.len {
                return Ok(0);
            }
            if self.fetch.failed.load(Relaxed) {
                return Err(io::Error::other("The connection was lost while streaming"));
            }
            waiting.get_or_insert_with(Instant::now);
            self.fetch.changed.notify_all();
            window = self.fetch.changed.wait_timeout(window, WINDOW_POLL).unwrap().0;
        }
    }
}

impl Seek for Reader {
    fn seek(&mut self, to: SeekFrom) -> io::Result<u64> {
        let target = match to {
            SeekFrom::Start(offset) => offset as i64,
            SeekFrom::Current(delta) => self.at as i64 + delta,
            SeekFrom::End(delta) => self.fetch.len as i64 + delta,
        };
        self.at = target.clamp(0, self.fetch.len as i64) as u64;
        Ok(self.at)
    }
}

impl MediaSource for Reader {
    fn is_seekable(&self) -> bool {
        self.seekable.load(Relaxed)
    }

    fn byte_len(&self) -> Option<u64> {
        Some(self.fetch.len)
    }
}

struct Track {
    video_id: String,
    generation: u64,
    format: Box<dyn FormatReader>,
    decoder: Box<dyn Decoder>,
    id: u32,
    time_base: Option<TimeBase>,
    buffer: Option<SampleBuffer<f32>>,
    trim_ms: Option<u64>,
    finished: bool,
    shared: Arc<Shared>,
    diagnostics: Option<AudioDiagnostics>,
}

impl Track {
    fn open(yt: &ytm::Client, urls: &Urls, video_id: &str, generation: u64, shared: &Arc<Shared>) -> Result<Track, String> {
        Self::open_if(yt, urls, video_id, generation, shared, None, &|| true)
            .map_err(|error| error.message)?
            .ok_or_else(|| "Stream resolution was cancelled".to_owned())
    }

    fn open_if(
        yt: &ytm::Client,
        urls: &Urls,
        video_id: &str,
        generation: u64,
        shared: &Arc<Shared>,
        diagnostics: Option<AudioDiagnostics>,
        current: &impl Fn() -> bool,
    ) -> Result<Option<Track>, ytm::StreamError> {
        let Some(mut stream) = resolve_if(yt, urls, video_id, current)? else {
            return Ok(None);
        };
        if !current() {
            return Ok(None);
        }
        let fallback = stream.fallback.take();
        match Self::open_stream(yt, stream, video_id, generation, shared, diagnostics.clone()) {
            Ok(track) if current() => Ok(Some(track)),
            Ok(_) => Ok(None),
            Err(_) if !current() => Ok(None),
            Err(message) => match fallback {
                Some(aac) if current() => Self::open_stream(yt, *aac, video_id, generation, shared, diagnostics)
                    .map(Some)
                    .map_err(Into::into),
                Some(_) => Ok(None),
                None => Err(message.into()),
            },
        }
    }

    fn open_stream(
        yt: &ytm::Client,
        stream: ytm::Stream,
        video_id: &str,
        generation: u64,
        shared: &Arc<Shared>,
        diagnostics: Option<AudioDiagnostics>,
    ) -> Result<Track, String> {
        let ytm::Stream {
            url,
            len,
            duration_ms,
            opus,
            ..
        } = stream;
        if shared.generation.load(Relaxed) == generation {
            shared.duration_ms.store(duration_ms, Relaxed);
        }
        let seekable = Arc::new(AtomicBool::new(false));
        let fetch = Fetch::start(yt, url, len, generation, shared.clone(), diagnostics.clone());
        let reader = Reader {
            fetch,
            at: 0,
            seekable: seekable.clone(),
        };
        let source = MediaSourceStream::new(Box::new(reader), Default::default());
        let mut hint = Hint::new();
        hint.with_extension(if opus { "webm" } else { "m4a" });
        let format = symphonia::default::get_probe()
            .format(&hint, source, &FormatOptions::default(), &MetadataOptions::default())
            .map_err(|e| e.to_string())?
            .format;
        seekable.store(true, Relaxed);
        let track = format.default_track().ok_or("This stream has no audio track")?;
        let (id, time_base) = (track.id, track.codec_params.time_base);
        let options = DecoderOptions::default();
        let decoder: Box<dyn Decoder> = if opus {
            Box::new(OpusStream::try_new(&track.codec_params, &options).map_err(|e| e.to_string())?)
        } else {
            symphonia::default::get_codecs()
                .make(&track.codec_params, &options)
                .map_err(|e| e.to_string())?
        };
        Ok(Track {
            video_id: video_id.to_owned(),
            generation,
            format,
            decoder,
            id,
            time_base,
            buffer: None,
            trim_ms: None,
            finished: false,
            shared: shared.clone(),
            diagnostics,
        })
    }

    fn next_chunk(&mut self, epoch: u64) -> Result<Option<Chunk>, String> {
        loop {
            let packet_started = Instant::now();
            let packet = match self.format.next_packet() {
                Ok(packet) => packet,
                Err(DecodeError::IoError(e)) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(e) => return Err(e.to_string()),
            };
            let packet_ms = packet_started.elapsed().as_millis() as u64;
            if packet_ms >= PACKET_WAIT_LOG_MS
                && let Some(diagnostics) = &self.diagnostics
            {
                diagnostics.record(Diagnostic::PacketWait {
                    elapsed_ms: packet_ms,
                    queue: self.shared.queued.load(Relaxed),
                });
            }
            if packet.track_id() != self.id {
                continue;
            }
            let decode_started = Instant::now();
            let decoded = match self.decoder.decode(&packet) {
                Ok(decoded) => decoded,
                Err(DecodeError::DecodeError(_)) => continue,
                Err(e) => return Err(e.to_string()),
            };
            let decode_ms = decode_started.elapsed().as_millis() as u64;
            if decode_ms >= DECODE_WAIT_LOG_MS
                && let Some(diagnostics) = &self.diagnostics
            {
                diagnostics.record(Diagnostic::DecodeWait {
                    elapsed_ms: decode_ms,
                    queue: self.shared.queued.load(Relaxed),
                });
            }
            let spec = *decoded.spec();
            let channels = spec.channels.count();
            let buffer = match &mut self.buffer {
                Some(buffer) if buffer.capacity() >= decoded.frames() * channels => buffer,
                slot => slot.insert(SampleBuffer::new(decoded.frames() as u64, spec)),
            };
            buffer.copy_interleaved_ref(decoded);
            let time = self.time_base.map(|base| base.calc_time(packet.ts()));
            let mut start_ms = time.map_or(0, |t| t.seconds * 1000 + (t.frac * 1000.0) as u64);
            let samples = buffer.samples();
            let mut from = 0;
            if let Some(target) = self.trim_ms {
                let end_ms = start_ms + (samples.len() / channels) as u64 * 1000 / spec.rate as u64;
                if end_ms <= target {
                    continue;
                }
                self.trim_ms = None;
                if start_ms < target {
                    from = ((target - start_ms) * spec.rate as u64 / 1000) as usize * channels;
                    start_ms = target;
                }
            }
            return Ok(Some(Chunk {
                samples: samples[from.min(samples.len())..].to_vec(),
                start_ms,
                epoch,
                rate: spec.rate,
                channels: spec.channels.count() as u16,
                end: false,
            }));
        }
    }

    fn seek(&mut self, ms: u64) {
        let from = ms.saturating_sub(PRE_ROLL_MS);
        let time = Time::new(from / 1000, (from % 1000) as f64 / 1000.0);
        if self
            .format
            .seek(
                SeekMode::Accurate,
                SeekTo::Time {
                    time,
                    track_id: Some(self.id),
                },
            )
            .is_ok()
        {
            self.decoder.reset();
            self.trim_ms = self.time_base.map(|_| ms);
            self.finished = false;
        }
    }
}

fn resolve_if(yt: &ytm::Client, urls: &Urls, video_id: &str, current: &impl Fn() -> bool) -> Result<Option<ytm::Stream>, ytm::StreamError> {
    if !current() {
        return Ok(None);
    }
    if let Some((at, stream)) = urls.lock().unwrap().get(video_id)
        && at.elapsed() < URL_LIFETIME
    {
        return Ok(Some(stream.clone()));
    }
    let Some(stream) = yt.stream_if_current(video_id, current)? else {
        return Ok(None);
    };
    if !current() {
        return Ok(None);
    }
    let mut urls = urls.lock().unwrap();
    if !current() {
        return Ok(None);
    }
    if urls.len() >= URL_CACHE_ENTRIES {
        urls.clear();
    }
    urls.insert(video_id.to_owned(), (Instant::now(), stream.clone()));
    Ok(Some(stream))
}

struct PcmSource {
    chunks: Chunks,
    samples: Vec<f32>,
    at: usize,
    rate: u32,
    channels: u16,
    starved: Option<Instant>,
    health_at: Instant,
    refill_at: Option<Instant>,
    deadline_balance_ms: i64,
    deadline_reported: bool,
    pause_epoch: u64,
    shared: Arc<Shared>,
    wake: Wake,
    diagnostics: Option<AudioDiagnostics>,
}

impl PcmSource {
    fn new(chunks: Chunks, shared: Arc<Shared>, wake: Wake, diagnostics: Option<AudioDiagnostics>) -> Self {
        Self {
            chunks,
            samples: vec![0.0; SILENCE_SAMPLES],
            at: 0,
            rate: 44_100,
            channels: 2,
            starved: None,
            health_at: Instant::now(),
            refill_at: None,
            deadline_balance_ms: 0,
            deadline_reported: false,
            pause_epoch: shared.pause_epoch.load(Relaxed),
            shared,
            wake,
            diagnostics,
        }
    }

    fn refill(&mut self) {
        self.observe_deadline();
        self.record_health();
        self.at = 0;
        loop {
            let chunk = {
                let chunks = self.chunks.lock().unwrap();
                chunks.try_recv().ok()
            };
            let Some(chunk) = chunk else { break };
            if self.shared.queued.load(Relaxed) > 0 {
                self.shared.queued.fetch_sub(1, Relaxed);
            }
            if chunk.epoch != self.shared.epoch.load(Relaxed) {
                continue;
            }
            if chunk.end {
                self.recovered();
                self.shared.buffering.store(false, Relaxed);
                self.shared.ended.store(true, Relaxed);
                (self.wake)();
                break;
            }
            if chunk.samples.is_empty() {
                continue;
            }
            self.recovered();
            self.shared.buffering.store(false, Relaxed);
            self.shared.position_ms.store(chunk.start_ms, Relaxed);
            self.rate = chunk.rate;
            self.channels = chunk.channels;
            self.samples = chunk.samples;
            return;
        }
        self.samples.clear();
        self.samples.resize(SILENCE_SAMPLES, 0.0);
        let first_empty = self.starved.is_none();
        let since = *self.starved.get_or_insert_with(Instant::now);
        if first_empty && let Some(diagnostics) = &self.diagnostics {
            diagnostics.record(Diagnostic::QueueStarved {
                queue: self.shared.queued.load(Relaxed),
            });
        }
        if since.elapsed() >= STALL_AFTER && !self.shared.paused.load(Relaxed) && !self.shared.buffering.swap(true, Relaxed) {
            (self.wake)();
        }
    }

    fn record_health(&mut self) {
        if self.health_at.elapsed() < AUDIO_HEALTH_EVERY {
            return;
        }
        self.health_at = Instant::now();
        if let Some(diagnostics) = &self.diagnostics {
            diagnostics.record(Diagnostic::AudioHealth {
                queue: self.shared.queued.load(Relaxed),
                position_ms: self.shared.position_ms.load(Relaxed),
                buffered: self.shared.buffered.load(Relaxed),
                rate: self.rate,
                channels: self.channels,
                paused: self.shared.paused.load(Relaxed),
            });
        }
    }

    fn recovered(&mut self) {
        if let Some(starved) = self.starved.take()
            && let Some(diagnostics) = &self.diagnostics
        {
            diagnostics.record(Diagnostic::QueueRecovered {
                queue: self.shared.queued.load(Relaxed),
                elapsed_ms: starved.elapsed().as_millis() as u64,
            });
        }
    }

    fn observe_deadline(&mut self) {
        let now = Instant::now();
        let pause_epoch = self.shared.pause_epoch.load(Relaxed);
        if pause_epoch != self.pause_epoch {
            self.deadline_balance_ms = 0;
            self.deadline_reported = false;
        } else if let Some(previous) = self.refill_at {
            let expected_ms = self.samples.len() as u64 * 1000 / self.channels.max(1) as u64 / self.rate.max(1) as u64;
            let actual_ms = now.duration_since(previous).as_millis() as u64;
            self.deadline_balance_ms += actual_ms as i64 - expected_ms as i64;
            if self.deadline_balance_ms >= AUDIO_LATE_LOG_MS as i64 && !self.deadline_reported && !self.shared.paused.load(Relaxed) {
                self.deadline_reported = true;
                if let Some(diagnostics) = &self.diagnostics {
                    diagnostics.record(Diagnostic::AudioLate {
                        late_ms: self.deadline_balance_ms as u64,
                        queue: self.shared.queued.load(Relaxed),
                    });
                }
            } else if self.deadline_balance_ms <= 0 {
                self.deadline_reported = false;
            }
        }
        self.refill_at = Some(now);
        self.pause_epoch = pause_epoch;
    }
}

impl Iterator for PcmSource {
    type Item = f32;

    fn next(&mut self) -> Option<f32> {
        if self.at >= self.samples.len() {
            self.refill();
        }
        let sample = self.samples[self.at];
        self.at += 1;
        Some(sample)
    }
}

impl rodio::Source for PcmSource {
    fn current_span_len(&self) -> Option<usize> {
        Some(self.samples.len())
    }

    fn channels(&self) -> rodio::ChannelCount {
        self.channels
    }

    fn sample_rate(&self) -> rodio::SampleRate {
        self.rate
    }

    fn total_duration(&self) -> Option<Duration> {
        None
    }
}

struct DecodeWorker {
    shared: Arc<Shared>,
    yt: Arc<ytm::Client>,
    urls: Urls,
    wake: Wake,
    diagnostics: Option<AudioDiagnostics>,
}

fn decode_loop(commands: Receiver<Cmd>, replies: Sender<Cmd>, pcm: SyncSender<Chunk>, worker: DecodeWorker) {
    let DecodeWorker {
        shared,
        yt,
        urls,
        wake,
        diagnostics,
    } = worker;
    let mut track: Option<Track> = None;
    let mut pending: Option<Chunk> = None;
    let mut resume: Option<u64> = None;
    let mut epoch = 0;
    let mut recoveries = 0;
    let mut recovered_from: Option<u64> = None;
    loop {
        if let Some(chunk) = pending.take() {
            match pcm.try_send(chunk) {
                Ok(()) => {
                    shared.queued.fetch_add(1, Relaxed);
                }
                Err(TrySendError::Full(chunk)) => pending = Some(chunk),
                Err(TrySendError::Disconnected(_)) => return,
            }
        }
        let decoding = track.as_ref().is_some_and(|t| !t.finished);
        let command = if pending.is_some() {
            let wait = if shared.paused.load(Relaxed) { PAUSED_POLL } else { FULL_POLL };
            match commands.recv_timeout(wait) {
                Ok(command) => Some(command),
                Err(RecvTimeoutError::Timeout) => None,
                Err(RecvTimeoutError::Disconnected) => return,
            }
        } else if decoding {
            match commands.try_recv() {
                Ok(command) => Some(command),
                Err(TryRecvError::Empty) => None,
                Err(TryRecvError::Disconnected) => return,
            }
        } else {
            match commands.recv() {
                Ok(command) => Some(command),
                Err(_) => return,
            }
        };
        let current = |generation: u64| shared.generation.load(Relaxed) == generation;
        match command {
            Some(Cmd::Load {
                video_id,
                generation,
                epoch: started,
                resume_ms,
                retry,
            }) if current(generation) => {
                if resume_ms.is_none() {
                    (recoveries, recovered_from) = (0, None);
                }
                (track, pending, resume, epoch) = (None, None, resume_ms, started);
                let (replies, shared, yt, urls, diagnostics) =
                    (replies.clone(), shared.clone(), yt.clone(), urls.clone(), diagnostics.clone());
                std::thread::spawn(move || {
                    let is_current = || shared.generation.load(Relaxed) == generation;
                    let mut attempt = retry;
                    let opened = loop {
                        if shared.generation.load(Relaxed) != generation {
                            return;
                        }
                        if let Some(delay) = retry_delay(attempt) {
                            std::thread::sleep(delay);
                            if shared.generation.load(Relaxed) != generation {
                                return;
                            }
                        }
                        let opened = Track::open_if(&yt, &urls, &video_id, generation, &shared, diagnostics.clone(), &is_current).or_else(
                            |message| {
                                if message.unavailable {
                                    return Err(message);
                                }
                                urls.lock().unwrap().remove(&video_id);
                                if is_current() && yt.refresh_clients() {
                                    Track::open_if(&yt, &urls, &video_id, generation, &shared, diagnostics.clone(), &is_current)
                                } else {
                                    Err(message)
                                }
                            },
                        );
                        attempt += 1;
                        if opened.is_ok() || opened.as_ref().is_err_and(|error| error.unavailable) || retry_delay(attempt).is_none() {
                            break opened;
                        }
                    };
                    if shared.generation.load(Relaxed) != generation {
                        return;
                    }
                    let reply = match opened {
                        Ok(Some(track)) => Cmd::Opened(track),
                        Ok(None) => return,
                        Err(message) => {
                            urls.lock().unwrap().remove(&video_id);
                            Cmd::Failed { generation, message }
                        }
                    };
                    let _ = replies.send(reply);
                });
            }
            Some(Cmd::Opened(mut opened)) if current(opened.generation) => {
                if let Some(diagnostics) = &diagnostics {
                    diagnostics.record(Diagnostic::TrackOpened {
                        video_id: opened.video_id.clone(),
                    });
                }
                if let Some(ms) = resume.take() {
                    opened.seek(ms);
                }
                track = Some(opened);
                shared.loading.store(false, Relaxed);
                wake();
            }
            Some(Cmd::Failed { generation, message }) if current(generation) => {
                shared.failed.store(true, Relaxed);
                *shared.error.lock().unwrap() = Some(message);
                shared.loading.store(false, Relaxed);
                wake();
            }
            Some(Cmd::Seek { ms, epoch: flushed }) => {
                (pending, epoch) = (None, flushed);
                match &mut track {
                    Some(track) => track.seek(ms),
                    None => resume = Some(ms),
                }
            }
            _ => {}
        }
        if pending.is_some() {
            continue;
        }
        let Some(playing) = track.as_mut().filter(|t| !t.finished) else {
            continue;
        };
        match playing.next_chunk(epoch) {
            Ok(Some(chunk)) => {
                if recovered_from.is_some_and(|from| chunk.start_ms >= from + RECOVERED_AFTER_MS) {
                    (recoveries, recovered_from) = (0, None);
                }
                pending = Some(chunk);
            }
            Ok(None) => {
                playing.finished = true;
                pending = Some(Chunk {
                    samples: Vec::new(),
                    start_ms: 0,
                    epoch,
                    rate: 0,
                    channels: 0,
                    end: true,
                });
            }
            Err(message) => {
                if current(playing.generation) {
                    urls.lock().unwrap().remove(&playing.video_id);
                    if recoveries < LOAD_RETRIES {
                        recoveries += 1;
                        let resume_ms = shared.position_ms.load(Relaxed);
                        recovered_from = Some(resume_ms);
                        shared.loading.store(true, Relaxed);
                        let _ = replies.send(Cmd::Load {
                            video_id: playing.video_id.clone(),
                            generation: playing.generation,
                            epoch,
                            resume_ms: Some(resume_ms),
                            retry: 1,
                        });
                    } else {
                        shared.failed.store(true, Relaxed);
                        *shared.error.lock().unwrap() = Some(message.into());
                    }
                    wake();
                }
                track = None;
            }
        }
    }
}

fn default_device() -> Option<String> {
    use rodio::cpal::traits::{DeviceTrait, HostTrait};
    rodio::cpal::default_host().default_output_device()?.name().ok()
}

pub struct Speaker {
    stream: Option<rodio::OutputStream>,
    device: Option<String>,
    device_lost: Arc<AtomicBool>,
    device_polled: Instant,
}

impl Speaker {
    pub fn new(player: &mut Player) -> Self {
        let mut speaker = Self {
            stream: None,
            device: None,
            device_lost: Arc::default(),
            device_polled: Instant::now(),
        };
        speaker.open(player);
        if speaker.stream.is_none() {
            player.fail(NO_OUTPUT);
        }
        speaker
    }

    fn open(&mut self, player: &mut Player) {
        self.device_lost.store(false, Relaxed);
        let lost = self.device_lost.clone();
        let diagnostics = player.diagnostics.clone();
        let notice_loss = move |error| {
            lost.fetch_or(matches!(error, rodio::cpal::StreamError::DeviceNotAvailable), Relaxed);
            if let Some(diagnostics) = &diagnostics {
                diagnostics.record(Diagnostic::DeviceError(error.to_string()));
            }
        };
        let stream = rodio::OutputStreamBuilder::from_default_device()
            .and_then(|device| device.with_error_callback(notice_loss).open_stream_or_fallback())
            .or_else(|_| rodio::OutputStreamBuilder::open_default_stream());
        if let Err(error) = &stream
            && let Some(diagnostics) = &player.diagnostics
        {
            diagnostics.record(Diagnostic::DeviceError(error.to_string()));
        }
        self.stream = stream.ok().map(|mut stream| {
            stream.log_on_drop(false);
            player.connect(stream.mixer());
            stream
        });
        self.device = default_device().filter(|_| self.stream.is_some());
    }

    pub fn follow_default_device(&mut self, player: &mut Player) {
        if self.device_polled.elapsed() < DEVICE_POLL {
            return;
        }
        self.device_polled = Instant::now();
        if default_device() == self.device && !self.device_lost.load(Relaxed) {
            return;
        }
        let paused = player.paused();
        player.disconnect();
        let played = self.stream.take().is_some();
        self.open(player);
        player.set_paused(paused);
        if played && self.stream.is_none() {
            player.fail(NO_OUTPUT);
        }
    }
}

#[derive(Clone)]
pub struct Status(Arc<Shared>);

impl Status {
    pub fn paused(&self) -> bool {
        self.0.paused.load(Relaxed)
    }

    pub fn position_ms(&self) -> u64 {
        self.0.position_ms.load(Relaxed)
    }

    pub fn duration_ms(&self) -> u64 {
        self.0.duration_ms.load(Relaxed)
    }

    pub fn buffered(&self) -> f32 {
        self.0.buffered.load(Relaxed) as f32 / 1000.0
    }

    pub fn loading(&self) -> bool {
        self.0.loading.load(Relaxed) || (self.0.buffering.load(Relaxed) && !self.0.paused.load(Relaxed))
    }

    pub fn failed(&self) -> bool {
        self.0.failed.load(Relaxed)
    }
}

pub struct Player {
    commands: Sender<Cmd>,
    shared: Arc<Shared>,
    sink: Option<rodio::Sink>,
    chunks: Chunks,
    wake: Wake,
    diagnostics: Option<AudioDiagnostics>,
    volume: f32,
    yt: Arc<ytm::Client>,
    urls: Urls,
}

impl Player {
    pub fn new(yt: Arc<ytm::Client>, wake: impl Fn() + Send + Sync + 'static) -> Self {
        let wake: Wake = Arc::new(wake);
        let diagnostics = AudioDiagnostics::start();
        let shared = Arc::new(Shared::default());
        shared.paused.store(true, Relaxed);
        let urls = Urls::default();
        let (commands, inbox) = mpsc::channel();
        let (pcm, chunks) = mpsc::sync_channel(QUEUED_CHUNKS);
        let worker = DecodeWorker {
            shared: shared.clone(),
            yt: yt.clone(),
            urls: urls.clone(),
            wake: wake.clone(),
            diagnostics: diagnostics.clone(),
        };
        let replies = commands.clone();
        std::thread::spawn(move || decode_loop(inbox, replies, pcm, worker));
        Self {
            commands,
            shared,
            sink: None,
            chunks: Arc::new(Mutex::new(chunks)),
            wake,
            diagnostics,
            volume: 1.0,
            yt,
            urls,
        }
    }

    pub fn status(&self) -> Status {
        Status(self.shared.clone())
    }

    fn connect(&mut self, mixer: &rodio::mixer::Mixer) {
        let sink = rodio::Sink::connect_new(mixer);
        sink.pause();
        self.shared.paused.store(true, Relaxed);
        sink.set_volume(self.volume * self.volume);
        sink.append(PcmSource::new(
            self.chunks.clone(),
            self.shared.clone(),
            self.wake.clone(),
            self.diagnostics.clone(),
        ));
        self.sink = Some(sink);
    }

    fn disconnect(&mut self) {
        self.sink = None;
        self.shared.paused.store(true, Relaxed);
    }

    fn fail(&self, message: &str) {
        if let Some(diagnostics) = &self.diagnostics {
            diagnostics.record(Diagnostic::DeviceError(message.to_owned()));
        }
        *self.shared.error.lock().unwrap() = Some(message.to_owned().into());
        (self.wake)();
    }

    pub fn load(&self, video_id: &str) {
        let generation = self.shared.generation.fetch_add(1, Relaxed) + 1;
        let epoch = self.shared.epoch.fetch_add(1, Relaxed) + 1;
        self.shared.position_ms.store(0, Relaxed);
        self.shared.duration_ms.store(0, Relaxed);
        self.shared.buffered.store(0, Relaxed);
        self.shared.ended.store(false, Relaxed);
        self.shared.loading.store(true, Relaxed);
        self.shared.failed.store(false, Relaxed);
        self.shared.buffering.store(false, Relaxed);
        self.shared.error.lock().unwrap().take();
        if let Some(diagnostics) = &self.diagnostics {
            diagnostics.record(Diagnostic::TrackLoad {
                video_id: video_id.to_owned(),
            });
        }
        let _ = self.commands.send(Cmd::Load {
            video_id: video_id.to_owned(),
            generation,
            epoch,
            resume_ms: None,
            retry: 0,
        });
        self.set_paused(false);
    }

    pub fn preload(&self, video_id: &str) {
        let (yt, urls, video_id, shared) = (self.yt.clone(), self.urls.clone(), video_id.to_owned(), self.shared.clone());
        let generation = shared.generation.load(Relaxed);
        std::thread::spawn(move || resolve_if(&yt, &urls, &video_id, &|| shared.generation.load(Relaxed) == generation));
    }

    pub fn seek(&self, ms: u64) {
        let epoch = self.shared.epoch.fetch_add(1, Relaxed) + 1;
        self.shared.position_ms.store(ms, Relaxed);
        self.shared.ended.store(false, Relaxed);
        let _ = self.commands.send(Cmd::Seek { ms, epoch });
    }

    pub fn set_paused(&self, paused: bool) {
        let paused = paused || self.sink.is_none();
        let was_paused = self.shared.paused.swap(paused, Relaxed);
        if paused && !was_paused {
            self.shared.pause_epoch.fetch_add(1, Relaxed);
        }
        if let Some(sink) = &self.sink {
            if paused { sink.pause() } else { sink.play() }
        }
    }

    pub fn paused(&self) -> bool {
        self.shared.paused.load(Relaxed)
    }

    pub fn set_volume(&mut self, volume: f32) {
        self.volume = volume;
        if let Some(sink) = &self.sink {
            sink.set_volume(volume * volume);
        }
    }

    pub fn failed(&self) -> bool {
        self.shared.failed.load(Relaxed)
    }

    pub fn loading(&self) -> bool {
        self.shared.loading.load(Relaxed)
    }

    pub fn volume(&self) -> f32 {
        self.volume
    }

    pub fn position_ms(&self) -> u64 {
        self.shared.position_ms.load(Relaxed)
    }

    #[cfg(test)]
    pub fn end_at(&self, position_ms: u64) {
        self.shared.position_ms.store(position_ms, Relaxed);
        self.shared.ended.store(true, Relaxed);
        (self.wake)();
    }

    pub fn take_ended(&self) -> bool {
        self.shared.ended.swap(false, Relaxed)
    }

    pub fn take_error(&self) -> Option<ytm::StreamError> {
        self.shared.error.lock().unwrap().take()
    }
}

pub fn selftest(query: &str) -> Result<String, String> {
    let started = Instant::now();
    let yt = ytm::Client::new(String::new());
    let song = yt.search(query)?.songs().into_iter().next().ok_or("search returned no songs")?;
    let searched = started.elapsed();
    let radio = yt.radio(&song.video_id)?;
    let shared = Arc::new(Shared::default());
    let opening = Instant::now();
    let mut track = Track::open(&yt, &Urls::default(), &song.video_id, 0, &shared)?;
    let opened = opening.elapsed();
    let mut frames = 0u64;
    let mut peak = 0f32;
    let mut rate = 0;
    while frames < 3 * 44_100 {
        let chunk = track.next_chunk(0)?.ok_or("stream ended early")?;
        frames += (chunk.samples.len() / chunk.channels.max(1) as usize) as u64;
        peak = chunk.samples.iter().fold(peak, |peak, sample| peak.max(sample.abs()));
        rate = chunk.rate;
    }
    if peak <= 0.001 {
        return Err("decoded audio is silent".to_owned());
    }
    let duration = shared.duration_ms.load(Relaxed);
    let far = if duration > SELFTEST_FAR_SEEK_MS + 60_000 {
        SELFTEST_FAR_SEEK_MS
    } else {
        60_000
    };
    let seeking = Instant::now();
    for target in [60_000, 5_000, far, 5_000] {
        track.seek(target);
        let landed = track.next_chunk(0)?.ok_or("seek ran past the end")?.start_ms;
        if landed.abs_diff(target) > 1000 {
            return Err(format!("seek to {target} ms landed at {landed} ms"));
        }
    }
    Ok(format!(
        "ok: \"{}\" by {} | search {searched:.0?} | open {opened:.0?} | {rate} Hz, peak {peak:.2} | seeks to 60s, 5s, {}s, 5s in {:.0?} | radio {} tracks | duration {duration} ms",
        song.title,
        song.subtitle,
        far / 1000,
        seeking.elapsed(),
        radio.len(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "requires live YouTube Music browsing and streaming"]
    fn podcast_catalog_show_and_audio_work_end_to_end() {
        let yt = ytm::Client::new(String::new());
        let catalog = yt.browse(ytm::PODCASTS, &|_| {}).expect("podcast catalog should load");
        assert!(!catalog.songs().is_empty(), "podcast catalog should contain playable episodes");
        let show = catalog
            .sections
            .iter()
            .flat_map(|section| &section.items)
            .find(|item| item.browse_id.starts_with("MPSP"))
            .expect("podcast catalog should contain shows");
        let page = yt.browse(&show.browse_id, &|_| {}).expect("podcast show should load");
        let episodes = page.songs();
        assert!(!episodes.is_empty(), "podcast show should contain playable episodes");
        assert!(episodes.iter().all(|item| !item.duration.is_empty()));
        let shared = Arc::new(Shared::default());
        let mut track = Track::open(&yt, &Urls::default(), &episodes[0].video_id, 0, &shared).expect("episode audio should open");
        let mut frames = 0;
        let mut peak = 0f32;
        while frames < 3 * 44_100 {
            let chunk = track
                .next_chunk(0)
                .expect("episode audio should decode")
                .expect("episode should not end early");
            frames += chunk.samples.len() / chunk.channels.max(1) as usize;
            peak = chunk.samples.iter().fold(peak, |peak, sample| peak.max(sample.abs()));
        }
        assert!(peak > 0.001, "decoded episode audio should not be silent");
        let duration = shared.duration_ms.load(Relaxed);
        assert!(duration > 60_000, "episode duration should be reported");
        track.seek(60_000);
        let landed = track
            .next_chunk(0)
            .expect("seek should decode")
            .expect("seek should not end the episode")
            .start_ms;
        assert!(landed.abs_diff(60_000) <= 1000, "episode seek should land within one second");
    }

    #[test]
    fn stale_preload_does_not_resolve_or_cache_a_stream() {
        let yt = ytm::Client::new(String::new());
        let urls = Urls::default();
        assert!(resolve_if(&yt, &urls, "stale", &|| false).unwrap().is_none());
        assert!(urls.lock().unwrap().is_empty());
    }

    #[test]
    fn retries_back_off_and_stop() {
        assert_eq!(retry_delay(0), None);
        assert_eq!(retry_delay(1), Some(Duration::from_secs(1)));
        assert_eq!(retry_delay(LOAD_RETRIES), Some(Duration::from_secs(4)));
        assert_eq!(retry_delay(LOAD_RETRIES + 1), None);
    }

    #[test]
    fn a_reopened_output_keeps_playing_the_queued_audio() {
        let shared = Arc::new(Shared::default());
        let (pcm, chunks) = mpsc::sync_channel(QUEUED_CHUNKS);
        let chunks: Chunks = Arc::new(Mutex::new(chunks));
        let open = || PcmSource::new(chunks.clone(), shared.clone(), Arc::new(|| {}), None);
        let queue = |start_ms| {
            let chunk = Chunk {
                samples: vec![0.5; 4],
                start_ms,
                epoch: 0,
                rate: 48_000,
                channels: 2,
                end: false,
            };
            pcm.try_send(chunk).is_ok()
        };
        assert!(queue(1000) && queue(2000));
        let mut first = open();
        assert_eq!(first.by_ref().take(SILENCE_SAMPLES + 4).filter(|sample| *sample == 0.5).count(), 4);
        drop(first);
        let mut second = open();
        assert_eq!(second.by_ref().take(SILENCE_SAMPLES + 4).filter(|sample| *sample == 0.5).count(), 4);
        assert_eq!(shared.position_ms.load(Relaxed), 2000);
        assert!(queue(3000));
    }

    #[test]
    fn pcm_starvation_and_recovery_are_diagnostic_events() {
        let shared = Arc::new(Shared::default());
        let (pcm, chunks) = mpsc::sync_channel(QUEUED_CHUNKS);
        let chunks = Arc::new(Mutex::new(chunks));
        let (diagnostics, events) = AudioDiagnostics::channel();
        let mut source = PcmSource::new(chunks, shared.clone(), Arc::new(|| {}), Some(diagnostics));
        assert!(source.by_ref().take(SILENCE_SAMPLES + 1).all(|sample| sample == 0.0));
        assert!(matches!(events.recv().unwrap().1, Diagnostic::QueueStarved { queue: 0 }));
        pcm.send(Chunk {
            samples: vec![0.5; 4],
            start_ms: 1000,
            epoch: 0,
            rate: 48_000,
            channels: 2,
            end: false,
        })
        .unwrap();
        assert!(source.by_ref().take(SILENCE_SAMPLES).any(|sample| sample == 0.5));
        assert!(matches!(events.recv().unwrap().1, Diagnostic::QueueRecovered { queue: 0, .. }));
        assert!(events.try_recv().is_err());
    }
}
