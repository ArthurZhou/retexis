//! The bridge: the only code that knows both the protocol and the window.
//!
//! Slint is immediate-mode-free: the UI is a set of properties and models, and
//! this module keeps them in sync. A timer reads the node ten times a second
//! (the node is thread-safe, the UI is not), and every callback from the UI
//! turns into one call on [`App`].
//!
//! Why a timer instead of a channel: the log is already a mutex-guarded ring,
//! and polling it costs less than pushing every event through an event loop.
//! It also means an ack that lands out of order still updates the row it
//! belongs to, because the whole tail is re-read, not just the new part.

use crate::app::{App, PeerView};
use crate::boot::Boot;
use crate::fonts;
use crate::model;
use crate::{ChatWindow, EntryRow, FoundRow, PeerRow, Tokens};
use retexis_core::{Announcement, NodeId};
use slint::{ComponentHandle, Model, ModelRc, Timer, TimerMode, VecModel};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::mpsc::{channel, Receiver, TryRecvError};
use std::sync::Arc;
use std::time::Duration;

const POLL: Duration = Duration::from_millis(100);
/// Rows kept in the window. The log keeps more; this is what the eye needs.
const VISIBLE: usize = 160;
/// How far back each poll re-reads, so an ack can rewrite an old row.
const TAIL: u64 = 48;

/// The dialog the window is showing. One value, so "both" is unrepresentable.
pub const DIALOG_NONE: i32 = 0;
pub const DIALOG_DISCOVER: i32 = 1;
pub const DIALOG_CONNECT: i32 = 2;

struct State {
    app: Arc<App>,
    entries: Vec<crate::Entry>,
    since: u64,
    /// The addressee of the next message; `None` is a broadcast.
    target: Option<NodeId>,
    /// Parallel to the peer rows: index 0 is "everyone".
    targets: Vec<Option<NodeId>>,
    discovery: Option<Receiver<Vec<Announcement>>>,
    scanning: bool,
}

impl State {
    fn addressee(&self) -> String {
        match self.target {
            None => "TO ALL".to_string(),
            Some(id) => {
                let name = self
                    .app
                    .view()
                    .peers
                    .iter()
                    .find(|p| p.id == id)
                    .map(|p| p.name.clone())
                    .unwrap_or_else(|| id.short());
                format!("TO {name}")
            }
        }
    }
}

/// Shared handles for the closures below.
struct Wiring {
    state: Rc<RefCell<State>>,
    entries: Rc<VecModel<EntryRow>>,
    peers: Rc<VecModel<PeerRow>>,
    found: Rc<VecModel<FoundRow>>,
    /// The rows currently in `peers`, to avoid a needless reset every tick.
    peer_rows: Rc<RefCell<Vec<PeerRow>>>,
}

pub fn run(boot: Boot) -> Result<(), slint::PlatformError> {
    let window = ChatWindow::new()?;
    // The timer must outlive `wire`, and `run` only returns when the window
    // closes, so keeping it here is enough.
    let _timer = wire(&window, boot);
    // `run` shows the window and then spins the event loop; showing it first
    // gives the platform a chance to take focus, so the composer is focused
    // *after* the window exists rather than into a window that is not mapped.
    window.show()?;
    window.invoke_focus_composer();
    open_requested_dialog(&window);
    let _shot = shot_timer(&window);
    window.run()
}

/// `RETEXIS_DIALOG=discover|connect`: open a dialog the moment the window is up.
///
/// There is no other way to photograph a dialog: the screenshot is written by
/// the application itself (see [`shot_timer`]), so whatever is on screen when
/// the timer fires is the picture. Nothing in the product path reads this, and
/// an unknown value is ignored rather than fatal.
fn open_requested_dialog(window: &ChatWindow) {
    match std::env::var("RETEXIS_DIALOG").as_deref() {
        // A real scan: the same code path the 发现 button takes, so the
        // screenshot is of a dialog with actual results in it.
        Ok("discover") => window.invoke_discover(),
        Ok("connect") => window.invoke_open_connect(),
        _ => {}
    }
}

/// `RETEXIS_SHOT=<file.bmp>`: keep the file holding the frame on screen.
///
/// Rewritten a few times a second rather than once, because the interesting
/// moment (the last message arrived, the dialog opened) is not known in
/// advance; the last write before the process exits is the picture.
fn shot_timer(window: &ChatWindow) -> Option<Timer> {
    let path = std::env::var_os("RETEXIS_SHOT")?;
    let path = std::path::PathBuf::from(path);
    let weak = window.as_weak();
    let timer = Timer::default();
    timer.start(TimerMode::Repeated, Duration::from_millis(400), move || {
        if let Some(ui) = weak.upgrade() {
            if let Err(e) = crate::shot::save(&path, &ui) {
                eprintln!("! cannot write {}: {e}", path.display());
            }
        }
    });
    Some(timer)
}

fn wire(window: &ChatWindow, boot: Boot) -> Timer {
    let app = Arc::clone(&boot.app);
    window.set_me_name(app.name().into());
    window.set_me_id(app.id().short().into());
    window.set_authed(app.is_authed());
    window.set_addressee("TO ALL".into());

    // The `.slint` file carries a Windows default so it renders sensibly on its
    // own; this replaces it with the family this machine actually has.
    let tokens = window.global::<Tokens>();
    tokens.set_sans(fonts::sans().into());
    tokens.set_mono(fonts::mono().into());

    let entries = Rc::new(VecModel::from(Vec::<EntryRow>::new()));
    let peers = Rc::new(VecModel::from(Vec::<PeerRow>::new()));
    let found = Rc::new(VecModel::from(Vec::<FoundRow>::new()));
    window.set_entries(ModelRc::from(entries.clone()));
    window.set_peers(ModelRc::from(peers.clone()));
    window.set_found(ModelRc::from(found.clone()));

    let state = Rc::new(RefCell::new(State {
        app: Arc::clone(&app),
        entries: Vec::new(),
        since: app.last_seq(),
        target: None,
        targets: vec![None],
        discovery: None,
        scanning: false,
    }));

    let wiring = Wiring {
        state,
        entries,
        peers,
        found,
        peer_rows: Rc::new(RefCell::new(Vec::new())),
    };

    // Anything that happened before the window existed (a `--say` line, the
    // first link coming up) is already in the log.
    {
        let mut st = wiring.state.borrow_mut();
        for entry in app.entries() {
            let row = model::row(&entry, app.at_ms());
            st.entries.push(entry);
            wiring.entries.push(row);
        }
        st.since = st.entries.last().map(|e| e.seq).unwrap_or(st.since);
    }
    refresh(window, &wiring);

    wire_callbacks(window, &wiring);
    start_timer(window, &wiring)
}

fn wire_callbacks(window: &ChatWindow, w: &Wiring) {
    // ---- send
    {
        let weak = window.as_weak();
        let w = Wiring {
            state: Rc::clone(&w.state),
            entries: Rc::clone(&w.entries),
            peers: Rc::clone(&w.peers),
            found: Rc::clone(&w.found),
            peer_rows: Rc::clone(&w.peer_rows),
        };
        window.on_send(move |text| {
            let Some(ui) = weak.upgrade() else {
                return;
            };
            let text = text.to_string();
            if text.trim().is_empty() {
                return;
            }
            // The borrow has to end before `refresh`, which borrows the same
            // state again.
            let sent = {
                let st = w.state.borrow();
                st.app.send(st.target, &text)
            };
            match sent {
                Ok(()) => {
                    ui.set_draft("".into());
                    ui.set_error_text("".into());
                    refresh(&ui, &w);
                }
                Err(e) => ui.set_error_text(e.into()),
            }
        });
    }

    // ---- pick a recipient
    {
        let weak = window.as_weak();
        let state = Rc::clone(&w.state);
        let peers = Rc::clone(&w.peers);
        let peer_rows = Rc::clone(&w.peer_rows);
        window.on_select(move |index| {
            let Some(ui) = weak.upgrade() else {
                return;
            };
            let mut st = state.borrow_mut();
            let target = st.targets.get(index.max(0) as usize).copied().flatten();
            st.target = target;
            ui.set_addressee(st.addressee().into());

            let rows: Vec<PeerRow> = st
                .app
                .view()
                .peers
                .iter()
                .map(|p| model::peer(p, target))
                .collect();
            peers.set_vec(rows.clone());
            *peer_rows.borrow_mut() = rows;
        });
    }

    // ---- discover
    {
        let weak = window.as_weak();
        let state = Rc::clone(&w.state);
        let found = Rc::clone(&w.found);
        window.on_discover(move || {
            let Some(ui) = weak.upgrade() else {
                return;
            };
            let mut st = state.borrow_mut();
            let app = Arc::clone(&st.app);
            let (tx, rx) = channel();
            std::thread::spawn(move || {
                let _ = tx.send(app.discover(Duration::from_millis(900)));
            });
            st.discovery = Some(rx);
            st.scanning = true;
            found.set_vec(Vec::new());
            ui.set_scanning(true);
            ui.set_error_text("".into());
            // A new scan replaces whatever dialog was open, including a
            // previous scan whose result nobody is waiting for any more.
            ui.set_dialog(DIALOG_DISCOVER);
        });
    }

    // ---- open the address dialog (the 连接 button)
    {
        let weak = window.as_weak();
        let state = Rc::clone(&w.state);
        window.on_open_connect(move || {
            let Some(ui) = weak.upgrade() else {
                return;
            };
            let mut st = state.borrow_mut();
            // Opening CONNECT also means the scan is no longer interesting, so
            // drop the receiver: the thread finishes and its result is ignored
            // instead of landing in a dialog that is no longer showing.
            st.discovery = None;
            st.scanning = false;
            ui.set_scanning(false);
            ui.set_error_text("".into());
            ui.set_dialog(DIALOG_CONNECT);
        });
    }

    // ---- dial
    {
        let weak = window.as_weak();
        let state = Rc::clone(&w.state);
        window.on_connect(move |url| {
            let Some(ui) = weak.upgrade() else {
                return;
            };
            let st = state.borrow();
            let url = url.to_string();
            if url.trim().is_empty() {
                return;
            }
            match st.app.connect(url.trim()) {
                Ok(_) => {
                    ui.set_error_text("".into());
                    ui.set_dialog(DIALOG_NONE);
                    ui.set_connect_url("".into());
                    // Back to the composer: the next thing the user does after
                    // dialling is say something.
                    ui.invoke_focus_composer();
                }
                Err(e) => ui.set_error_text(e.into()),
            }
        });
    }

    // ---- close dialogs (the scrim, the ×, Esc, switching to CONNECT)
    {
        let weak = window.as_weak();
        let state = Rc::clone(&w.state);
        window.on_close_dialogs(move || {
            let Some(ui) = weak.upgrade() else {
                return;
            };
            let mut st = state.borrow_mut();
            st.discovery = None;
            st.scanning = false;
            ui.set_dialog(DIALOG_NONE);
            ui.set_scanning(false);
            ui.set_error_text("".into());
        });
    }
}

fn start_timer(window: &ChatWindow, w: &Wiring) -> Timer {
    let timer = Timer::default();
    let weak = window.as_weak();
    let w = Wiring {
        state: Rc::clone(&w.state),
        entries: Rc::clone(&w.entries),
        peers: Rc::clone(&w.peers),
        found: Rc::clone(&w.found),
        peer_rows: Rc::clone(&w.peer_rows),
    };
    timer.start(TimerMode::Repeated, POLL, move || {
        let Some(ui) = weak.upgrade() else {
            return;
        };
        {
            let mut st = w.state.borrow_mut();
            poll_discovery(&ui, &mut st, &w.found);
        }
        refresh(&ui, &w);
    });
    timer
}

fn poll_discovery(ui: &ChatWindow, st: &mut State, found: &Rc<VecModel<FoundRow>>) {
    let Some(rx) = &st.discovery else {
        return;
    };
    match rx.try_recv() {
        Ok(nodes) => {
            let rows: Vec<FoundRow> = nodes
                .iter()
                .map(|n| FoundRow {
                    name: if n.name.is_empty() {
                        "(anon)".into()
                    } else {
                        n.name.as_str().into()
                    },
                    id: n.node.map(|id| id.short()).unwrap_or_default().into(),
                    urls: ModelRc::from(Rc::new(VecModel::from(
                        n.endpoints
                            .iter()
                            .map(|e| slint::SharedString::from(e.url()))
                            .collect::<Vec<_>>(),
                    ))),
                })
                .collect();
            found.set_vec(rows);
            st.scanning = false;
            st.discovery = None;
            ui.set_scanning(false);
        }
        Err(TryRecvError::Empty) => {}
        Err(TryRecvError::Disconnected) => {
            st.scanning = false;
            st.discovery = None;
            ui.set_scanning(false);
        }
    }
}

/// Brings the window up to date with the node. Cheap: one lock, one pass over
/// the tail, and only the rows that changed are written.
fn refresh(ui: &ChatWindow, w: &Wiring) {
    let mut st = w.state.borrow_mut();
    let view = st.app.view();
    let now = st.app.at_ms();

    // ---- timeline
    let from = st.since.saturating_sub(TAIL);
    let mut grew = false;
    for entry in st.app.entries_since(from) {
        let row = model::row(&entry, now);
        match st.entries.binary_search_by_key(&entry.seq, |e| e.seq) {
            Ok(i) => {
                st.entries[i] = entry;
                w.entries.set_row_data(i, row);
            }
            Err(i) => {
                st.entries.insert(i, entry);
                w.entries.insert(i, row);
                grew = true;
            }
        }
    }
    st.since = st.entries.last().map(|e| e.seq).unwrap_or(st.since);

    // The window shows a tail; the log keeps everything.
    while st.entries.len() > VISIBLE {
        st.entries.remove(0);
        w.entries.remove(0);
    }

    // ---- peers
    let rows: Vec<PeerRow> = view
        .peers
        .iter()
        .map(|p: &PeerView| model::peer(p, st.target))
        .collect();
    if *w.peer_rows.borrow() != rows {
        st.targets = std::iter::once(None)
            .chain(view.peers.iter().map(|p| Some(p.id)))
            .collect();
        w.peers.set_vec(rows.clone());
        *w.peer_rows.borrow_mut() = rows;
    }

    // ---- header
    ui.set_counts(format!("PEERS {} · LINKS {}", view.peers.len(), view.links.len()).into());
    ui.set_addressee(st.addressee().into());

    // ---- follow the bottom
    if grew {
        ui.set_scroll_request(ui.get_scroll_request() + 1);
    }
}
