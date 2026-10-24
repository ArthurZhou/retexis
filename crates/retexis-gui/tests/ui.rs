//! The window's behaviour, driven the way a person drives it.
//!
//! These are not snapshot tests and there is no GPU: Slint's headless backend
//! lays the window out with fixed font metrics, and the helpers below push the
//! same `WindowEvent`s the winit backend would. So "Esc closes the dialog" is
//! answered by pressing Esc at a known coordinate, not by reading the `.slint`
//! file and hoping the focus chain agrees.
//!
//! Coordinates are top-left anchored and derived from the layout constants in
//! `ui/chat.slint`: header 46px, toolbar 50px, sidebar 196px, padding 14px.

use i_slint_backend_testing::init_no_event_loop;
use retexis_gui::{ChatWindow, FoundRow, PeerRow};
use slint::platform::{Key, PointerEventButton, WindowEvent};
use slint::{ComponentHandle, LogicalPosition, ModelRc, SharedString, VecModel};
use std::cell::Cell;
use std::rc::Rc;

const NONE: i32 = 0;
const DISCOVER: i32 = 1;
const CONNECT: i32 = 2;

/// Every test starts its own headless platform; they do not share a window.
///
/// `close-dialogs` is wired here the way `bridge.rs` wires it. The window only
/// reports the intent (a scrim click, a ×, Esc); what "closed" means is the
/// application's decision, and this is that decision, minus the scan thread.
fn window() -> ChatWindow {
    init_no_event_loop();
    let ui = ChatWindow::new().expect("the window must build");
    let weak = ui.as_weak();
    ui.on_close_dialogs(move || {
        if let Some(ui) = weak.upgrade() {
            ui.set_dialog(NONE);
        }
    });
    ui
}

fn click(ui: &ChatWindow, x: f32, y: f32) {
    let position = LogicalPosition::new(x, y);
    // Hover first: `TouchArea.has-hover` drives the visuals and a real backend
    // always sends a move before the press.
    ui.window()
        .dispatch_event(WindowEvent::PointerMoved { position });
    ui.window().dispatch_event(WindowEvent::PointerPressed {
        position,
        button: PointerEventButton::Left,
    });
    ui.window().dispatch_event(WindowEvent::PointerReleased {
        position,
        button: PointerEventButton::Left,
    });
}

/// Toolbar buttons: 发现 at x 14..86, 连接 at x 94..166, both y 46..96.
fn click_discover_button(ui: &ChatWindow) {
    click(ui, 50.0, 71.0);
}

fn click_connect_button(ui: &ChatWindow) {
    click(ui, 130.0, 71.0);
}

/// Somewhere the scrim covers and the panel does not: the panel is centred and
/// at most 480px wide, so the top-left corner of the window is always scrim.
fn click_outside_the_panel(ui: &ChatWindow) {
    click(ui, 5.0, 5.0);
}

/// The middle of the window: inside the panel for both dialogs.
fn click_inside_the_panel(ui: &ChatWindow) {
    let size = ui.window().size();
    click(ui, size.width as f32 / 2.0, size.height as f32 / 2.0);
}

fn press_escape(ui: &ChatWindow) {
    let text: SharedString = Key::Escape.into();
    ui.window()
        .dispatch_event(WindowEvent::KeyPressed { text: text.clone() });
    ui.window()
        .dispatch_event(WindowEvent::KeyReleased { text });
}

fn press_enter(ui: &ChatWindow) {
    let text: SharedString = Key::Return.into();
    ui.window()
        .dispatch_event(WindowEvent::KeyPressed { text: text.clone() });
    ui.window()
        .dispatch_event(WindowEvent::KeyReleased { text });
}

fn type_text(ui: &ChatWindow, text: &str) {
    for ch in text.chars() {
        let text = SharedString::from(ch);
        ui.window()
            .dispatch_event(WindowEvent::KeyPressed { text: text.clone() });
        ui.window()
            .dispatch_event(WindowEvent::KeyReleased { text });
    }
}

// ---------------------------------------------------------------- open/close

#[test]
fn a_dialog_starts_closed_and_opens_on_request() {
    let ui = window();
    assert_eq!(ui.get_dialog(), NONE, "nothing is open at startup");

    ui.set_dialog(DISCOVER);
    assert_eq!(ui.get_dialog(), DISCOVER);

    ui.set_dialog(CONNECT);
    assert_eq!(
        ui.get_dialog(),
        CONNECT,
        "one dialog at a time, by construction"
    );
}

#[test]
fn a_click_on_the_scrim_closes_the_dialog() {
    let ui = window();
    ui.set_dialog(DISCOVER);

    click_outside_the_panel(&ui);
    assert_eq!(
        ui.get_dialog(),
        NONE,
        "clicking the blank area must dismiss"
    );
}

#[test]
fn a_click_inside_the_panel_does_not_close_it() {
    let ui = window();
    ui.set_dialog(CONNECT);

    click_inside_the_panel(&ui);
    assert_eq!(
        ui.get_dialog(),
        CONNECT,
        "the panel must swallow clicks, or the dialog is unusable"
    );
}

#[test]
fn escape_closes_the_dialog() {
    let ui = window();
    ui.set_dialog(DISCOVER);

    press_escape(&ui);
    assert_eq!(ui.get_dialog(), NONE);
}

#[test]
fn escape_closes_the_dialog_while_the_composer_has_focus() {
    // The composer takes focus at startup, so this is the normal case: the key
    // handler has to be an ancestor of the focused item, not its sibling.
    let ui = window();
    type_text(&ui, "draft");
    assert_eq!(
        ui.get_draft(),
        "draft",
        "the composer must already have focus"
    );

    ui.set_dialog(CONNECT);
    press_escape(&ui);
    assert_eq!(ui.get_dialog(), NONE, "Esc must bubble up from the input");
}

#[test]
fn escape_without_a_dialog_is_harmless() {
    let ui = window();
    press_escape(&ui);
    assert_eq!(ui.get_dialog(), NONE);
}

#[test]
fn the_connect_button_opens_the_connect_dialog() {
    let ui = window();
    let weak = ui.as_weak();
    ui.on_open_connect(move || {
        if let Some(ui) = weak.upgrade() {
            ui.set_dialog(CONNECT);
        }
    });

    click_connect_button(&ui);
    assert_eq!(ui.get_dialog(), CONNECT);
    assert_eq!(
        ui.get_connect_url(),
        "",
        "a fresh dialog starts with an empty address"
    );
}

#[test]
fn the_discover_button_asks_for_a_scan_and_shows_its_progress() {
    let ui = window();
    let asked = Rc::new(Cell::new(false));
    {
        let asked = Rc::clone(&asked);
        let weak = ui.as_weak();
        ui.on_discover(move || {
            asked.set(true);
            if let Some(ui) = weak.upgrade() {
                ui.set_dialog(DISCOVER);
                ui.set_scanning(true);
            }
        });
    }

    click_discover_button(&ui);
    assert!(asked.get(), "the button must reach the application");
    assert_eq!(ui.get_dialog(), DISCOVER);
    assert!(ui.get_scanning());
}

// ---------------------------------------------------------------- input

#[test]
fn the_composer_has_focus_at_startup() {
    let ui = window();
    type_text(&ui, "hello");
    assert_eq!(
        ui.get_draft(),
        "hello",
        "typing must land in the composer without clicking it first"
    );
}

#[test]
fn typing_goes_to_the_address_field_when_connect_is_open() {
    let ui = window();
    ui.set_dialog(CONNECT);

    type_text(&ui, "tcp://10.0.0.7:7100");
    assert_eq!(
        ui.get_connect_url(),
        "tcp://10.0.0.7:7100",
        "the dialog must take the keyboard with it"
    );
    assert_eq!(
        ui.get_draft(),
        "",
        "the composer behind the scrim must not receive the text"
    );
}

#[test]
fn closing_connect_gives_the_keyboard_back_to_the_composer() {
    let ui = window();
    ui.set_dialog(CONNECT);
    ui.set_dialog(NONE);

    type_text(&ui, "hi");
    assert_eq!(ui.get_draft(), "hi");
}

#[test]
fn a_discover_dialog_takes_the_keyboard_away_from_the_composer() {
    let ui = window();
    ui.set_dialog(DISCOVER);
    type_text(&ui, "stray");

    assert_eq!(
        ui.get_draft(),
        "",
        "there is nothing to type into, so nothing may be typed"
    );
    // ...and Esc still arrives, because focus is parked on the window itself.
    press_escape(&ui);
    assert_eq!(ui.get_dialog(), NONE);
}

// ---------------------------------------------------------------- sidebar

#[test]
fn the_first_peer_row_selects_the_first_peer_not_everyone() {
    let ui = window();
    ui.set_peers(ModelRc::from(Rc::new(VecModel::from(vec![PeerRow {
        name: "bob".into(),
        meta: "direct".into(),
        hops: "1".into(),
        selected: false,
    }]))));

    let picked = Rc::new(Cell::new(-1));
    {
        let picked = Rc::clone(&picked);
        ui.on_select(move |i| picked.set(i));
    }

    // TO ALL is the row right under the PEERS label: 14 (padding) + 16 (label)
    // + 34/2.
    click(&ui, 100.0, 46.0 + 50.0 + 14.0 + 16.0 + 17.0);
    assert_eq!(picked.get(), 0, "the TO ALL row is index 0");

    // The first real peer is the next 42px row.
    click(&ui, 100.0, 46.0 + 50.0 + 14.0 + 16.0 + 34.0 + 1.0 + 21.0);
    assert_eq!(
        picked.get(),
        1,
        "clicking the first peer must not address everyone"
    );
}

// ---------------------------------------------------------------- results

#[test]
fn many_results_stay_inside_the_panel() {
    let ui = window();
    ui.set_dialog(DISCOVER);
    ui.set_scanning(false);

    // Twelve nodes with two URLs each: the case that used to push the panel
    // past the bottom of the window.
    let found: Vec<FoundRow> = (0..12)
        .map(|i| FoundRow {
            name: format!("node-{i}").into(),
            id: format!("{:08x}", i).into(),
            urls: ModelRc::from(Rc::new(VecModel::from(vec![
                SharedString::from(format!("tcp://10.0.0.{i}:7100")),
                SharedString::from(format!("udp://10.0.0.{i}:7101")),
            ]))),
        })
        .collect();
    ui.set_found(ModelRc::from(Rc::new(VecModel::from(found))));

    // The dialog is still dismissible, which is the property that matters: a
    // panel taller than the window would put its own scrim out of reach.
    click_outside_the_panel(&ui);
    assert_eq!(ui.get_dialog(), NONE);
}

// ---------------------------------------------------------------- drills

/// The three ways to misuse this window that a person actually finds in the
/// first minute. None of them may crash, and none may leave the window in a
/// state the next keystroke cannot fix.

#[test]
fn enter_on_an_empty_composer_sends_nothing() {
    let ui = window();
    let sent = Rc::new(Cell::new(0));
    {
        let sent = Rc::clone(&sent);
        ui.on_send(move |_| sent.set(sent.get() + 1));
    }

    press_enter(&ui);
    assert_eq!(sent.get(), 0, "an empty box is not a message");

    type_text(&ui, "hi");
    press_enter(&ui);
    assert_eq!(sent.get(), 1, "a typed message is");
}

#[test]
fn the_send_button_on_an_empty_composer_sends_nothing() {
    let ui = window();
    let sent = Rc::new(Cell::new(0));
    {
        let sent = Rc::clone(&sent);
        ui.on_send(move |_| sent.set(sent.get() + 1));
    }

    // The 发送 button sits at the right end of the composer, 72px wide.
    let size = ui.window().size();
    click(
        &ui,
        size.width as f32 - 14.0 - 36.0,
        size.height as f32 - 14.0 - 16.0,
    );
    assert_eq!(sent.get(), 0);
}

#[test]
fn double_clicking_connect_opens_then_closes_and_leaves_the_window_usable() {
    let ui = window();
    let opened = Rc::new(Cell::new(0));
    {
        let opened = Rc::clone(&opened);
        let weak = ui.as_weak();
        ui.on_open_connect(move || {
            opened.set(opened.get() + 1);
            if let Some(ui) = weak.upgrade() {
                ui.set_dialog(CONNECT);
            }
        });
    }

    // A double-click is two clicks: the first opens the dialog, and by the
    // second the scrim is over the toolbar, so it lands on the scrim and
    // dismisses. That is what a modal means — the button is not clickable
    // while a dialog covers it.
    click_connect_button(&ui);
    assert_eq!(ui.get_dialog(), CONNECT);
    click_connect_button(&ui);
    assert_eq!(ui.get_dialog(), NONE, "the second click hits the scrim");

    assert_eq!(opened.get(), 1, "the button itself fired once, not twice");

    // ...and the window is not stuck: the composer took the keyboard back.
    type_text(&ui, "after the double click");
    assert_eq!(ui.get_draft(), "after the double click");
}

#[test]
fn escape_and_a_scrim_click_do_not_stack_up_on_each_other() {
    let ui = window();
    ui.set_dialog(DISCOVER);

    press_escape(&ui);
    assert_eq!(ui.get_dialog(), NONE);

    // A second Esc with nothing open is not an error, and a click on the blank
    // window afterwards is a click on the window, not on a ghost dialog.
    press_escape(&ui);
    click_outside_the_panel(&ui);
    assert_eq!(ui.get_dialog(), NONE);

    type_text(&ui, "still works");
    assert_eq!(ui.get_draft(), "still works");
}
