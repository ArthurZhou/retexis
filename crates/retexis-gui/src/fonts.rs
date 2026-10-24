//! Which font family to ask for.
//!
//! Slint's `default-font-family` takes one family, not a CSS list. It asks the
//! platform font database for that family and, when a glyph is missing, falls
//! back per glyph to whatever else the database has. So the job here is
//! narrower than a browser's `font-family`: name the family this platform is
//! most likely to ship that covers CJK, keep a second and a third choice for
//! the systems that do not ship the first, and accept that a wrong answer
//! degrades to the platform default rather than to tofu — the per-glyph
//! fallback still happens.
//!
//! The check is a file-existence probe, not a font query: Slint does not expose
//! its font database, and a probe is enough to choose between "the family this
//! OS always has" and "the family it probably has". Candidates without a probe
//! are taken on faith, which is the right default for the Linux names: there is
//! no one path to look at, and the family names are standard.
//!
//! This replaces a hard-coded `"Microsoft YaHei"`, which is correct on exactly
//! one of the three platforms and silently wrong on the other two.

/// A family to try, and where to look to see whether it is installed.
struct Candidate {
    family: &'static str,
    /// `None`: no sensible single path, take the name on faith.
    probe: Option<&'static str>,
}

const fn known(family: &'static str, probe: &'static str) -> Candidate {
    Candidate {
        family,
        probe: Some(probe),
    }
}

const fn guessed(family: &'static str) -> Candidate {
    Candidate {
        family,
        probe: None,
    }
}

/// UI text. The first entry is the one that carries CJK on that platform.
#[cfg(target_os = "windows")]
const SANS: &[Candidate] = &[
    known("Microsoft YaHei", r"C:\Windows\Fonts\msyh.ttc"),
    known("Segoe UI", r"C:\Windows\Fonts\segoeui.ttf"),
    guessed("Arial"),
];

#[cfg(target_os = "macos")]
const SANS: &[Candidate] = &[
    known("PingFang SC", "/System/Library/Fonts/PingFang.ttc"),
    known(
        "Hiragino Sans GB",
        "/System/Library/Fonts/Hiragino Sans GB.ttc",
    ),
    guessed("Helvetica Neue"),
];

#[cfg(not(any(target_os = "windows", target_os = "macos")))]
const SANS: &[Candidate] = &[
    known(
        "Noto Sans CJK SC",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    ),
    known(
        "Source Han Sans SC",
        "/usr/share/fonts/opentype/adobe-source-han-sans/SourceHanSansSC-Regular.otf",
    ),
    guessed("WenQuanYi Micro Hei"),
    guessed("DejaVu Sans"),
];

/// The monospace family used for ids, ages and counters. No CJK needed here:
/// every string that uses it is ASCII by construction.
#[cfg(target_os = "windows")]
const MONO: &[Candidate] = &[
    known("Consolas", r"C:\Windows\Fonts\consola.ttf"),
    known("Cascadia Mono", r"C:\Windows\Fonts\CascadiaMono.ttf"),
    guessed("Courier New"),
];

#[cfg(target_os = "macos")]
const MONO: &[Candidate] = &[
    known("SF Mono", "/System/Library/Fonts/SFNSMono.ttf"),
    known("Menlo", "/System/Library/Fonts/Menlo.ttc"),
    guessed("Monaco"),
];

#[cfg(not(any(target_os = "windows", target_os = "macos")))]
const MONO: &[Candidate] = &[
    known(
        "JetBrains Mono",
        "/usr/share/fonts/truetype/jetbrains-mono/JetBrainsMono-Regular.ttf",
    ),
    known(
        "DejaVu Sans Mono",
        "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
    ),
    guessed("Liberation Mono"),
];

/// The family for UI text.
pub fn sans() -> &'static str {
    pick(SANS)
}

/// The family for ids, ages and counters.
pub fn mono() -> &'static str {
    pick(MONO)
}

/// First candidate that is installed, else the last one as a guess.
fn pick(list: &[Candidate]) -> &'static str {
    for c in list {
        match c.probe {
            Some(path) if std::path::Path::new(path).exists() => return c.family,
            Some(_) => continue,
            None => return c.family,
        }
    }
    list.last().map(|c| c.family).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn there_is_always_an_answer() {
        assert!(!sans().is_empty());
        assert!(!mono().is_empty());
    }

    #[test]
    fn the_first_choice_is_the_platform_one() {
        // A regression here means the wrong family is hard-coded again.
        let expected = if cfg!(target_os = "windows") {
            "Microsoft YaHei"
        } else if cfg!(target_os = "macos") {
            "PingFang SC"
        } else {
            "Noto Sans CJK SC"
        };
        assert_eq!(SANS[0].family, expected);
    }

    #[test]
    fn a_present_probe_wins_and_a_missing_one_is_skipped() {
        // Any path that certainly exists on every platform, and one that cannot.
        // Leaked so it satisfies the `'static` the const tables need.
        let here: &'static str = Box::leak(
            std::env::current_exe()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string()
                .into_boxed_str(),
        );
        let list = [
            Candidate {
                family: "missing",
                probe: Some("/nonexistent/retexis/font.ttf"),
            },
            Candidate {
                family: "present",
                probe: Some(here),
            },
            Candidate {
                family: "fallback",
                probe: None,
            },
        ];
        assert_eq!(pick(&list), "present");
    }

    #[test]
    fn with_nothing_installed_the_last_guess_is_used() {
        let list = [
            Candidate {
                family: "first",
                probe: Some("/nonexistent/retexis/font.ttf"),
            },
            Candidate {
                family: "last",
                probe: None,
            },
        ];
        assert_eq!(pick(&list), "last");
    }
}
