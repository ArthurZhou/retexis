"""Produce the screenshot in `docs/gui.png`.

Two nodes: `alice` is the window under test, `bob` is a headless CLI node that
talks to it. The picture is not a screen grab: the window is drawn by the
software renderer, and `RETEXIS_SHOT` makes the application write its own
framebuffer to a file a few times a second. Asking Windows for the pixels
(`PrintWindow`) returns whatever DWM last composited — on a remote session that
is a stale, half-painted surface — so the application is the only reliable
witness of what it drew.

    python scripts/gui_demo.py

Requires a built `target/debug/retexis-gui` and `target/debug/retexis`.
"""

import os
import struct
import subprocess
import sys
import time
import zlib

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TMP = os.environ.get("TEMP", "/tmp")

LONG = "".join(
    [
        "流式测试：这条不是一帧，而是 StreamOpen / StreamChunk / StreamClose 三帧，接收端按 seq 拼回来。",
        "中继节点看到的只是一串普通的 Body::Data，它不认识什么是流，也不需要认识——照常转发、去重、限流，转发路径上不保存任何流状态。",
        "接收端由 StreamAssembler 缓冲，乱序的先存着，重复的直接丢掉，Close 到了才把整条交给界面，所以这里是一个气泡。",
        "阈值是 1 KiB：超过它的普通消息会自动改走流式通道，这里用 /stream 把同一条路强制走了一遍。",
    ]
)


# ---------------------------------------------------------------- bmp -> png

def read_bmp(path):
    with open(path, "rb") as f:
        data = f.read()
    if data[:2] != b"BM":
        raise SystemExit(f"{path} is not a BMP")
    offset = struct.unpack("<I", data[10:14])[0]
    width, height = struct.unpack("<ii", data[18:26])
    bits = struct.unpack("<H", data[28:30])[0]
    if bits != 32 or height <= 0:
        raise SystemExit(f"unexpected BMP layout: {bits}bpp, height {height}")
    pixels = data[offset:]
    rows = []
    for y in range(height):
        row = pixels[y * width * 4:(y + 1) * width * 4]
        rgb = bytearray()
        for x in range(width):
            b, g, r = row[x * 4], row[x * 4 + 1], row[x * 4 + 2]
            rgb += bytes((r, g, b))
        rows.append(bytes(rgb))
    rows.reverse()  # BMP rows run bottom-up
    return width, height, rows


def save_png(path, width, height, rows):
    def chunk(tag, payload):
        return (
            struct.pack(">I", len(payload))
            + tag
            + payload
            + struct.pack(">I", zlib.crc32(tag + payload) & 0xFFFFFFFF)
        )

    data = b"\x89PNG\r\n\x1a\n"
    data += chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
    data += chunk(b"IDAT", zlib.compress(b"".join(b"\x00" + r for r in rows), 9))
    data += chunk(b"IEND", b"")
    with open(path, "wb") as f:
        f.write(data)


# ---------------------------------------------------------------- demo

def built(name):
    """`cargo build` writes `retexis-gui` on Unix and `retexis-gui.exe` here."""
    for candidate in (name, name + ".exe"):
        if os.path.exists(candidate):
            return candidate
    raise SystemExit(f"missing {name}; run `cargo build -p retexis-gui -p retexis-node`")


def main():
    gui = built(os.path.join(ROOT, "target", "debug", "retexis-gui"))
    cli = built(os.path.join(ROOT, "target", "debug", "retexis"))

    subprocess.run(["taskkill", "/F", "/IM", "retexis-gui.exe"], capture_output=True)
    subprocess.run(["taskkill", "/F", "/IM", "retexis.exe"], capture_output=True)

    shot = os.path.join(TMP, "retexis-shot.bmp")
    if os.path.exists(shot):
        os.remove(shot)

    alice = subprocess.Popen(
        [
            gui,
            "--name", "alice",
            "--no-announce",
            "--listen", "tcp://127.0.0.1:7100",
            "--say", "都上线了吗？我在 7100",
        ],
        env={**os.environ, "RETEXIS_SHOT": shot},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(2.5)

    bob = subprocess.Popen(
        [
            cli,
            "--name", "bob",
            "--connect", "tcp://127.0.0.1:7100",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
        encoding="utf-8",
    )
    assert bob.stdin
    for delay, line in [
        (2.0, "在。刚连上，1 跳"),
        (2.5, "/msg alice 收到，ACK 回来了"),
        (2.5, f"/stream alice {LONG}"),
    ]:
        time.sleep(delay)
        bob.stdin.write(line + "\n")
        bob.stdin.flush()

    # The window rewrites the file every 400 ms; one more beat and it is current.
    time.sleep(1.2)
    width, height, rows = read_bmp(shot)
    target = os.path.join(ROOT, "docs", "gui.png")
    save_png(target, width, height, rows)
    save_png(os.path.join(TMP, "gui-demo.png"), width, height, rows)
    print(f"{width}x{height} -> {target}")

    bob.kill()
    alice.kill()


def dialogs():
    """Photograph the two dialogs, which are the parts no test can eyeball.

    `RETEXIS_DIALOG` makes the window open one of them on startup; the scan
    behind 发现 is the real one, so the picture is of a dialog with real
    answers in it rather than a mock with fake rows.
    """
    gui = built(os.path.join(ROOT, "target", "debug", "retexis-gui"))
    cli = built(os.path.join(ROOT, "target", "debug", "retexis"))
    subprocess.run(["taskkill", "/F", "/IM", "retexis-gui.exe"], capture_output=True)
    subprocess.run(["taskkill", "/F", "/IM", "retexis.exe"], capture_output=True)

    # A peer that listens, so it has an endpoint to put in its reply and the
    # scan has something to find.
    bob = subprocess.Popen(
        [cli, "--name", "bob", "--listen", "tcp://127.0.0.1:7111", "--announce"],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
        encoding="utf-8",
    )
    time.sleep(1.5)

    for kind, wait in (("discover", 4.0), ("connect", 2.5)):
        shot = os.path.join(TMP, f"retexis-{kind}.bmp")
        if os.path.exists(shot):
            os.remove(shot)
        window = subprocess.Popen(
            [
                gui,
                "--name", "alice",
                "--no-announce",
                "--listen", "tcp://127.0.0.1:7112",
            ],
            env={**os.environ, "RETEXIS_SHOT": shot, "RETEXIS_DIALOG": kind},
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        time.sleep(wait)
        width, height, rows = read_bmp(shot)
        target = os.path.join(ROOT, "docs", f"gui-{kind}.png")
        save_png(target, width, height, rows)
        print(f"{width}x{height} -> {target}")
        window.kill()
        time.sleep(0.5)

    bob.kill()


if __name__ == "__main__":
    if "--dialogs" in sys.argv:
        sys.exit(dialogs())
    sys.exit(main())
