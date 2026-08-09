#!/usr/bin/env python3
"""One-off local web UI for manually reviewing sampled NFT media pairs."""

from __future__ import annotations

import argparse
import csv
import json
import mimetypes
import os
import threading
import webbrowser
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse


POOLS = ("intra_chain", "cross_chain")
DECISIONS = {"same": "相同", "different": "不相同"}
CSV_NAMES = {
    "intra_chain": "intra_chain_review.csv",
    "cross_chain": "cross_chain_review.csv",
}
IMAGE_EXTENSIONS = {".avif", ".gif", ".jpeg", ".jpg", ".png", ".svg", ".webp"}
VIDEO_EXTENSIONS = {".m4v", ".mov", ".mp4", ".webm"}
AUDIO_EXTENSIONS = {".aac", ".flac", ".m4a", ".mp3", ".ogg", ".wav"}
MEDIA_EXTENSIONS = IMAGE_EXTENSIONS | VIDEO_EXTENSIONS | AUDIO_EXTENSIONS


PAGE = r"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>NFT 媒体对人工审核</title>
  <style>
    :root { color-scheme: light; font-family: system-ui, "Microsoft YaHei", sans-serif; }
    * { box-sizing: border-box; }
    body { margin: 0; background: #f4f6f8; color: #18212b; }
    header { height: 70px; padding: 10px 18px; display: flex; align-items: center;
      gap: 14px; background: #fff; border-bottom: 1px solid #d8dee6; }
    h1 { margin: 0; font-size: 19px; white-space: nowrap; }
    .pool-tabs { display: flex; gap: 8px; }
    .pool-tab { border: 1px solid #aeb8c4; background: #fff; border-radius: 7px;
      padding: 8px 13px; cursor: pointer; font-weight: 650; }
    .pool-tab.active { color: #fff; background: #2367d1; border-color: #2367d1; }
    #status { margin-left: auto; text-align: right; font-size: 14px; line-height: 1.45; }
    main { height: calc(100vh - 158px); min-height: 430px; padding: 12px;
      display: grid; grid-template-columns: minmax(220px, .9fr) minmax(260px, 1.15fr)
      minmax(260px, 1.15fr) minmax(220px, .9fr); gap: 10px; }
    .panel { min-width: 0; background: #fff; border: 1px solid #d8dee6;
      border-radius: 8px; overflow: hidden; display: flex; flex-direction: column; }
    .panel-title { padding: 8px 11px; border-bottom: 1px solid #e1e6ec;
      font-weight: 700; font-size: 13px; background: #fafbfc; }
    pre { margin: 0; padding: 11px; overflow: auto; white-space: pre-wrap;
      overflow-wrap: anywhere; font: 12px/1.45 Consolas, monospace; }
    .media-box { flex: 1; min-height: 0; padding: 10px; display: flex;
      align-items: center; justify-content: center; background: #eef1f4; overflow: auto; }
    .media-box img, .media-box video { display: block; max-width: 100%; max-height: 100%;
      object-fit: contain; background: #fff; }
    .media-box audio { width: min(95%, 520px); }
    .empty { color: #687586; text-align: center; padding: 30px; }
    footer { height: 88px; background: #fff; border-top: 1px solid #d8dee6;
      display: flex; align-items: center; justify-content: center; gap: 22px; }
    .decision { min-width: 210px; height: 56px; border: 0; border-radius: 9px;
      color: #fff; font-size: 19px; font-weight: 750; cursor: pointer; }
    .decision:disabled { opacity: .45; cursor: wait; }
    #same { background: #16834b; }
    #different { background: #c23b3b; }
    .hint { position: absolute; right: 18px; color: #697586; font-size: 12px; }
    @media (max-width: 1050px) {
      main { overflow-x: auto; grid-template-columns: 260px 320px 320px 260px; }
      .hint { display: none; }
    }
  </style>
</head>
<body>
  <header>
    <h1>NFT 媒体对人工审核</h1>
    <div class="pool-tabs">
      <button class="pool-tab" data-pool="intra_chain">单链</button>
      <button class="pool-tab" data-pool="cross_chain">跨链</button>
    </div>
    <div id="status">正在读取……</div>
  </header>
  <main id="review-grid">
    <section class="panel"><div class="panel-title" id="json-a-title">A JSON</div><pre id="json-a"></pre></section>
    <section class="panel"><div class="panel-title" id="media-a-title">媒体 A</div><div class="media-box" id="media-a"></div></section>
    <section class="panel"><div class="panel-title" id="media-b-title">媒体 B</div><div class="media-box" id="media-b"></div></section>
    <section class="panel"><div class="panel-title" id="json-b-title">B JSON</div><pre id="json-b"></pre></section>
  </main>
  <footer>
    <button class="decision" id="same">相同</button>
    <button class="decision" id="different">不相同</button>
    <span class="hint">快捷键：← / A = 相同，→ / D = 不相同</span>
  </footer>
  <script>
    let pool = localStorage.getItem("nft-review-pool") || "intra_chain";
    let current = null;
    let saving = false;

    const $ = id => document.getElementById(id);
    const buttons = [$('same'), $('different')];

    function setButtons(enabled) {
      buttons.forEach(button => button.disabled = !enabled);
    }

    function mediaElement(item, side) {
      const box = $(`media-${side}`);
      box.replaceChildren();
      const ext = item.media_ext.toLowerCase();
      let element;
      if ([".mp4", ".webm", ".mov", ".m4v"].includes(ext)) {
        element = document.createElement('video');
        element.controls = true;
        element.loop = true;
      } else if ([".mp3", ".wav", ".ogg", ".m4a", ".aac", ".flac"].includes(ext)) {
        element = document.createElement('audio');
        element.controls = true;
      } else {
        element = document.createElement('img');
        element.alt = `样本 ${current.id}${side}`;
      }
      element.src = item.media_url;
      box.appendChild(element);
    }

    function clearReview(message) {
      current = null;
      $('json-a').textContent = '';
      $('json-b').textContent = '';
      $('media-a').innerHTML = `<div class="empty">${message}</div>`;
      $('media-b').innerHTML = `<div class="empty">${message}</div>`;
      setButtons(false);
    }

    function updateTabs(stats) {
      document.querySelectorAll('.pool-tab').forEach(tab => {
        const name = tab.dataset.pool;
        tab.classList.toggle('active', name === pool);
        const label = name === 'intra_chain' ? '单链' : '跨链';
        const item = stats[name];
        const sameRatio = item.reviewed ? `${(item.same / item.reviewed * 100).toFixed(1)}%` : '—';
        tab.textContent = `${label} ${item.reviewed}/${item.total} · 相同 ${sameRatio}`;
      });
    }

    async function loadNext() {
      setButtons(false);
      const response = await fetch(`/api/state?pool=${encodeURIComponent(pool)}`, {cache: 'no-store'});
      if (!response.ok) throw new Error(await response.text());
      const data = await response.json();
      updateTabs(data.stats);
      const active = data.stats[pool];
      const sameRatio = active.reviewed ? `${(active.same / active.reviewed * 100).toFixed(1)}%` : '—';
      $('status').textContent = `${pool === 'intra_chain' ? '单链' : '跨链'}：已审核 ${active.reviewed} / ${active.total}`
        + `；相同 ${active.same} / ${active.reviewed}（${sameRatio}）`
        + (data.invalid_count ? `；忽略不完整目录 ${data.invalid_count}` : '');
      if (!data.pair) {
        clearReview('此分类已经审核完成');
        return;
      }
      current = data.pair;
      $('json-a').textContent = current.a.json_text;
      $('json-b').textContent = current.b.json_text;
      $('json-a-title').textContent = `${current.id}A JSON · ${current.a.chain}`;
      $('json-b-title').textContent = `${current.id}B JSON · ${current.b.chain}`;
      $('media-a-title').textContent = `${current.id}A · ${current.a.media_name}`;
      $('media-b-title').textContent = `${current.id}B · ${current.b.media_name}`;
      mediaElement(current.a, 'a');
      mediaElement(current.b, 'b');
      setButtons(true);
    }

    async function decide(decision) {
      if (!current || saving) return;
      saving = true;
      setButtons(false);
      $('status').textContent = `正在写入 ${current.id}……`;
      try {
        const response = await fetch('/api/review', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({pool, id: current.id, decision})
        });
        if (!response.ok) throw new Error(await response.text());
        await loadNext();
      } catch (error) {
        $('status').textContent = `写入失败：${error.message}`;
        setButtons(true);
      } finally {
        saving = false;
      }
    }

    document.querySelectorAll('.pool-tab').forEach(tab => tab.addEventListener('click', async () => {
      if (saving) return;
      pool = tab.dataset.pool;
      localStorage.setItem("nft-review-pool", pool);
      try { await loadNext(); } catch (error) { clearReview(error.message); }
    }));
    $('same').addEventListener('click', () => decide('same'));
    $('different').addEventListener('click', () => decide('different'));
    window.addEventListener('keydown', event => {
      if (event.repeat || saving || !current) return;
      if (event.key === 'ArrowLeft' || event.key.toLowerCase() === 'a') decide('same');
      if (event.key === 'ArrowRight' || event.key.toLowerCase() === 'd') decide('different');
    });
    loadNext().catch(error => clearReview(`加载失败：${error.message}`));
  </script>
</body>
</html>
"""


@dataclass(frozen=True)
class Side:
    json_path: Path
    media_path: Path


@dataclass(frozen=True)
class Pair:
    pool: str
    sample_id: str
    a: Side
    b: Side


def numeric_sort_key(value: str) -> tuple[int, int | str]:
    return (0, int(value)) if value.isdigit() else (1, value)


def load_side(pair_dir: Path, sample_id: str, suffix: str) -> Side:
    json_path = pair_dir / f"{sample_id}{suffix}.json"
    if not json_path.is_file():
        raise ValueError(f"missing JSON {sample_id}{suffix}")
    candidates = sorted(
        path
        for path in pair_dir.glob(f"{sample_id}{suffix}.*")
        if path.is_file() and path.suffix.lower() in MEDIA_EXTENSIONS
    )
    if len(candidates) != 1:
        raise ValueError(f"cannot identify media {sample_id}{suffix}")
    return Side(json_path=json_path, media_path=candidates[0])


def scan_pairs(root: Path) -> tuple[dict[str, list[Pair]], int]:
    pairs: dict[str, list[Pair]] = {pool: [] for pool in POOLS}
    invalid = 0
    for pool in POOLS:
        pool_dir = root / pool
        if not pool_dir.is_dir():
            continue
        pair_dirs = sorted((path for path in pool_dir.iterdir() if path.is_dir()), key=lambda p: numeric_sort_key(p.name))
        for pair_dir in pair_dirs:
            try:
                pair = Pair(
                    pool=pool,
                    sample_id=pair_dir.name,
                    a=load_side(pair_dir, pair_dir.name, "a"),
                    b=load_side(pair_dir, pair_dir.name, "b"),
                )
            except (OSError, ValueError):
                invalid += 1
                continue
            pairs[pool].append(pair)
    return pairs, invalid


def load_reviews(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    reviews: dict[str, str] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            sample_id = (row.get("编号") or "").strip()
            decision = (row.get("判断结果") or "").strip()
            if sample_id and decision in DECISIONS.values():
                reviews[sample_id] = decision
    return reviews


class ReviewState:
    def __init__(self, root: Path, results_dir: Path) -> None:
        self.root = root.resolve()
        self.results_dir = results_dir.resolve()
        self.pairs, self.invalid_count = scan_pairs(self.root)
        self.by_id = {pool: {pair.sample_id: pair for pair in values} for pool, values in self.pairs.items()}
        self.csv_paths = {pool: self.results_dir / CSV_NAMES[pool] for pool in POOLS}
        self.reviews = {pool: load_reviews(self.csv_paths[pool]) for pool in POOLS}
        self.lock = threading.Lock()

    def stats(self) -> dict[str, dict[str, int]]:
        return {
            pool: {
                "total": len(self.pairs[pool]),
                "reviewed": len(self.reviews[pool]),
                "same": sum(decision == DECISIONS["same"] for decision in self.reviews[pool].values()),
            }
            for pool in POOLS
        }

    def next_pair(self, pool: str) -> Pair | None:
        return next((pair for pair in self.pairs[pool] if pair.sample_id not in self.reviews[pool]), None)

    def record(self, pool: str, sample_id: str, decision: str) -> None:
        if pool not in POOLS or decision not in DECISIONS:
            raise ValueError("invalid pool or decision")
        if sample_id not in self.by_id[pool]:
            raise ValueError("unknown sample id")
        with self.lock:
            if sample_id in self.reviews[pool]:
                return
            self.results_dir.mkdir(parents=True, exist_ok=True)
            path = self.csv_paths[pool]
            needs_header = not path.exists() or path.stat().st_size == 0
            encoding = "utf-8-sig" if needs_header else "utf-8"
            with path.open("a", encoding=encoding, newline="") as handle:
                writer = csv.writer(handle)
                if needs_header:
                    writer.writerow(["编号", "判断结果"])
                writer.writerow([sample_id, DECISIONS[decision]])
                handle.flush()
                os.fsync(handle.fileno())
            self.reviews[pool][sample_id] = DECISIONS[decision]

    def media_key(self, pair: Pair, suffix: str) -> str:
        return f"{pair.pool}/{pair.sample_id}/{suffix}"

    def media_path(self, key: str) -> Path | None:
        parts = key.split("/")
        if len(parts) != 3:
            return None
        pool, sample_id, suffix = parts
        pair = self.by_id.get(pool, {}).get(sample_id)
        if pair is None:
            return None
        return pair.a.media_path if suffix == "a" else pair.b.media_path if suffix == "b" else None

    def pair_payload(self, pair: Pair) -> dict[str, Any]:
        def side_payload(side: Side, suffix: str) -> dict[str, str]:
            data = json.loads(side.json_path.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                raise ValueError(f"JSON root must be an object: {side.json_path}")
            return {
                "chain": str(data.get("chain", "未知链")),
                "json_text": json.dumps(data, ensure_ascii=False, indent=2),
                "media_name": side.media_path.name,
                "media_ext": side.media_path.suffix.lower(),
                "media_url": f"/media/{self.media_key(pair, suffix)}",
            }

        return {
            "id": pair.sample_id,
            "pool": pair.pool,
            "a": side_payload(pair.a, "a"),
            "b": side_payload(pair.b, "b"),
        }


class Handler(BaseHTTPRequestHandler):
    server: "ReviewServer"

    def log_message(self, format_string: str, *args: object) -> None:
        print(f"[{self.log_date_time_string()}] {format_string % args}")

    def send_bytes(self, data: bytes, content_type: str, status: HTTPStatus = HTTPStatus.OK) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def send_json(self, payload: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
        self.send_bytes(
            json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            "application/json; charset=utf-8",
            status,
        )

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self.send_bytes(PAGE.encode("utf-8"), "text/html; charset=utf-8")
            return
        if parsed.path == "/api/state":
            pool = parse_qs(parsed.query).get("pool", ["intra_chain"])[0]
            if pool not in POOLS:
                self.send_json({"error": "invalid pool"}, HTTPStatus.BAD_REQUEST)
                return
            with self.server.state.lock:
                pair = self.server.state.next_pair(pool)
                stats = self.server.state.stats()
            try:
                pair_payload = self.server.state.pair_payload(pair) if pair else None
            except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
                self.send_json(
                    {"error": f"cannot read sample {pair.sample_id}: {exc}"},
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                )
                return
            payload = {
                "pair": pair_payload,
                "stats": stats,
                "invalid_count": self.server.state.invalid_count,
            }
            self.send_json(payload)
            return
        if parsed.path.startswith("/media/"):
            media_path = self.server.state.media_path(parsed.path.removeprefix("/media/"))
            if media_path is None:
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            try:
                content = media_path.read_bytes()
            except OSError:
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            content_type = mimetypes.guess_type(media_path.name)[0] or "application/octet-stream"
            self.send_bytes(content, content_type)
            return
        self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:  # noqa: N802
        if urlparse(self.path).path != "/api/review":
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > 16_384:
                raise ValueError("invalid request size")
            payload = json.loads(self.rfile.read(length))
            pool = str(payload.get("pool", ""))
            sample_id = str(payload.get("id", ""))
            decision = str(payload.get("decision", ""))
            self.server.state.record(pool, sample_id, decision)
        except (ValueError, json.JSONDecodeError) as exc:
            self.send_json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)
            return
        except OSError as exc:
            self.send_json({"error": f"cannot write CSV: {exc}"}, HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json({"ok": True})


class ReviewServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: tuple[str, int], state: ReviewState) -> None:
        super().__init__(address, Handler)
        self.state = state


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[2]
    default_input = repo_root / "out" / "metadata_sample_images"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=default_input, help="metadata_sample_images directory")
    parser.add_argument("--results-dir", type=Path, help="CSV output directory; defaults to --input")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--no-browser", action="store_true", help="do not open the browser automatically")
    parser.add_argument("--check", action="store_true", help="scan inputs and print counts without starting a server")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = args.input.resolve()
    if not root.is_dir():
        raise SystemExit(f"input directory does not exist: {root}")
    if not 0 <= args.port <= 65_535:
        raise SystemExit("--port must be between 0 and 65535")
    state = ReviewState(root, (args.results_dir or root).resolve())
    print(f"Input: {state.root}")
    print(f"Valid pairs: intra_chain={len(state.pairs['intra_chain'])}, cross_chain={len(state.pairs['cross_chain'])}")
    print(f"Existing reviews: intra_chain={len(state.reviews['intra_chain'])}, cross_chain={len(state.reviews['cross_chain'])}")
    print(f"Ignored incomplete/invalid pair directories: {state.invalid_count}")
    print(f"CSV: {state.csv_paths['intra_chain']}")
    print(f"CSV: {state.csv_paths['cross_chain']}")
    if args.check:
        return 0

    server = ReviewServer(("127.0.0.1", args.port), state)
    host, port = server.server_address
    url = f"http://{host}:{port}/"
    print(f"Open: {url}")
    print("Press Ctrl+C to stop; restart with the same command to continue.")
    if not args.no_browser:
        threading.Timer(0.4, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        print("\nStopped. Saved CSV files are unchanged and can be resumed.")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
