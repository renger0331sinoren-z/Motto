"""
Motto v1.00.0  (FastAPI + WebSocket + WebRTC + SQLite)

主な機能
  - アカウント / フレンド(コード招待) / サーバー(作成・参加・公開検索) / ロール / DM
  - テキスト・ファイル(サイズ無制限)・スタンプ・リアクション・オリジナル絵文字・返信
  - ボイスチャンネル / DM通話 / ビデオ / 画面共有 / インカメラ・アウトカメラ切替
  - 通知 (ブラウザ通知 + Web Push) / PWA(ホーム画面に追加) / スマホ対応UI

起動:  pip install -r requirements.txt   →   python main.py
環境変数(任意):  DATA_DIR  データ保存先 (Renderのディスクを使うときに指定)
"""
import asyncio
import base64
import hashlib
import hmac
import http.client
import ipaddress
import json
import os
import re
import secrets
import socket
import sqlite3
import ssl as _ssl
import time
import traceback
import uuid
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlparse

from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response

app = FastAPI()

def _pick_data_dir():
    """データ保存先。再デプロイ(アップデート)でも消えないよう、永続ディスクを優先して使う。"""
    env = os.environ.get("DATA_DIR", "").strip()
    if env:
        return Path(env)
    for cand in ("/var/data", "/data", "/mnt/data"):  # Render / Fly などの永続ディスクの定番の場所
        p = Path(cand)
        if p.is_dir() and os.access(cand, os.W_OK):
            return p
    return Path(".")


DATA = _pick_data_dir()
DATA.mkdir(parents=True, exist_ok=True)
if not os.environ.get("DATABASE_URL", "").strip():
    if str(DATA.resolve()) == str(Path(".").resolve()):
        print("警告: データをプログラムと同じ場所に保存しています。Renderなどでは更新のたびに消えます。"
              "環境変数 DATABASE_URL に外部データベース(Neon)の接続URLを設定してください。")
    print("データ保存先:", DATA.resolve())
UPLOADS = DATA / "uploads"
UPLOADS.mkdir(exist_ok=True)
MAX_STAMP = 5 * 1024 * 1024  # スタンプ画像は5MBまで (通常ファイルは無制限)
MAX_EMOJI = 512 * 1024  # オリジナル絵文字は512KBまで
MAX_MEDIA = None  # バナー / 背景(画像・動画)は無制限
MEDIA_KINDS = ("banner", "bg")
MAX_AVATAR = 2 * 1024 * 1024  # アカウント / サーバーのアイコンは2MBまで
OWNER_NAME = os.environ.get("OWNER_NAME", "レンジャー").strip()  # サイトの管理者の名前 (公式アナウンスを投稿できるのはこの人だけ)
OWNER_PASSWORD = os.environ.get("OWNER_PASSWORD", "")  # 管理者アカウントを最初に作るときのパスワード (Renderの環境変数に設定)
if OWNER_NAME and not OWNER_PASSWORD:
    print(f"警告: 環境変数 OWNER_PASSWORD が未設定です。他の人が先に「{OWNER_NAME}」を登録すると管理者になってしまいます。")
MAX_CALL = 8  # 通話の最大人数 (メッシュ接続なので多すぎると重くなります)

# ------------------------------------------------------------------ DB
# DATABASE_URL (Neon などの外部PostgreSQL) があればそこに保存 → 更新しても消えない。
# 無ければ従来どおり SQLite (minicord.db) を使う。
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
SCHEMA = """
create table if not exists users(id integer primary key autoincrement, name text unique, salt blob, pw blob, created real);
create table if not exists sessions(token text primary key, user_id integer, created real);
create table if not exists servers(id integer primary key autoincrement, name text, owner_id integer, invite text unique);
create table if not exists members(server_id integer, user_id integer, primary key(server_id, user_id));
create table if not exists roles(id integer primary key autoincrement, server_id integer, name text, perms integer, is_default integer default 0);
create table if not exists member_roles(server_id integer, user_id integer, role_id integer, primary key(user_id, role_id));
create table if not exists channels(id integer primary key autoincrement, server_id integer, name text);
create table if not exists dm_members(channel_id integer, user_id integer, primary key(channel_id, user_id));
create table if not exists messages(id integer primary key autoincrement, channel_id integer, user_id integer, text text,
    file_id text, file_name text, file_type text, ts real, mentions text default '[]');
create table if not exists reactions(message_id integer, user_id integer, emoji text, ts real default 0, primary key(message_id, user_id, emoji));
create table if not exists friends(user_id integer, friend_id integer, primary key(user_id, friend_id));
create table if not exists blocks(user_id integer, blocked_id integer, primary key(user_id, blocked_id));
create table if not exists mutes(user_id integer, muted_id integer, primary key(user_id, muted_id));
create table if not exists stamps(id integer primary key autoincrement, name text, file_id text, mime text, owner_id integer, hidden integer default 0);
create table if not exists emojis(id integer primary key autoincrement, name text, file_id text, mime text, owner_id integer, hidden integer default 0);
create table if not exists kv(k text primary key, v text);
create table if not exists blobs(id text primary key, data blob);
create table if not exists push_subs(endpoint text primary key, user_id integer, sub text);
create index if not exists idx_msg on messages(channel_id, id);
"""
MIGRATIONS = [  # 古いDB(v2/v3)からの移行
    ("messages", "stamp_id", "integer"),
    ("messages", "reply_to", "integer"),
    ("channels", "kind", "text default 'text'"),
    ("channels", "owner_id", "integer"),
    ("users", "friend_code", "text"),
    ("users", "notify_mode", "text default 'mention'"),
    ("servers", "is_public", "integer default 0"),
    ("users", "bio", "text"),
    ("users", "avatar_file", "text"),
    ("users", "avatar_mime", "text"),
    ("users", "avatar_ver", "integer"),
    ("messages", "edited", "integer"),
    ("servers", "icon_file", "text"),
    ("servers", "icon_mime", "text"),
    ("servers", "icon_ver", "integer"),
    ("reactions", "ts", "real default 0"),
    ("users", "banner_file", "text"),
    ("users", "banner_mime", "text"),
    ("users", "banner_ver", "integer"),
    ("users", "banner_opacity", "integer default 100"),
    ("users", "bg_file", "text"),
    ("users", "bg_mime", "text"),
    ("users", "bg_ver", "integer"),
    ("users", "bg_opacity", "integer default 100"),
]
_ID_TABLES = {"users", "servers", "roles", "channels", "messages", "stamps", "emojis"}  # id列があるテーブル
_UPSERT = {"blobs": ("id", ["data"]), "kv": ("k", ["v"]), "push_subs": ("endpoint", ["user_id", "sub"])}


def pg_type(sql):
    """SQLiteの型名をPostgreSQLの型名に直す"""
    sql = re.sub(r"\binteger primary key autoincrement\b", "bigserial primary key", sql)
    sql = re.sub(r"\binteger\b", "bigint", sql)
    sql = re.sub(r"\breal\b", "double precision", sql)
    return re.sub(r"\bblob\b", "bytea", sql)


def pg_sql(sql):
    """SQLite用に書いたSQLをPostgreSQL用に変換する"""
    s = sql.strip().replace("%", "%%").replace("?", "%s")
    s = re.sub(r"\blike\b", "ilike", s)  # SQLiteのLIKEは大文字小文字を区別しない
    suffix = ""
    if re.match(r"insert or ignore into", s):
        s = s.replace("insert or ignore into", "insert into", 1)
        suffix = " on conflict do nothing"
    else:
        m = re.match(r"insert or replace into (\w+)", s)
        if m:
            key, cols = _UPSERT[m.group(1)]
            s = s.replace("insert or replace into", "insert into", 1)
            suffix = f" on conflict ({key}) do update set " + ",".join(f"{c}=excluded.{c}" for c in cols)
        else:
            m = re.match(r"insert into (\w+)", s)
            if m and m.group(1) in _ID_TABLES:
                suffix = " returning id"
    return s + suffix


class _Res:
    def __init__(self, rows=None, rowcount=-1, lastrowid=None):
        self._rows, self.rowcount, self.lastrowid, self._i = rows or [], rowcount, lastrowid, 0

    @staticmethod
    def _fix(r):
        return tuple(bytes(x) if isinstance(x, memoryview) else x for x in r)

    def fetchone(self):
        if self._i < len(self._rows):
            self._i += 1
            return self._fix(self._rows[self._i - 1])
        return None

    def fetchall(self):
        out = [self._fix(r) for r in self._rows[self._i:]]
        self._i = len(self._rows)
        return out

    def __iter__(self):
        return iter(self.fetchall())


class PGDB:
    """sqlite3 のように db.execute(...) で使える PostgreSQL 接続 (切れたら自動で再接続)"""

    def __init__(self, url):
        import psycopg2  # noqa

        self.pg = psycopg2
        self.url = url
        self.conn = None
        self._connect()

    def _connect(self):
        self.conn = self.pg.connect(self.url, connect_timeout=15, keepalives=1, keepalives_idle=30,
                                    keepalives_interval=10, keepalives_count=3)
        self.conn.autocommit = True  # 1文ごとに確定する (commit() は何もしない)

    def _run(self, fn):
        for attempt in (0, 1):
            try:
                with self.conn.cursor() as cur:
                    return fn(cur)
            except (self.pg.OperationalError, self.pg.InterfaceError):
                if attempt:
                    raise
                try:
                    self.conn.close()
                except Exception:
                    pass
                self._connect()

    def execute(self, sql, params=()):
        q = pg_sql(sql)

        def go(cur):
            cur.execute(q, tuple(params))
            rows = cur.fetchall() if cur.description else []
            last = rows[0][0] if rows and q.endswith("returning id") else None
            return _Res([] if last is not None else rows, cur.rowcount, last)

        return self._run(go)

    def executemany(self, sql, seq):
        q = pg_sql(sql)
        self._run(lambda cur: cur.executemany(q, [tuple(p) for p in seq]))

    def executescript(self, script):
        for st in pg_type(script).split(";"):
            if st.strip():
                self._run(lambda cur, st=st: cur.execute(st))

    def commit(self):
        pass


if DATABASE_URL:
    try:
        db = PGDB(DATABASE_URL)
    except ImportError:
        raise RuntimeError("DATABASE_URL が設定されていますが psycopg2-binary が入っていません (requirements.txt を確認してください)")
    DB_ERRORS = (sqlite3.IntegrityError, db.pg.IntegrityError)
    print("データ保存先: 外部データベース (PostgreSQL)")
    db.executescript(SCHEMA)
    for _t, _c, _d in MIGRATIONS:
        db.execute(f"alter table {_t} add column if not exists {_c} {pg_type(_d)}")
else:
    db = sqlite3.connect(str(DATA / "minicord.db"), check_same_thread=False)  # 旧バージョンと同じファイル名のまま (データ引き継ぎのため)
    DB_ERRORS = (sqlite3.IntegrityError,)
    db.execute("pragma journal_mode=WAL")
    db.execute("pragma synchronous=NORMAL")
    db.executescript(SCHEMA)

    def _has_col(t, c):
        return any(r[1] == c for r in db.execute(f"pragma table_info({t})"))

    for _t, _c, _d in MIGRATIONS:
        if not _has_col(_t, _c):
            db.execute(f"alter table {_t} add column {_c} {_d}")
db.commit()


# ------------------------------------------------------------------ アップロードファイルをDBにも控える
# DATABASE_URL (外部DB) を使っているときだけ有効。再デプロイでディスクが消えても、起動時にDBから復元する。
MIRROR_MAX = int(os.environ.get("MIRROR_MAX_MB", "50")) * 1024 * 1024


def mirror_file(fid):
    if not DATABASE_URL:
        return
    try:
        path = UPLOADS / fid
        if not path.exists() or path.stat().st_size > MIRROR_MAX:
            return
        data = path.read_bytes()
        db.execute("insert or replace into blobs values(?,?)", (fid, db.pg.Binary(data)))
    except Exception as e:  # noqa
        print("ファイルのバックアップに失敗:", e)


def rm_file(fid):
    if not fid:
        return
    (UPLOADS / fid).unlink(missing_ok=True)
    if DATABASE_URL:
        try:
            db.execute("delete from blobs where id=?", (fid,))
        except Exception:
            pass


def restore_blobs():
    if not DATABASE_URL:
        return
    n = 0
    try:
        for (fid,) in db.execute("select id from blobs").fetchall():
            if not re.fullmatch(r"[0-9a-f]{32}", fid) or (UPLOADS / fid).exists():
                continue
            r = db.execute("select data from blobs where id=?", (fid,)).fetchone()
            if r and r[0] is not None:
                (UPLOADS / fid).write_bytes(bytes(r[0]))
                n += 1
        if n:
            print(f"DBからファイルを {n} 件復元しました")
    except Exception as e:  # noqa
        print("ファイルの復元に失敗:", e)


restore_blobs()

# ------------------------------------------------------------------ アップデート機能 (管理者が main.py をアップロードして更新)
import subprocess
import sys
import tempfile

SELF = Path(__file__).resolve()


def _sha(b):
    return hashlib.sha256(b).hexdigest()


def _ukv_get(k, d=None):
    r = db.execute("select v from kv where k=?", (k,)).fetchone()
    return r[0] if r else d


def _ukv_set(k, v):
    db.execute("insert or replace into kv values(?,?)", (k, v))
    db.commit()


def _ukv_del(*ks):
    for k in ks:
        db.execute("delete from kv where k=?", (k,))
    db.commit()


def ver_parts():
    try:
        a, b, c = [int(x) for x in _ukv_get("app_ver", "1.0.0").split(".")]
        return a, b, c
    except Exception:  # noqa
        return 1, 0, 0


def ver_str(parts=None):
    a, b, c = parts or ver_parts()
    return f"v{a}.{b:02d}.{c}"


def ver_bump(parts, kind):
    a, b, c = parts
    if kind == "major":
        return a + 1, 0, 0
    if kind == "minor":
        return a, b + 1, 0
    return a, b, c + 1


def _write_self(data):
    tmp = SELF.with_name(SELF.name + ".new")
    tmp.write_bytes(data)
    os.replace(tmp, SELF)


def _restart():
    print("アップデートのため再起動します...")
    try:
        argv = list(getattr(sys, "orig_argv", None) or [sys.executable] + sys.argv)
        os.execv(sys.executable, argv)
    except Exception as e:  # noqa
        print("その場での再起動に失敗。プロセスを終了します (自動で再起動されます):", e)
        os._exit(3)


def _bootstrap_code():
    """起動時: 管理者がアップロードした最新コードを(再デプロイ後でも)引き継ぐ。起動に失敗した更新は自動で元に戻す。"""
    if os.environ.get("MOTTO_PREFLIGHT"):
        return
    try:
        cur = _sha(SELF.read_bytes())
        pend = _ukv_get("upd_pending")
        if pend is not None:
            prev = _ukv_get("code_prev")
            if pend != "0" and prev and cur == _ukv_get("code_sha"):
                print("更新後の起動に失敗したため、前のバージョンへ戻します")
                data = prev.encode("utf-8")
                _write_self(data)
                _ukv_set("code_py", prev)
                _ukv_set("code_sha", _sha(data))
                _ukv_set("app_ver", _ukv_get("ver_prev", "1.0.0"))
                _ukv_del("upd_pending", "code_prev")
                _restart()
                return
            _ukv_set("upd_pending", "1")
        stored = _ukv_get("code_py")
        if not stored:
            return
        if cur == _ukv_get("code_sha"):
            return
        if cur == _ukv_get("code_base"):
            print("管理者がアップロードした最新のコードで起動します")
            _write_self(stored.encode("utf-8"))
            _restart()
        else:
            print("新しいデプロイを検出。アップロード済みのコードは破棄しました")
            _ukv_del("code_py", "code_sha", "code_base", "code_prev", "upd_pending")
    except Exception as e:  # noqa
        print("アップデートの確認に失敗:", e)


_bootstrap_code()


@app.on_event("startup")
async def _upd_started():
    _ukv_del("upd_pending")  # ここまで来れば起動成功


# 公式アナウンス用の特別なチャンネル (全員が読めて、オーナーだけが投稿できる)
_a = db.execute("select id from channels where kind='announce'").fetchone()
if _a:
    ANNOUNCE_CID = _a[0]
else:
    ANNOUNCE_CID = db.execute("insert into channels(server_id,name,kind) values(NULL,'公式アナウンス','announce')").lastrowid
db.commit()

# ------------------------------------------------------------------ Web Push (pywebpush が入っていれば有効)
HAVE_PUSH = False
VAPID = None
VAPID_PUB = ""
VAPID_SUB = os.environ.get("VAPID_SUB", "mailto:admin@example.com")
try:
    from cryptography.hazmat.primitives import serialization
    from py_vapid import Vapid
    from pywebpush import WebPushException, webpush

    _r = db.execute("select v from kv where k='vapid_pem'").fetchone()
    if _r:
        VAPID = Vapid.from_pem(_r[0].encode())
    else:
        VAPID = Vapid()
        VAPID.generate_keys()
        db.execute("insert into kv values('vapid_pem',?)", (VAPID.private_pem().decode(),))
        db.commit()
    _pub = VAPID.public_key.public_bytes(serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint)
    VAPID_PUB = base64.urlsafe_b64encode(_pub).rstrip(b"=").decode()
    HAVE_PUSH = True
except Exception as _e:  # noqa
    print("Web Push は無効です (pywebpush 未インストール等):", _e)

_tasks: set = set()


def spawn(coro):
    t = asyncio.create_task(coro)
    _tasks.add(t)
    t.add_done_callback(_tasks.discard)


def _send_push(sub_json, payload):
    try:
        webpush(
            subscription_info=json.loads(sub_json),
            data=json.dumps(payload),
            vapid_private_key=VAPID,
            vapid_claims={"sub": VAPID_SUB},
            ttl=3600,
        )
        return 200
    except WebPushException as e:
        r = getattr(e, "response", None)
        return r.status_code if r is not None else 0
    except Exception:
        traceback.print_exc()
        return 0


# ------------------------------------------------------------------ 権限
P_SEND, P_FILES, P_REACT, P_CHANNELS, P_ROLES, P_ADMIN = 1, 2, 4, 8, 16, 32
ALL = 63
DEFAULT = P_SEND | P_FILES | P_REACT
PERM_NAMES = {
    "send": P_SEND, "files": P_FILES, "react": P_REACT,
    "channels": P_CHANNELS, "roles": P_ROLES, "admin": P_ADMIN,
}


_OWNER_CACHE = {"id": None}


def site_owner_id():
    if OWNER_NAME:  # 名前で決まる管理者 (名前の登録は register / update_profile で保護している)
        if _OWNER_CACHE["id"] is None:
            x = db.execute("select id from users where name=?", (OWNER_NAME,)).fetchone()
            _OWNER_CACHE["id"] = x[0] if x else None
        return _OWNER_CACHE["id"]
    r = db.execute("select v from kv where k='owner_id'").fetchone()
    if r:
        return int(r[0])
    if OWNER_NAME:
        x = db.execute("select id from users where name=?", (OWNER_NAME,)).fetchone()
    else:
        x = db.execute("select min(id) from users").fetchone()
    if x and x[0] is not None:
        db.execute("insert or replace into kv values('owner_id',?)", (str(x[0]),))
        db.commit()
        return x[0]
    return None


def is_site_owner(uid):
    return uid == site_owner_id()


def server_users(sid):
    return [r[0] for r in db.execute("select user_id from members where server_id=?", (sid,))]


def server_perms(uid, sid):
    s = db.execute("select owner_id from servers where id=?", (sid,)).fetchone()
    if not s or not db.execute("select 1 from members where server_id=? and user_id=?", (sid, uid)).fetchone():
        return 0
    if s[0] == uid:
        return ALL
    p = 0
    for (x,) in db.execute(
        "select perms from roles where server_id=? and (is_default=1 or id in "
        "(select role_id from member_roles where server_id=? and user_id=?))",
        (sid, sid, uid),
    ):
        p |= x
    return ALL if p & P_ADMIN else p


def chan_info(uid, cid):
    """アクセスできなければ None、できれば権限ビットを返す"""
    if cid == ANNOUNCE_CID:
        return DEFAULT if is_site_owner(uid) else 0  # オーナー以外は閲覧だけ
    ch = db.execute("select server_id from channels where id=?", (cid,)).fetchone()
    if not ch:
        return None
    if ch[0] is None:  # DM
        ok = db.execute("select 1 from dm_members where channel_id=? and user_id=?", (cid, uid)).fetchone()
        return DEFAULT if ok else None
    if not db.execute("select 1 from members where server_id=? and user_id=?", (ch[0], uid)).fetchone():
        return None
    return server_perms(uid, ch[0])


def require(uid, cid, bit=0):
    p = chan_info(uid, cid)
    if p is None:
        raise HTTPException(403, "このチャンネルにアクセスできません")
    if bit and not p & bit:
        raise HTTPException(403, "その操作の権限がありません")


def channel_users(cid):
    if cid == ANNOUNCE_CID:
        return [r[0] for r in db.execute("select id from users")]
    ch = db.execute("select server_id from channels where id=?", (cid,)).fetchone()
    if not ch:
        return []
    if ch[0] is None:
        return [r[0] for r in db.execute("select user_id from dm_members where channel_id=?", (cid,))]
    return server_users(ch[0])


# ------------------------------------------------------------------ リアルタイム配信
class Hub:
    def __init__(self):
        self.users: dict[int, set[WebSocket]] = {}
        # 通話ルーム: channel_id -> {user_id: {ws, name, mic, cam, screen}}
        self.rooms: dict[int, dict[int, dict]] = {}

    async def push(self, uids, event):
        data = json.dumps(event)
        for uid in set(uids):
            for ws in list(self.users.get(uid, ())):
                try:
                    await ws.send_text(data)
                except Exception:
                    self.users[uid].discard(ws)


hub = Hub()
push = hub.push


async def send_ws(ws, event):
    try:
        await ws.send_text(json.dumps(event))
    except Exception:
        pass


def friend_ids(uid):
    return [r[0] for r in db.execute("select friend_id from friends where user_id=?", (uid,))]


def is_blocked_between(a, b):
    """aとbのどちらかが相手をブロックしていれば True"""
    return bool(db.execute(
        "select 1 from blocks where (user_id=? and blocked_id=?) or (user_id=? and blocked_id=?)", (a, b, b, a)
    ).fetchone())


def dm_partner(uid, cid):
    """1対1のDMなら相手のid。DM以外 (サーバー / グループ / アナウンス) は None"""
    ch = db.execute("select server_id,kind from channels where id=?", (cid,)).fetchone()
    if not ch or ch[0] is not None or (ch[1] or "") in ("group", "announce"):
        return None
    r = db.execute("select user_id from dm_members where channel_id=? and user_id!=?", (cid, uid)).fetchone()
    return r[0] if r else None


def ensure_dm_allowed(uid, cid):
    """1対1のDMで、どちらかがブロックしていたら送信・通話を拒否する"""
    pid = dm_partner(uid, cid)
    if pid is None:
        return
    if db.execute("select 1 from blocks where user_id=? and blocked_id=?", (uid, pid)).fetchone():
        raise HTTPException(403, "ブロック中のユーザーです。ブロックを解除すると送信できます")
    if db.execute("select 1 from blocks where user_id=? and blocked_id=?", (pid, uid)).fetchone():
        raise HTTPException(403, "このユーザーにはメッセージを送れません")


def notif_suppressed(uid, sender_id):
    """uid が sender_id をミュート / ブロックしていれば、その人からの通知は出さない"""
    return bool(
        db.execute("select 1 from mutes where user_id=? and muted_id=?", (uid, sender_id)).fetchone()
        or db.execute("select 1 from blocks where user_id=? and blocked_id=?", (uid, sender_id)).fetchone()
    )


async def presence(uid, online):
    ids = set(friend_ids(uid))
    ids.update(r[0] for r in db.execute(
        "select user_id from members where server_id in (select server_id from members where user_id=?)", (uid,)))
    ids.update(r[0] for r in db.execute(
        "select d2.user_id from dm_members d join dm_members d2 on d2.channel_id=d.channel_id and d2.user_id!=d.user_id "
        "where d.user_id=?", (uid,)))
    ids.discard(uid)
    await push(ids, {"type": "presence", "id": uid, "online": online})


# ------------------------------------------------------------------ 通話ルーム
def room_users(cid):
    return [
        {"id": i, "name": v["name"], "mic": v["mic"], "cam": v["cam"], "screen": v["screen"]}
        for i, v in hub.rooms.get(cid, {}).items()
    ]


async def broadcast_call(cid):
    await push(channel_users(cid), {"type": "call_state", "channel": cid, "users": room_users(cid)})


async def call_leave(uid, cid):
    room = hub.rooms.get(cid)
    if room and room.pop(uid, None) is not None:
        if not room:
            del hub.rooms[cid]
        await broadcast_call(cid)


async def call_join(ws, uid, name, cid):
    require(uid, cid)
    ensure_dm_allowed(uid, cid)
    if cid == ANNOUNCE_CID:
        raise HTTPException(400, "このチャンネルでは通話できません")
    ch = db.execute("select server_id,kind from channels where id=?", (cid,)).fetchone()
    if ch[0] is not None and ch[1] != "voice":
        raise HTTPException(400, "このチャンネルでは通話できません")
    for ocid in [c for c, r in hub.rooms.items() if uid in r]:  # 同時に入れる通話は1つだけ
        await call_leave(uid, ocid)
    room = hub.rooms.setdefault(cid, {})
    if len(room) >= MAX_CALL:
        raise HTTPException(400, f"通話は最大{MAX_CALL}人までです")
    peers = [{"id": i, "name": v["name"]} for i, v in room.items()]
    room[uid] = {"ws": ws, "name": name, "mic": True, "cam": False, "screen": False}
    await send_ws(ws, {"type": "call_peers", "channel": cid, "peers": peers})
    await broadcast_call(cid)


# ------------------------------------------------------------------ 認証
def hash_pw(pw: str, salt: bytes) -> bytes:
    return hashlib.scrypt(pw.encode(), salt=salt, n=2**14, r=8, p=1, dklen=32)


def new_session(uid):
    t = secrets.token_urlsafe(32)
    db.execute("insert into sessions values(?,?,?)", (t, uid, time.time()))
    db.commit()
    return t


def user_from_token(t):
    r = db.execute(
        "select u.id,u.name from sessions s join users u on u.id=s.user_id where s.token=?", (t,)
    ).fetchone()
    return {"id": r[0], "name": r[1]} if r else None


def current_user(request: Request):
    h = request.headers.get("authorization", "")
    t = h[7:] if h.startswith("Bearer ") else request.query_params.get("token", "")
    u = user_from_token(t)
    if not u:
        raise HTTPException(401, "ログインが必要です")
    return u


def new_friend_code():
    return secrets.token_urlsafe(9)


@app.post("/api/register")
async def register(b: dict):
    name = str(b.get("name", "")).strip()
    pw = str(b.get("password", ""))
    if not re.fullmatch(r"[^\s@]{2,20}", name):
        raise HTTPException(400, "名前は2〜20文字で、空白と@は使えません")
    if len(pw) < 6:
        raise HTTPException(400, "パスワードは6文字以上にしてください")
    if db.execute("select 1 from users where name=?", (name,)).fetchone():
        raise HTTPException(400, "その名前は使われています")
    if OWNER_NAME and name == OWNER_NAME and OWNER_PASSWORD and pw != OWNER_PASSWORD:
        raise HTTPException(400, "その名前は使えません")
    salt = secrets.token_bytes(16)
    cur = db.execute(
        "insert into users(name,salt,pw,created,friend_code) values(?,?,?,?,?)",
        (name, salt, hash_pw(pw, salt), time.time(), new_friend_code()),
    )
    db.commit()
    _OWNER_CACHE["id"] = None
    return {"token": new_session(cur.lastrowid)}


@app.post("/api/login")
async def login(b: dict):
    name = str(b.get("name", "")).strip()
    r = db.execute("select id,salt,pw from users where name=?", (name,)).fetchone()
    if not r or not hmac.compare_digest(hash_pw(str(b.get("password", "")), r[1]), r[2]):
        raise HTTPException(401, "名前またはパスワードが違います")
    return {"token": new_session(r[0])}


@app.post("/api/logout")
async def logout(request: Request, u=Depends(current_user)):
    h = request.headers.get("authorization", "")
    db.execute("delete from sessions where token=?", (h[7:],))
    db.commit()
    return {"ok": True}


@app.get("/api/me")
async def me(u=Depends(current_user)):
    uid = u["id"]
    servers = []
    for sid, name, owner, invite, pub, icon in db.execute(
        "select s.id,s.name,s.owner_id,s.invite,s.is_public,case when s.icon_file is null then null else s.icon_ver end "
        "from servers s join members m on m.server_id=s.id where m.user_id=? order by s.id", (uid,)
    ).fetchall():
        chans = [
            {"id": i, "name": n, "kind": k or "text"}
            for i, n, k in db.execute("select id,name,kind from channels where server_id=? order by id", (sid,))
        ]
        servers.append({
            "id": sid, "name": name, "owner": owner == uid, "admin": bool(server_perms(uid, sid) & P_ADMIN),
            "invite": invite, "public": bool(pub), "icon": icon, "channels": chans,
        })
    dms = []
    friend_set = set(friend_ids(uid))
    muted_set = {r[0] for r in db.execute("select muted_id from mutes where user_id=?", (uid,))}
    blocked_set = {r[0] for r in db.execute("select blocked_id from blocks where user_id=?", (uid,))}
    for cid, pid, pname in db.execute(
        "select c.id,u2.id,u2.name from channels c "
        "join dm_members d on d.channel_id=c.id and d.user_id=? "
        "join dm_members d2 on d2.channel_id=c.id and d2.user_id!=? "
        "join users u2 on u2.id=d2.user_id where c.server_id is null and coalesce(c.kind,'') not in ('announce','group') order by c.id", (uid, uid)
    ):
        dms.append({"id": cid, "name": pname, "uid": pid, "online": bool(hub.users.get(pid)),
                    "friend": pid in friend_set, "muted": pid in muted_set, "blocked": pid in blocked_set})
    for gid, gname, gowner in db.execute(
        "select c.id,c.name,c.owner_id from channels c join dm_members d on d.channel_id=c.id and d.user_id=? "
        "where c.server_id is null and c.kind='group' order by c.id", (uid,)
    ).fetchall():
        mem = [
            {"id": i, "name": n, "online": bool(hub.users.get(i))}
            for i, n in db.execute("select u.id,u.name from dm_members d join users u on u.id=d.user_id where d.channel_id=? order by u.name", (gid,))
        ]
        dms.append({"id": gid, "name": gname, "uid": None, "online": False, "group": True, "members": mem, "owner": gowner == uid})
    row = db.execute("select friend_code,notify_mode from users where id=?", (uid,)).fetchone()
    code = row[0]
    if not code:
        code = new_friend_code()
        db.execute("update users set friend_code=? where id=?", (code, uid))
        db.commit()
    friends = [
        {"id": i, "name": n, "online": bool(hub.users.get(i))}
        for i, n in db.execute(
            "select u.id,u.name from friends f join users u on u.id=f.friend_id where f.user_id=? order by u.name", (uid,)
        )
    ]
    blocked_list = [
        {"id": i, "name": n}
        for i, n in db.execute("select u.id,u.name from blocks b join users u on u.id=b.blocked_id where b.user_id=? order by u.name", (uid,))
    ]
    muted_list = [
        {"id": i, "name": n}
        for i, n in db.execute("select u.id,u.name from mutes m join users u on u.id=m.muted_id where m.user_id=? order by u.name", (uid,))
    ]
    calls = {}
    for cid in list(hub.rooms):
        if hub.rooms.get(cid) and chan_info(uid, cid) is not None:
            calls[str(cid)] = room_users(cid)
    return {
        "user": u, "servers": servers, "dms": dms, "friend_code": code, "friends": friends, "blocked": blocked_list, "muted": muted_list, "calls": calls,
        "notify_mode": row[1] or "mention", "push_available": HAVE_PUSH,
        "announce": ANNOUNCE_CID, "is_owner": is_site_owner(uid), "persistent": bool(DATABASE_URL), "app_version": ver_str(),
    }


@app.post("/api/settings")
async def settings(b: dict, u=Depends(current_user)):
    mode = b.get("notify_mode")
    if mode not in ("all", "mention", "off"):
        raise HTTPException(400, "不正な設定です")
    db.execute("update users set notify_mode=? where id=?", (mode, u["id"]))
    db.commit()
    return {"ok": True}


# ------------------------------------------------------------------ プロフィール (アイコン / 名前 / 自己紹介)
@app.post("/api/profile/get")
async def profile_get(b: dict, u=Depends(current_user)):
    r = db.execute(
        "select id,name,bio,created,avatar_ver,avatar_file,banner_file,banner_mime,banner_ver,banner_opacity,"
        "bg_file,bg_mime,bg_ver,bg_opacity from users where name=?", (str(b.get("name", "")).strip(),)
    ).fetchone()
    if not r:
        raise HTTPException(404, "ユーザーが見つかりません")

    def media(i):
        has = bool(r[i]) and (UPLOADS / r[i]).exists()
        return {"has": has, "mime": r[i + 1] if has else "", "v": r[i + 2] or 0, "opacity": 100 if r[i + 3] is None else r[i + 3]}

    return {"banner": media(6), "bg": media(10), 
        "id": r[0], "name": r[1], "bio": r[2] or "", "created": r[3], "me": r[0] == u["id"],
        "online": bool(hub.users.get(r[0])), "avatar": r[4] if r[5] else None,
    }


@app.post("/api/profile")
async def update_profile(b: dict, u=Depends(current_user)):
    uid = u["id"]
    if "name" in b:
        name = str(b["name"]).strip()
        if name != u["name"]:
            if not re.fullmatch(r"[^\s@]{2,20}", name):
                raise HTTPException(400, "名前は2〜20文字で、空白と@は使えません")
            if OWNER_NAME and name == OWNER_NAME:
                raise HTTPException(400, "その名前は使えません")
            if db.execute("select 1 from users where name=? and id!=?", (name, uid)).fetchone():
                raise HTTPException(400, "その名前は使われています")
            try:
                db.execute("update users set name=? where id=?", (name, uid))
            except DB_ERRORS:
                raise HTTPException(400, "その名前は使われています")
            for room in hub.rooms.values():
                if uid in room:
                    room[uid]["name"] = name
    if "bio" in b:
        db.execute("update users set bio=? where id=?", (str(b["bio"]).strip()[:200], uid))
    for k in MEDIA_KINDS:
        if f"{k}_opacity" in b:
            try:
                v = max(0, min(100, int(b[f"{k}_opacity"])))
            except (TypeError, ValueError):
                raise HTTPException(400, "不透明度が正しくありません")
            db.execute(f"update users set {k}_opacity=? where id=?", (v, uid))
    db.commit()
    _OWNER_CACHE["id"] = None
    for cid in [c for c, r in hub.rooms.items() if uid in r]:
        await broadcast_call(cid)
    await push(list(hub.users.keys()), {"type": "profile"})
    return {"ok": True}


@app.post("/api/profile/avatar")
async def set_avatar(request: Request, u=Depends(current_user)):
    fid, mime = await save_image(request, MAX_AVATAR)
    old = db.execute("select avatar_file from users where id=?", (u["id"],)).fetchone()
    db.execute("update users set avatar_file=?,avatar_mime=?,avatar_ver=? where id=?", (fid, mime, int(time.time()), u["id"]))
    db.commit()
    if old and old[0]:
        rm_file(old[0])
    await push(list(hub.users.keys()), {"type": "profile"})
    return {"ok": True}


@app.delete("/api/profile/avatar")
async def delete_avatar(u=Depends(current_user)):
    old = db.execute("select avatar_file from users where id=?", (u["id"],)).fetchone()
    db.execute("update users set avatar_file=NULL,avatar_mime=NULL,avatar_ver=NULL where id=?", (u["id"],))
    db.commit()
    if old and old[0]:
        rm_file(old[0])
    await push(list(hub.users.keys()), {"type": "profile"})
    return {"ok": True}


@app.post("/api/profile/media/{kind}")
async def set_media(kind: str, request: Request, u=Depends(current_user)):
    if kind not in MEDIA_KINDS:
        raise HTTPException(404)
    fid, mime = await save_image(request, MAX_MEDIA, sniff_media)
    old = db.execute(f"select {kind}_file from users where id=?", (u["id"],)).fetchone()
    db.execute(f"update users set {kind}_file=?,{kind}_mime=?,{kind}_ver=? where id=?", (fid, mime, int(time.time()), u["id"]))
    db.commit()
    if old and old[0]:
        rm_file(old[0])
    await push(list(hub.users.keys()), {"type": "profile"})
    return {"ok": True}


@app.delete("/api/profile/media/{kind}")
async def delete_media(kind: str, u=Depends(current_user)):
    if kind not in MEDIA_KINDS:
        raise HTTPException(404)
    old = db.execute(f"select {kind}_file from users where id=?", (u["id"],)).fetchone()
    db.execute(f"update users set {kind}_file=NULL,{kind}_mime=NULL,{kind}_ver=NULL where id=?", (u["id"],))
    db.commit()
    if old and old[0]:
        rm_file(old[0])
    await push(list(hub.users.keys()), {"type": "profile"})
    return {"ok": True}


@app.get("/api/profile/media")
async def media_file(name: str, kind: str, u=Depends(current_user)):
    if kind not in MEDIA_KINDS:
        raise HTTPException(404)
    r = db.execute(f"select {kind}_file,{kind}_mime from users where name=?", (name,)).fetchone()
    if not r or not r[0] or not (UPLOADS / r[0]).exists():
        raise HTTPException(404)
    return FileResponse(
        UPLOADS / r[0], media_type=r[1],
        headers={"X-Content-Type-Options": "nosniff", "Cache-Control": "private, max-age=86400"},
    )


@app.get("/api/avatar")
async def avatar_img(name: str, u=Depends(current_user)):
    r = db.execute("select avatar_file,avatar_mime from users where name=?", (name,)).fetchone()
    if not r or not r[0] or not (UPLOADS / r[0]).exists():
        raise HTTPException(404)
    return FileResponse(
        UPLOADS / r[0], media_type=r[1],
        headers={"X-Content-Type-Options": "nosniff", "Cache-Control": "private, max-age=86400"},
    )


@app.get("/api/avatars")
async def avatars(u=Depends(current_user)):
    """自分と、同じサーバー / DM / フレンドの人のうち、アイコンを設定している人の一覧 (名前 -> 版)"""
    uid = u["id"]
    rows = db.execute(
        "select name,avatar_ver from users where avatar_file is not null and ("
        "id=? or id in (select user_id from members where server_id in (select server_id from members where user_id=?)) "
        "or id in (select user_id from dm_members where channel_id in (select channel_id from dm_members where user_id=?)) "
        "or id in (select friend_id from friends where user_id=?) or id=?)",
        (uid, uid, uid, uid, site_owner_id() or -1),
    ).fetchall()
    return {"avatars": {r[0]: r[1] for r in rows}}


# ------------------------------------------------------------------ Push通知の登録
@app.get("/api/push/key")
async def push_key(u=Depends(current_user)):
    return {"key": VAPID_PUB if HAVE_PUSH else None}


@app.post("/api/push/subscribe")
async def push_subscribe(b: dict, u=Depends(current_user)):
    sub = b.get("subscription") or {}
    ep = str(sub.get("endpoint", ""))[:600]
    if not HAVE_PUSH or not ep.startswith("https://"):
        raise HTTPException(400, "Push通知を登録できません")
    db.execute("insert or replace into push_subs values(?,?,?)", (ep, u["id"], json.dumps(sub)))
    db.commit()
    return {"ok": True}


# ------------------------------------------------------------------ フレンド
@app.post("/api/friends/add")
async def add_friend(b: dict, u=Depends(current_user)):
    code = str(b.get("code", "")).strip()
    if not code:
        raise HTTPException(400, "フレンドコードを入力してください")
    r = db.execute("select id,name from users where friend_code=?", (code,)).fetchone()
    if not r:
        raise HTTPException(404, "フレンドコードが見つかりません")
    if r[0] == u["id"]:
        raise HTTPException(400, "自分自身は追加できません")
    if is_blocked_between(u["id"], r[0]):
        raise HTTPException(403, "このユーザーとはフレンドになれません")
    db.execute("insert or ignore into friends values(?,?)", (u["id"], r[0]))
    db.execute("insert or ignore into friends values(?,?)", (r[0], u["id"]))
    db.commit()
    await push([u["id"], r[0]], {"type": "refresh"})
    return {"name": r[1]}


@app.post("/api/friends/remove")
async def remove_friend(b: dict, u=Depends(current_user)):
    fid = int(b.get("id", 0))
    db.execute("delete from friends where (user_id=? and friend_id=?) or (user_id=? and friend_id=?)", (u["id"], fid, fid, u["id"]))
    db.commit()
    await push([u["id"], fid], {"type": "refresh"})
    return {"ok": True}


def _target_user(u, b):
    try:
        tid = int(b.get("id", 0))
    except (TypeError, ValueError):
        raise HTTPException(400, "不正なユーザーです")
    if tid == u["id"]:
        raise HTTPException(400, "自分自身は指定できません")
    if not db.execute("select 1 from users where id=?", (tid,)).fetchone():
        raise HTTPException(404, "そのユーザーは存在しません")
    return tid


@app.post("/api/blocks/add")
async def block_add(b: dict, u=Depends(current_user)):
    tid = _target_user(u, b)
    db.execute("insert or ignore into blocks values(?,?)", (u["id"], tid))
    # ブロックするとフレンドも解除する
    db.execute("delete from friends where (user_id=? and friend_id=?) or (user_id=? and friend_id=?)", (u["id"], tid, tid, u["id"]))
    db.commit()
    await push([u["id"], tid], {"type": "refresh"})
    return {"ok": True}


@app.post("/api/blocks/remove")
async def block_remove(b: dict, u=Depends(current_user)):
    tid = _target_user(u, b)
    db.execute("delete from blocks where user_id=? and blocked_id=?", (u["id"], tid))
    db.commit()
    await push([u["id"], tid], {"type": "refresh"})
    return {"ok": True}


@app.post("/api/mutes/add")
async def mute_add(b: dict, u=Depends(current_user)):
    tid = _target_user(u, b)
    db.execute("insert or ignore into mutes values(?,?)", (u["id"], tid))
    db.commit()
    await push([u["id"]], {"type": "refresh"})
    return {"ok": True}


@app.post("/api/mutes/remove")
async def mute_remove(b: dict, u=Depends(current_user)):
    tid = _target_user(u, b)
    db.execute("delete from mutes where user_id=? and muted_id=?", (u["id"], tid))
    db.commit()
    await push([u["id"]], {"type": "refresh"})
    return {"ok": True}


@app.post("/api/friends/newcode")
async def new_code(u=Depends(current_user)):
    code = new_friend_code()
    db.execute("update users set friend_code=? where id=?", (code, u["id"]))
    db.commit()
    await push([u["id"]], {"type": "refresh"})
    return {"code": code}



def _preflight(data):
    """新しいコードを別プロセスで読み込んでみて、起動できるか確かめる (本番のデータには触れない)"""
    with tempfile.TemporaryDirectory() as td:
        f = Path(td) / "motto_new.py"
        f.write_bytes(data)
        env = dict(os.environ, MOTTO_PREFLIGHT="1", DATA_DIR=str(Path(td) / "data"))
        env.pop("DATABASE_URL", None)
        code = (
            "import importlib.util,sys\n"
            "sp=importlib.util.spec_from_file_location('motto_new',sys.argv[1])\n"
            "m=importlib.util.module_from_spec(sp)\n"
            "sp.loader.exec_module(m)\n"
            "assert hasattr(m,'app')\n"
            "print('PREFLIGHT_OK')\n"
        )
        try:
            r = subprocess.run([sys.executable, "-c", code, str(f)], cwd=td, env=env, capture_output=True, text=True, timeout=90)
        except subprocess.TimeoutExpired:
            return False, "テスト起動が90秒でタイムアウトしました"
        if r.returncode != 0 or "PREFLIGHT_OK" not in r.stdout:
            tail = (r.stderr or r.stdout).strip().splitlines()[-6:]
            return False, "テスト起動に失敗しました (足りないライブラリは requirements.txt にも追加してください):\n" + "\n".join(tail)
    return True, ""


def _need_owner(u):
    if not is_site_owner(u["id"]):
        raise HTTPException(403, "管理者だけが使えます")


def _log_update(entry):
    try:
        log = json.loads(_ukv_get("upd_log", "[]"))
    except Exception:  # noqa
        log = []
    log.insert(0, entry)
    _ukv_set("upd_log", json.dumps(log[:30], ensure_ascii=False))


@app.get("/api/admin/update")
async def admin_update_info(u=Depends(current_user)):
    _need_owner(u)
    try:
        log = json.loads(_ukv_get("upd_log", "[]"))
    except Exception:  # noqa
        log = []
    return {
        "version": ver_str(), "log": log, "can_rollback": bool(_ukv_get("code_prev")),
        "persistent": bool(DATABASE_URL), "writable": os.access(SELF.parent, os.W_OK),
    }


@app.post("/api/admin/update")
async def admin_update(b: dict, u=Depends(current_user)):
    _need_owner(u)
    code = b.get("code")
    if not isinstance(code, str) or len(code) < 1000 or len(code) > 12 * 1024 * 1024:
        raise HTTPException(400, "main.py のファイルを選んでください")
    kind = b.get("bump") if b.get("bump") in ("patch", "minor", "major") else "patch"
    notes = str(b.get("notes", "")).strip()[:1500]
    try:
        compile(code, "main.py", "exec")
    except SyntaxError as e:
        raise HTTPException(400, f"文法エラーがあります (行 {e.lineno}): {e.msg}")
    if "FastAPI(" not in code:
        raise HTTPException(400, "Mottoの main.py ではないようです")
    data = code.encode("utf-8")
    cur_bytes = SELF.read_bytes()
    if data == cur_bytes:
        raise HTTPException(400, "いま動いているものと同じファイルです")
    if not os.access(SELF.parent, os.W_OK):
        raise HTTPException(500, "サーバー上の main.py を書き換えられません")
    ok, err = await asyncio.to_thread(_preflight, data)
    if not ok:
        raise HTTPException(400, err)
    cur_sha = _sha(cur_bytes)
    base = (_ukv_get("code_base") or cur_sha) if (_ukv_get("code_py") and _ukv_get("code_sha") == cur_sha) else cur_sha
    prev_ver, new_ver = ver_parts(), ver_bump(ver_parts(), kind)
    try:
        SELF.with_name(SELF.name + ".bak").write_bytes(cur_bytes)
    except Exception:  # noqa
        pass
    _ukv_set("code_prev", cur_bytes.decode("utf-8", "replace"))
    _ukv_set("ver_prev", ".".join(map(str, prev_ver)))
    _ukv_set("code_py", code)
    _ukv_set("code_sha", _sha(data))
    _ukv_set("code_base", base)
    _ukv_set("app_ver", ".".join(map(str, new_ver)))
    _ukv_set("upd_pending", "0")
    _log_update({"v": ver_str(new_ver), "ts": time.time(), "notes": notes, "by": u["name"]})
    text = f"🆕 アップデート {ver_str(new_ver)}\n" + (notes or "機能の追加・不具合の修正を行いました。") + "\n\n(数秒間サーバーが再起動します。自動で再接続されます)"
    await post_message(u["id"], ANNOUNCE_CID, text)
    _write_self(data)
    asyncio.get_running_loop().call_later(1.5, _restart)
    return {"ok": True, "version": ver_str(new_ver)}


@app.post("/api/admin/rollback")
async def admin_rollback(u=Depends(current_user)):
    _need_owner(u)
    prev = _ukv_get("code_prev")
    if not prev:
        raise HTTPException(400, "戻せる前のバージョンがありません")
    data = prev.encode("utf-8")
    old_ver = ver_str()
    ver = tuple(int(x) for x in _ukv_get("ver_prev", "1.0.0").split("."))
    _ukv_set("code_py", prev)
    _ukv_set("code_sha", _sha(data))
    _ukv_set("app_ver", ".".join(map(str, ver)))
    _ukv_del("code_prev", "upd_pending")
    _log_update({"v": ver_str(ver), "ts": time.time(), "notes": f"{old_ver} から戻しました", "by": u["name"]})
    await post_message(u["id"], ANNOUNCE_CID, f"⏪ {old_ver} から {ver_str(ver)} に戻しました。\n(数秒間サーバーが再起動します)")
    _write_self(data)
    asyncio.get_running_loop().call_later(1.5, _restart)
    return {"ok": True, "version": ver_str(ver)}


# ------------------------------------------------------------------ サーバー / チャンネル / ロール
@app.post("/api/servers")
async def create_server(b: dict, u=Depends(current_user)):
    name = str(b.get("name", "")).strip()[:30]
    if not name:
        raise HTTPException(400, "サーバー名を入力してください")
    sid = db.execute(
        "insert into servers(name,owner_id,invite,is_public) values(?,?,?,?)",
        (name, u["id"], secrets.token_urlsafe(6), 1 if b.get("public") else 0),
    ).lastrowid
    db.execute("insert into roles(server_id,name,perms,is_default) values(?,?,?,1)", (sid, "@everyone", DEFAULT))
    db.execute("insert into roles(server_id,name,perms) values(?,?,?)", (sid, "Admin", ALL))
    db.execute("insert into members values(?,?)", (sid, u["id"]))
    db.execute("insert into channels(server_id,name,kind) values(?,?,'text')", (sid, "general"))
    db.execute("insert into channels(server_id,name,kind) values(?,?,'voice')", (sid, "ボイス"))
    db.commit()
    return {"id": sid}


@app.post("/api/servers/join")
async def join_server(b: dict, u=Depends(current_user)):
    r = db.execute("select id from servers where invite=?", (str(b.get("invite", "")).strip(),)).fetchone()
    if not r:
        raise HTTPException(404, "招待コードが見つかりません")
    db.execute("insert or ignore into members values(?,?)", (r[0], u["id"]))
    db.commit()
    await push(server_users(r[0]), {"type": "refresh"})
    return {"id": r[0]}


@app.get("/api/servers/discover")
async def discover(q: str = "", u=Depends(current_user)):
    like = "%" + q.replace("%", "").replace("_", "").strip() + "%"
    rows = db.execute(
        "select s.id,s.name,(select count(*) from members m where m.server_id=s.id),"
        "exists(select 1 from members m2 where m2.server_id=s.id and m2.user_id=?),"
        "case when s.icon_file is null then null else s.icon_ver end "
        "from servers s where s.is_public=1 and s.name like ? order by 3 desc, s.id limit 50",
        (u["id"], like),
    ).fetchall()
    return {"servers": [{"id": r[0], "name": r[1], "members": r[2], "joined": bool(r[3]), "icon": r[4]} for r in rows]}


@app.post("/api/servers/{sid}/join_public")
async def join_public(sid: int, u=Depends(current_user)):
    r = db.execute("select is_public from servers where id=?", (sid,)).fetchone()
    if not r or not r[0]:
        raise HTTPException(404, "公開サーバーが見つかりません")
    db.execute("insert or ignore into members values(?,?)", (sid, u["id"]))
    db.commit()
    await push(server_users(sid), {"type": "refresh"})
    return {"id": sid}


@app.post("/api/servers/{sid}/public")
async def set_public(sid: int, b: dict, u=Depends(current_user)):
    r = db.execute("select owner_id from servers where id=?", (sid,)).fetchone()
    if not r or r[0] != u["id"]:
        raise HTTPException(403, "サーバーのオーナーだけが変更できます")
    db.execute("update servers set is_public=? where id=?", (1 if b.get("public") else 0, sid))
    db.commit()
    await push(server_users(sid), {"type": "refresh"})
    return {"ok": True}


@app.post("/api/servers/{sid}/edit")
async def edit_server(sid: int, b: dict, u=Depends(current_user)):
    if not server_perms(u["id"], sid) & P_ADMIN:
        raise HTTPException(403, "サーバーの管理者だけが変更できます")
    name = str(b.get("name", "")).strip()[:30]
    if not name:
        raise HTTPException(400, "サーバー名を入力してください")
    db.execute("update servers set name=? where id=?", (name, sid))
    db.commit()
    await push(server_users(sid), {"type": "refresh"})
    return {"ok": True}


@app.post("/api/servers/{sid}/icon")
async def set_server_icon(sid: int, request: Request, u=Depends(current_user)):
    if not server_perms(u["id"], sid) & P_ADMIN:
        raise HTTPException(403, "サーバーの管理者だけが変更できます")
    fid, mime = await save_image(request, MAX_AVATAR)
    old = db.execute("select icon_file from servers where id=?", (sid,)).fetchone()
    db.execute("update servers set icon_file=?,icon_mime=?,icon_ver=? where id=?", (fid, mime, int(time.time()), sid))
    db.commit()
    if old and old[0]:
        rm_file(old[0])
    await push(server_users(sid), {"type": "refresh"})
    return {"ok": True}


@app.delete("/api/servers/{sid}/icon")
async def delete_server_icon(sid: int, u=Depends(current_user)):
    if not server_perms(u["id"], sid) & P_ADMIN:
        raise HTTPException(403, "サーバーの管理者だけが変更できます")
    old = db.execute("select icon_file from servers where id=?", (sid,)).fetchone()
    db.execute("update servers set icon_file=NULL,icon_mime=NULL,icon_ver=NULL where id=?", (sid,))
    db.commit()
    if old and old[0]:
        rm_file(old[0])
    await push(server_users(sid), {"type": "refresh"})
    return {"ok": True}


@app.get("/api/servers/{sid}/icon")
async def server_icon(sid: int, u=Depends(current_user)):
    r = db.execute("select icon_file,icon_mime from servers where id=?", (sid,)).fetchone()
    if not r or not r[0] or not (UPLOADS / r[0]).exists():
        raise HTTPException(404)
    return FileResponse(
        UPLOADS / r[0], media_type=r[1],
        headers={"X-Content-Type-Options": "nosniff", "Cache-Control": "private, max-age=86400"},
    )


@app.get("/api/servers/{sid}/members")
async def list_members(sid: int, u=Depends(current_user)):
    s = db.execute("select owner_id from servers where id=?", (sid,)).fetchone()
    if not s or not db.execute("select 1 from members where server_id=? and user_id=?", (sid, u["id"])).fetchone():
        raise HTTPException(403, "このサーバーのメンバーではありません")
    members = []
    for uid, name in db.execute(
        "select u.id,u.name from members m join users u on u.id=m.user_id where m.server_id=? order by u.name", (sid,)
    ).fetchall():
        roles = [r[0] for r in db.execute(
            "select r.name from member_roles mr join roles r on r.id=mr.role_id where mr.server_id=? and mr.user_id=?", (sid, uid))]
        members.append({"id": uid, "name": name, "owner": uid == s[0], "roles": roles, "online": bool(hub.users.get(uid))})
    return {"members": members}


@app.post("/api/servers/{sid}/channels")
async def create_channel(sid: int, b: dict, u=Depends(current_user)):
    if not server_perms(u["id"], sid) & P_CHANNELS:
        raise HTTPException(403, "チャンネル管理の権限がありません")
    name = re.sub(r"\s+", "-", str(b.get("name", "")).strip())[:30]
    if not name:
        raise HTTPException(400, "チャンネル名を入力してください")
    kind = "voice" if b.get("kind") == "voice" else "text"
    cid = db.execute("insert into channels(server_id,name,kind) values(?,?,?)", (sid, name, kind)).lastrowid
    db.commit()
    await push(server_users(sid), {"type": "refresh"})
    return {"id": cid}


@app.post("/api/servers/{sid}/roles")
async def create_role(sid: int, b: dict, u=Depends(current_user)):
    mine = server_perms(u["id"], sid)
    if not mine & P_ROLES:
        raise HTTPException(403, "ロール管理の権限がありません")
    name = str(b.get("name", "")).strip()[:30]
    if not name:
        raise HTTPException(400, "ロール名を入力してください")
    perms = 0
    for p in b.get("perms", []):
        perms |= PERM_NAMES.get(str(p), 0)
    if perms & ~mine:  # 自分が持っていない権限を持つロールは作れない(権限昇格の防止)
        raise HTTPException(403, "自分が持っていない権限は付与できません")
    if db.execute("select 1 from roles where server_id=? and name=?", (sid, name)).fetchone():
        raise HTTPException(400, "同じ名前のロールがあります")
    db.execute("insert into roles(server_id,name,perms) values(?,?,?)", (sid, name, perms))
    db.commit()
    await push(server_users(sid), {"type": "refresh"})
    return {"ok": True}


@app.post("/api/servers/{sid}/assign")
async def assign_role(sid: int, b: dict, u=Depends(current_user)):
    mine = server_perms(u["id"], sid)
    if not mine & P_ROLES:
        raise HTTPException(403, "ロール管理の権限がありません")
    t = db.execute(
        "select u.id from users u join members m on m.user_id=u.id where u.name=? and m.server_id=?",
        (str(b.get("user", "")), sid),
    ).fetchone()
    role = db.execute("select id,perms,is_default from roles where server_id=? and name=?", (sid, str(b.get("role", "")))).fetchone()
    if not t:
        raise HTTPException(404, "そのユーザーはこのサーバーにいません")
    if not role or role[2]:
        raise HTTPException(404, "ロールが見つかりません")
    if role[1] & ~mine:
        raise HTTPException(403, "自分が持っていない権限のロールは付与できません")
    if b.get("remove"):
        db.execute("delete from member_roles where user_id=? and role_id=?", (t[0], role[0]))
    else:
        db.execute("insert or ignore into member_roles values(?,?,?)", (sid, t[0], role[0]))
    db.commit()
    await push(server_users(sid), {"type": "refresh"})
    return {"ok": True}


# ------------------------------------------------------------------ DM
@app.post("/api/dm")
async def open_dm(b: dict, u=Depends(current_user)):
    t = db.execute("select id from users where name=?", (str(b.get("name", "")).strip(),)).fetchone()
    if not t:
        raise HTTPException(404, "そのユーザーは存在しません")
    if t[0] == u["id"]:
        raise HTTPException(400, "自分にはDMを送れません")
    r = db.execute(
        "select c.id from channels c where c.server_id is null and coalesce(c.kind,'') not in ('group','announce') "
        "and exists(select 1 from dm_members where channel_id=c.id and user_id=?) "
        "and exists(select 1 from dm_members where channel_id=c.id and user_id=?)", (u["id"], t[0])
    ).fetchone()
    if r:
        return {"id": r[0]}
    if is_blocked_between(u["id"], t[0]):
        raise HTTPException(403, "このユーザーにはDMを送れません")
    cid = db.execute("insert into channels(server_id,name,kind) values(NULL,'dm','dm')").lastrowid
    db.executemany("insert into dm_members values(?,?)", [(cid, u["id"]), (cid, t[0])])
    db.commit()
    await push([u["id"], t[0]], {"type": "refresh"})
    return {"id": cid}




# ------------------------------------------------------------------ グループ (3人以上のチャット / 通話)
def _group_member(uid, cid):
    r = db.execute("select c.name from channels c join dm_members d on d.channel_id=c.id and d.user_id=? "
                   "where c.id=? and c.kind='group' and c.server_id is null", (uid, cid)).fetchone()
    if not r:
        raise HTTPException(404, "グループが見つかりません")
    return r[0]


def _user_ids(names):
    ids = []
    for n in names:
        r = db.execute("select id from users where name=?", (str(n).strip(),)).fetchone()
        if r and r[0] not in ids:
            ids.append(r[0])
    return ids


@app.post("/api/groups")
async def group_create(b: dict, u=Depends(current_user)):
    name = str(b.get("name", "")).strip()[:30]
    if not name:
        raise HTTPException(400, "グループ名を入れてください")
    ids = [i for i in _user_ids(b.get("members") or []) if i != u["id"]][:49]
    cid = db.execute("insert into channels(server_id,name,kind,owner_id) values(NULL,?,'group',?)", (name, u["id"])).lastrowid
    db.executemany("insert into dm_members values(?,?)", [(cid, x) for x in [u["id"]] + ids])
    db.commit()
    await push([u["id"]] + ids, {"type": "refresh"})
    return {"id": cid}


@app.post("/api/groups/{cid}/add")
async def group_add(cid: int, b: dict, u=Depends(current_user)):
    _group_member(u["id"], cid)
    have = {r[0] for r in db.execute("select user_id from dm_members where channel_id=?", (cid,))}
    if len(have) >= 50:
        raise HTTPException(400, "グループは50人までです")
    ids = [i for i in _user_ids(b.get("names") or []) if i not in have]
    db.executemany("insert or ignore into dm_members values(?,?)", [(cid, x) for x in ids])
    db.commit()
    await push(list(have) + ids, {"type": "refresh"})
    return {"ok": True, "added": len(ids)}


@app.post("/api/groups/{cid}/rename")
async def group_rename(cid: int, b: dict, u=Depends(current_user)):
    _group_member(u["id"], cid)
    name = str(b.get("name", "")).strip()[:30]
    if not name:
        raise HTTPException(400, "グループ名を入れてください")
    db.execute("update channels set name=? where id=?", (name, cid))
    db.commit()
    await push(channel_users(cid), {"type": "refresh"})
    return {"ok": True}


@app.post("/api/groups/{cid}/leave")
async def group_leave(cid: int, u=Depends(current_user)):
    _group_member(u["id"], cid)
    users = channel_users(cid)
    await call_leave(u["id"], cid)
    db.execute("delete from dm_members where channel_id=? and user_id=?", (cid, u["id"]))
    if not db.execute("select 1 from dm_members where channel_id=?", (cid,)).fetchone():
        for r in db.execute("select file_id from messages where channel_id=? and file_id is not null", (cid,)).fetchall():
            rm_file(r[0])
        ids = [r[0] for r in db.execute("select id from messages where channel_id=?", (cid,)).fetchall()]
        for mid in ids:
            db.execute("delete from reactions where message_id=?", (mid,))
        db.execute("delete from messages where channel_id=?", (cid,))
        db.execute("delete from channels where id=?", (cid,))
    db.commit()
    await push(users, {"type": "refresh"})
    return {"ok": True}

# ------------------------------------------------------------------ 画像(スタンプ / オリジナル絵文字)
def sniff_image(h: bytes):
    if h.startswith(b"\x89PNG"):
        return "image/png"
    if h.startswith(b"\xff\xd8"):
        return "image/jpeg"
    if h.startswith(b"GIF8"):
        return "image/gif"
    if h[:4] == b"RIFF" and h[8:12] == b"WEBP":
        return "image/webp"
    return None


def sniff_media(h: bytes):
    m = sniff_image(h)
    if m:
        return m
    if h[4:8] == b"ftyp":
        return "video/mp4"
    if h.startswith(b"\x1a\x45\xdf\xa3"):
        return "video/webm"
    return None


async def save_image(request: Request, limit: int, sniff=None):
    fid = uuid.uuid4().hex
    path = UPLOADS / fid
    size, head = 0, b""
    try:
        with open(path, "wb") as f:
            async for chunk in request.stream():
                size += len(chunk)
                if limit and size > limit:
                    raise HTTPException(413, f"ファイルは{limit / 1048576:g}MBまでです")
                if len(head) < 16:
                    head += chunk[: 16 - len(head)]
                f.write(chunk)
        mime = (sniff or sniff_image)(head)
        if not mime:
            raise HTTPException(400, "png / jpg / gif / webp / mp4 / webm のファイルを選んでください" if sniff else "png / jpg / gif / webp の画像を選んでください")
    except BaseException:
        path.unlink(missing_ok=True)
        raise
    await asyncio.to_thread(mirror_file, fid)
    return fid, mime


def image_response(table, iid):
    r = db.execute(f"select file_id,mime from {table} where id=?", (iid,)).fetchone()
    if not r or not (UPLOADS / r[0]).exists():
        raise HTTPException(404)
    return FileResponse(
        UPLOADS / r[0], media_type=r[1],
        headers={"X-Content-Type-Options": "nosniff", "Cache-Control": "private, max-age=86400"},
    )


@app.get("/api/stamps")
async def list_stamps(u=Depends(current_user)):
    rows = db.execute("select id,name,owner_id,file_id from stamps where hidden=0 order by id").fetchall()
    return {"stamps": [{"id": r[0], "name": r[1], "mine": r[2] == u["id"]} for r in rows if (UPLOADS / r[3]).exists()]}


@app.post("/api/stamps")
async def add_stamp(request: Request, name: str = "stamp", u=Depends(current_user)):
    fid, mime = await save_image(request, MAX_STAMP)
    db.execute("insert into stamps(name,file_id,mime,owner_id) values(?,?,?,?)", (name.strip()[:20] or "stamp", fid, mime, u["id"]))
    db.commit()
    await push(list(hub.users.keys()), {"type": "stamps"})
    return {"ok": True}


@app.delete("/api/stamps/{sid}")
async def delete_stamp(sid: int, u=Depends(current_user)):
    cur = db.execute("update stamps set hidden=1 where id=? and owner_id=?", (sid, u["id"]))  # 過去のメッセージ表示は残す
    db.commit()
    if cur.rowcount == 0:
        raise HTTPException(403, "自分のスタンプだけ削除できます")
    await push(list(hub.users.keys()), {"type": "stamps"})
    return {"ok": True}


@app.get("/api/stamps/{sid}/img")
async def stamp_img(sid: int, u=Depends(current_user)):
    return image_response("stamps", sid)


@app.get("/api/emojis")
async def list_emojis(u=Depends(current_user)):
    rows = db.execute("select id,name,owner_id,file_id from emojis where hidden=0 order by name").fetchall()
    return {"emojis": [{"id": r[0], "name": r[1], "mine": r[2] == u["id"]} for r in rows if (UPLOADS / r[3]).exists()]}


@app.post("/api/emojis")
async def add_emoji(request: Request, name: str = "", u=Depends(current_user)):
    name = name.strip().lower()
    if not re.fullmatch(r"[a-z0-9_]{2,12}", name):
        raise HTTPException(400, "名前は英小文字・数字・_ の2〜12文字にしてください")
    if db.execute("select 1 from emojis where name=? and hidden=0", (name,)).fetchone():
        raise HTTPException(400, "その名前は使われています")
    fid, mime = await save_image(request, MAX_EMOJI)
    db.execute("insert into emojis(name,file_id,mime,owner_id) values(?,?,?,?)", (name, fid, mime, u["id"]))
    db.commit()
    await push(list(hub.users.keys()), {"type": "emojis"})
    return {"ok": True}


@app.delete("/api/emojis/{eid}")
async def delete_emoji(eid: int, u=Depends(current_user)):
    cur = db.execute("update emojis set hidden=1 where id=? and owner_id=?", (eid, u["id"]))
    db.commit()
    if cur.rowcount == 0:
        raise HTTPException(403, "自分が追加した絵文字だけ削除できます")
    await push(list(hub.users.keys()), {"type": "emojis"})
    return {"ok": True}


@app.get("/api/emojis/{eid}/img")
async def emoji_img(eid: int, u=Depends(current_user)):
    return image_response("emojis", eid)


def valid_emoji(e: str):
    e = e.strip()
    if not e or len(e) > 16:
        return None
    if e.startswith(":"):
        if not re.fullmatch(r":[a-z0-9_]{2,12}:", e) or not db.execute(
            "select 1 from emojis where name=? and hidden=0", (e[1:-1],)
        ).fetchone():
            raise HTTPException(404, "そのオリジナル絵文字は存在しません")
    return e


# ------------------------------------------------------------------ メッセージ
MSG_SQL = (
    "select m.id,m.channel_id,u.name,m.text,m.file_id,m.file_name,m.file_type,m.ts,m.mentions,m.stamp_id,m.reply_to,m.edited "
    "from messages m join users u on u.id=m.user_id "
)


def reactions_of(mid):
    d: dict[str, list[str]] = {}
    for emoji, name in db.execute(
        "select r.emoji,u.name from reactions r join users u on u.id=r.user_id where r.message_id=? order by r.ts", (mid,)
    ):
        d.setdefault(emoji, []).append(name)
    return d


def reply_info(rid):
    if not rid:
        return None
    r = db.execute(
        "select m.id,u.name,m.text,m.file_id,m.stamp_id from messages m join users u on u.id=m.user_id where m.id=?", (rid,)
    ).fetchone()
    if not r:
        return None
    kind = "stamp" if r[4] else ("file" if r[3] and not r[2] else "text")
    return {"id": r[0], "user": r[1], "text": (r[2] or "")[:80], "kind": kind}


_NO = object()


def row_to_msg(r, rx=_NO, rep=_NO):
    return {
        "id": r[0], "channel": r[1], "user": r[2], "text": r[3], "file_id": r[4], "file_name": r[5],
        "file_type": r[6], "ts": r[7], "mentions": json.loads(r[8]), "stamp": r[9],
        "reply": reply_info(r[10]) if rep is _NO else rep, "edited": bool(r[11]),
        "reactions": reactions_of(r[0]) if rx is _NO else rx,
    }


def find_mentions(cid, text):
    names = set(re.findall(r"@([^\s@]+)", text))
    ids = channel_users(cid)
    if not names or not ids:
        return []
    rows = db.execute(f"select name from users where id in ({','.join('?' * len(ids))})", ids).fetchall()
    valid = {r[0] for r in rows}
    return sorted(n for n in names if n in valid)


async def push_offline(cid, msg, sender_id):
    """アプリを開いていない人へ Web Push を送る"""
    ch = db.execute("select server_id,name from channels where id=?", (cid,)).fetchone()
    if not ch:
        return
    title = msg["user"]
    if cid == ANNOUNCE_CID:
        title = "公式アナウンス"
    elif ch[0] is not None:
        sv = db.execute("select name from servers where id=?", (ch[0],)).fetchone()
        title = f"{msg['user']} ({sv[0] if sv else ''} / {ch[1]})"
    body = (msg["text"] or "")[:120] or ("スタンプを送信しました" if msg["stamp"] else "ファイルを送信しました")
    loop = asyncio.get_running_loop()
    for uid in channel_users(cid):
        if uid == sender_id or hub.users.get(uid):
            continue
        row = db.execute("select name,notify_mode from users where id=?", (uid,)).fetchone()
        if not row or row[1] == "off":
            continue
        if notif_suppressed(uid, sender_id):
            continue
        hit = ch[0] is None or row[0] in msg["mentions"] or bool(msg["reply"] and msg["reply"]["user"] == row[0])
        if (row[1] or "mention") == "mention" and not hit:
            continue
        subs = db.execute("select endpoint,sub from push_subs where user_id=?", (uid,)).fetchall()
        for ep, sub in subs:
            code = await loop.run_in_executor(None, _send_push, sub, {"title": title, "body": body, "channel": cid, "tag": f"ch{cid}"})
            if code in (404, 410):
                db.execute("delete from push_subs where endpoint=?", (ep,))
                db.commit()


async def post_message(uid, cid, text="", file=None, stamp=None, reply_to=None):
    ensure_dm_allowed(uid, cid)
    if reply_to and not db.execute("select 1 from messages where id=? and channel_id=?", (reply_to, cid)).fetchone():
        reply_to = None
    ments = find_mentions(cid, text)
    cur = db.execute(
        "insert into messages(channel_id,user_id,text,file_id,file_name,file_type,ts,mentions,stamp_id,reply_to) "
        "values(?,?,?,?,?,?,?,?,?,?)",
        (cid, uid, text, *(file or (None, None, None)), time.time(), json.dumps(ments), stamp, reply_to),
    )
    db.commit()
    msg = row_to_msg(db.execute(MSG_SQL + "where m.id=?", (cur.lastrowid,)).fetchone())
    await push(channel_users(cid), {"type": "message", "message": msg})
    if HAVE_PUSH:
        spawn(push_offline(cid, msg, uid))


async def toggle_reaction(uid, mid, emoji):
    r = db.execute("select channel_id from messages where id=?", (mid,)).fetchone()
    if not r:
        raise HTTPException(404, "メッセージが見つかりません")
    require(uid, r[0], P_REACT)
    emoji = valid_emoji(emoji)
    if not emoji:
        return
    cur = db.execute("delete from reactions where message_id=? and user_id=? and emoji=?", (mid, uid, emoji))
    if cur.rowcount == 0:
        db.execute("insert into reactions values(?,?,?,?)", (mid, uid, emoji, time.time()))
    db.commit()
    await push(channel_users(r[0]), {"type": "reactions", "channel": r[0], "message_id": mid, "reactions": reactions_of(mid)})


@app.get("/api/channels/{cid}/messages")
async def get_messages(cid: int, u=Depends(current_user)):
    require(u["id"], cid)
    rows = db.execute(MSG_SQL + "where m.channel_id=? order by m.id desc limit 100", (cid,)).fetchall()
    rows = list(reversed(rows))
    ids = [r[0] for r in rows]
    rx: dict = {i: {} for i in ids}
    if ids:
        for mid, emoji, name in db.execute(
            "select r.message_id,r.emoji,u.name from reactions r join users u on u.id=r.user_id "
            f"where r.message_id in ({','.join('?' * len(ids))}) order by r.ts", ids
        ):
            rx[mid].setdefault(emoji, []).append(name)
    rids = list({r[10] for r in rows if r[10]})
    reps: dict = {}
    if rids:
        for x in db.execute(
            "select m.id,u.name,m.text,m.file_id,m.stamp_id from messages m join users u on u.id=m.user_id "
            f"where m.id in ({','.join('?' * len(rids))})", rids
        ):
            reps[x[0]] = {"id": x[0], "user": x[1], "text": (x[2] or "")[:80],
                          "kind": "stamp" if x[4] else ("file" if x[3] and not x[2] else "text")}
    return {"messages": [row_to_msg(r, rx[r[0]], reps.get(r[10]) if r[10] else None) for r in rows]}


@app.get("/api/channels/{cid}/search")
async def search_messages(cid: int, q: str = "", u=Depends(current_user)):
    require(u["id"], cid)
    q = q.strip()[:100]
    if not q:
        return {"messages": []}
    like = "%" + q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
    rows = db.execute(MSG_SQL + "where m.channel_id=? and m.text like ? escape '\\' order by m.id desc limit 50", (cid, like)).fetchall()
    return {"messages": [row_to_msg(r) for r in rows]}


@app.post("/api/messages/{mid}/edit")
async def edit_message(mid: int, b: dict, u=Depends(current_user)):
    r = db.execute("select channel_id,user_id,stamp_id from messages where id=?", (mid,)).fetchone()
    if not r:
        raise HTTPException(404, "メッセージが見つかりません")
    if r[1] != u["id"]:
        raise HTTPException(403, "自分のメッセージだけ編集できます")
    require(u["id"], r[0], P_SEND)
    if r[2]:
        raise HTTPException(400, "スタンプは編集できません")
    text = str(b.get("text", "")).strip()[:4000]
    if not text:
        raise HTTPException(400, "メッセージを入力してください")
    db.execute(
        "update messages set text=?, mentions=?, edited=? where id=?",
        (text, json.dumps(find_mentions(r[0], text)), int(time.time()), mid),
    )
    db.commit()
    msg = row_to_msg(db.execute(MSG_SQL + "where m.id=?", (mid,)).fetchone())
    await push(channel_users(r[0]), {"type": "message_edit", "message": msg})
    return {"ok": True}


@app.delete("/api/messages/{mid}")
async def delete_message(mid: int, u=Depends(current_user)):
    r = db.execute("select channel_id,user_id,file_id from messages where id=?", (mid,)).fetchone()
    if not r:
        raise HTTPException(404, "メッセージが見つかりません")
    cid = r[0]
    perms = chan_info(u["id"], cid)
    if perms is None:
        raise HTTPException(403, "このチャンネルにアクセスできません")
    if r[1] != u["id"]:  # 他の人のメッセージ: 公式アナウンスはオーナー、サーバーは管理者だけ。DMは不可
        ch = db.execute("select server_id from channels where id=?", (cid,)).fetchone()
        if cid == ANNOUNCE_CID:
            ok = is_site_owner(u["id"])
        elif ch and ch[0] is not None:
            ok = bool(perms & P_ADMIN)
        else:
            ok = False
        if not ok:
            raise HTTPException(403, "このメッセージは削除できません")
    users = channel_users(cid)
    db.execute("delete from reactions where message_id=?", (mid,))
    db.execute("update messages set reply_to=NULL where reply_to=?", (mid,))
    db.execute("delete from messages where id=?", (mid,))
    db.commit()
    if r[2]:
        rm_file(r[2])
    await push(users, {"type": "message_delete", "channel": cid, "id": mid})
    return {"ok": True}


@app.post("/api/channels/{cid}/upload")
async def upload(cid: int, request: Request, name: str = "file",
                 type: str = "application/octet-stream", u=Depends(current_user)):
    """サイズ制限なし。受け取りながらディスクへ書くのでメモリを使わない。"""
    require(u["id"], cid, P_FILES)
    ensure_dm_allowed(u["id"], cid)  # ブロック中ならファイルを受け取る前に断る
    fid = uuid.uuid4().hex
    path = UPLOADS / fid
    try:
        with open(path, "wb") as f:
            async for chunk in request.stream():
                f.write(chunk)
    except BaseException:
        path.unlink(missing_ok=True)  # 途中で切れたら破棄
        raise
    await asyncio.to_thread(mirror_file, fid)
    await post_message(u["id"], cid, "", (fid, name[:200], type[:100]))
    return {"ok": True}


@app.get("/files/{fid}")
async def get_file(fid: str, dl: int = 0, u=Depends(current_user)):
    r = db.execute("select channel_id,file_name,file_type from messages where file_id=?", (fid,)).fetchone()
    if not r or chan_info(u["id"], r[0]) is None or not (UPLOADS / fid).exists():
        raise HTTPException(404)
    ftype = r[2] or ""
    # HTML等をその場で開くとXSSになるので、動画/音声/一部の画像だけ表示し、他はダウンロード扱い
    safe = ftype.startswith(("video/", "audio/")) or ftype in ("image/png", "image/jpeg", "image/gif", "image/webp")
    return FileResponse(
        UPLOADS / fid,
        media_type=ftype if safe else "application/octet-stream",
        filename=r[1],
        content_disposition_type="inline" if safe and not dl else "attachment",
        headers={"X-Content-Type-Options": "nosniff"},
    )


# ------------------------------------------------------------------ WebSocket
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket, token: str = ""):
    u = user_from_token(token)
    if not u:
        await ws.close(code=4401)
        return
    uid = u["id"]
    await ws.accept()
    first = not hub.users.get(uid)
    hub.users.setdefault(uid, set()).add(ws)
    if first:
        await presence(uid, True)
    try:
        while True:
            raw = await ws.receive_text()
            try:
                d = json.loads(raw)
                t = d.get("type")
                if t == "send":
                    cid = int(d["channel"])
                    text = str(d.get("text", "")).strip()[:4000]
                    require(uid, cid, P_SEND)
                    if text:
                        rp = d.get("reply_to")
                        await post_message(uid, cid, text, None, None, int(rp) if rp else None)
                elif t == "stamp":
                    cid = int(d["channel"])
                    sid = int(d["stamp_id"])
                    require(uid, cid, P_SEND)
                    if not db.execute("select 1 from stamps where id=?", (sid,)).fetchone():
                        raise HTTPException(404, "スタンプが見つかりません")
                    rp = d.get("reply_to")
                    await post_message(uid, cid, "", None, sid, int(rp) if rp else None)
                elif t == "typing":
                    cid = int(d["channel"])
                    require(uid, cid, P_SEND)
                    await push([x for x in channel_users(cid) if x != uid and not is_blocked_between(uid, x)], {"type": "typing", "channel": cid, "name": u["name"]})
                elif t == "react":
                    await toggle_reaction(uid, int(d["message_id"]), str(d["emoji"])[:16])
                elif t == "call_join":
                    await call_join(ws, uid, u["name"], int(d["channel"]))
                elif t == "call_leave":
                    await call_leave(uid, int(d["channel"]))
                elif t == "call_media":
                    cid = next((c for c, r in hub.rooms.items() if r.get(uid, {}).get("ws") is ws), None)
                    if cid is not None:
                        e = hub.rooms[cid][uid]
                        e["mic"] = bool(d.get("mic", True))
                        e["cam"] = bool(d.get("cam", False))
                        e["screen"] = bool(d.get("screen", False))
                        await broadcast_call(cid)
                elif t == "rtc":  # WebRTCのシグナリング(同じ通話ルームの相手にだけ中継)
                    cid, to = int(d["channel"]), int(d["to"])
                    room = hub.rooms.get(cid, {})
                    if uid in room and to in room:
                        await send_ws(room[to]["ws"], {"type": "rtc", "from": uid, "name": u["name"], "channel": cid, "data": d["data"]})
            except HTTPException as e:
                await send_ws(ws, {"type": "error", "message": e.detail})
            except (ValueError, KeyError, TypeError):
                pass
            except WebSocketDisconnect:
                raise
            except Exception:
                traceback.print_exc()
    except WebSocketDisconnect:
        pass
    finally:
        for cid in [c for c, r in hub.rooms.items() if r.get(uid, {}).get("ws") is ws]:
            await call_leave(uid, cid)
        conns = hub.users.get(uid)
        if conns is not None:
            conns.discard(ws)
            if not conns:
                del hub.users[uid]
                await presence(uid, False)


# ------------------------------------------------------------------ アイコン / PWA / Service Worker
ICON192 = base64.b64decode("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")
ICON512 = base64.b64decode("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")
ICONMASK = base64.b64decode("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")
CACHE = {"Cache-Control": "public, max-age=86400"}


@app.get("/icon-192.png")
async def icon192():
    return Response(ICON192, media_type="image/png", headers=CACHE)


@app.get("/icon-512.png")
async def icon512():
    return Response(ICON512, media_type="image/png", headers=CACHE)


@app.get("/icon-maskable.png")
async def icon_mask():
    return Response(ICONMASK, media_type="image/png", headers=CACHE)


@app.get("/manifest.webmanifest")
async def manifest():
    return JSONResponse(
        {
            "name": "Motto", "short_name": "Motto", "description": "みんなともっと楽しい時間を",
            "start_url": "/", "scope": "/", "display": "standalone",
            "background_color": "#0f1015", "theme_color": "#0f1015",
            "icons": [
                {"src": "/icon-192.png", "sizes": "192x192", "type": "image/png", "purpose": "any"},
                {"src": "/icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "any"},
                {"src": "/icon-maskable.png", "sizes": "512x512", "type": "image/png", "purpose": "maskable"},
            ],
        },
        media_type="application/manifest+json",
    )


SW_JS = r"""
self.addEventListener("install",()=>self.skipWaiting());
self.addEventListener("activate",e=>e.waitUntil(self.clients.claim()));
self.addEventListener("push",e=>{
  let d={};try{d=e.data.json()}catch(x){}
  e.waitUntil(self.registration.showNotification(d.title||"Motto",{
    body:d.body||"",icon:"/icon-192.png",badge:"/icon-192.png",tag:d.tag||"minicord",data:{channel:d.channel}}));
});
self.addEventListener("notificationclick",e=>{
  e.notification.close();
  const cid=e.notification.data&&e.notification.data.channel;
  e.waitUntil((async()=>{
    const cs=await self.clients.matchAll({type:"window",includeUncontrolled:true});
    for(const c of cs){if("focus" in c){await c.focus();c.postMessage({type:"open",channel:cid});return}}
    if(self.clients.openWindow)await self.clients.openWindow(cid?"/?c="+cid:"/");
  })());
});
self.addEventListener("fetch",()=>{});
"""


@app.get("/sw.js")
async def sw():
    return Response(SW_JS, media_type="application/javascript", headers={"Cache-Control": "no-cache", "Service-Worker-Allowed": "/"})


# ------------------------------------------------------------------ リンクのプレビュー (OGP)
# 他人のURLをサーバーが取りに行くので、内部ネットワークへの接続 (SSRF) は厳しく防ぐ:
#  - http/https の 80/443/8080/8443 だけ
#  - 名前解決した全アドレスが「公開アドレス」でなければ拒否 (localhost・プライベート・リンクローカル等)
#  - 検証したIPへそのまま接続 (DNSの引き直しで差し替えられない)、リダイレクトも毎回検証
#  - 本文は最大 512KB / 全体 10秒 まで
class _PinHTTP(http.client.HTTPConnection):
    def __init__(self, host, ip, port, timeout):
        super().__init__(host, port, timeout=timeout)
        self._ip = ip

    def connect(self):
        self.sock = socket.create_connection((self._ip, self.port), self.timeout)


class _PinHTTPS(http.client.HTTPSConnection):
    def __init__(self, host, ip, port, timeout):
        super().__init__(host, port, timeout=timeout, context=_ssl.create_default_context())
        self._ip = ip

    def connect(self):
        sock = socket.create_connection((self._ip, self.port), self.timeout)
        self.sock = self._context.wrap_socket(sock, server_hostname=self.host)


def _resolve_public(host, port):
    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except (OSError, UnicodeError):
        raise ValueError("名前解決できません")
    first = None
    for info in infos:
        addr = info[4][0].split("%")[0]
        ip = ipaddress.ip_address(addr)
        if getattr(ip, "ipv4_mapped", None):
            ip = ip.ipv4_mapped
        if not ip.is_global:
            raise ValueError("このアドレスには接続できません")
        first = first or info[4][0]
    if not first:
        raise ValueError("名前解決できません")
    return first


def _safe_fetch(url, accept, read_types, max_bytes, timeout=6, total=10):
    """(最終URL, content-type, charset, 本文 or None) を返す。本文は read_types に当てはまるときだけ読む"""
    deadline = time.monotonic() + total
    for _ in range(5):
        p = urlparse(url)
        if p.scheme not in ("http", "https") or not p.hostname:
            raise ValueError("http / https のURLだけ使えます")
        port = p.port or (443 if p.scheme == "https" else 80)
        if port not in (80, 443, 8080, 8443):
            raise ValueError("このポートには接続できません")
        ip = _resolve_public(p.hostname, port)
        conn = (_PinHTTPS if p.scheme == "https" else _PinHTTP)(p.hostname, ip, port, timeout)
        try:
            path = (p.path or "/") + (("?" + p.query) if p.query else "")
            conn.request("GET", path, headers={
                "User-Agent": "Mozilla/5.0 (compatible; MottoLinkPreview/1.0)",
                "Accept": "text/html,application/xhtml+xml,image/*;q=0.8",
                "Accept-Language": "ja,en;q=0.8",
                "Accept-Encoding": "identity",
            })
            r = conn.getresponse()
            if r.status in (301, 302, 303, 307, 308):
                loc = r.getheader("Location")
                if not loc:
                    raise ValueError("リダイレクト先がありません")
                url = urljoin(url, loc)
                continue
            if r.status != 200:
                raise ValueError("取得できません")
            head = r.getheader("Content-Type") or ""
            ctype = head.split(";")[0].strip().lower()
            if not any(ctype.startswith(a) for a in accept):
                raise ValueError("対応していない形式です")
            m = re.search(r"charset=([\w\-]+)", head, re.I)
            body = None
            if any(ctype.startswith(t) for t in read_types):
                buf = bytearray()
                while len(buf) < max_bytes and time.monotonic() < deadline:
                    chunk = r.read(min(16384, max_bytes - len(buf)))
                    if not chunk:
                        break
                    buf += chunk
                body = bytes(buf)
            return url, ctype, (m.group(1) if m else ""), body
        finally:
            conn.close()
    raise ValueError("リダイレクトが多すぎます")


class _OG(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.meta = {}
        self.title = ""
        self._in_title = False

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "meta":
            k = (a.get("property") or a.get("name") or "").lower()
            if k and a.get("content") and k not in self.meta:
                self.meta[k] = a["content"]
        elif tag == "title":
            self._in_title = True

    def handle_endtag(self, tag):
        if tag == "title":
            self._in_title = False

    def handle_data(self, data):
        if self._in_title and len(self.title) < 300:
            self.title += data


def _decode_html(body, charset):
    if not charset:
        m = re.search(rb"<meta[^>]+charset=[\"']?([\w\-]+)", body[:4096], re.I)
        charset = m.group(1).decode("ascii", "ignore") if m else "utf-8"
    try:
        return body.decode(charset, "replace")
    except LookupError:
        return body.decode("utf-8", "replace")


def _clip(s, n):
    return " ".join((s or "").split())[:n]


_IMG_OK = ("image/png", "image/jpeg", "image/gif", "image/webp", "image/avif")
_LP_CACHE = {}


def _link_preview(url):
    now = time.time()
    hit = _LP_CACHE.get(url)
    if hit and hit[0] > now:
        return hit[1]
    try:
        final, ctype, cs, body = _safe_fetch(
            url, ("text/html", "application/xhtml", "image/"), ("text/html", "application/xhtml"), 512 * 1024
        )
        if ctype.startswith("image/"):
            res = {"ok": ctype in _IMG_OK, "kind": "image", "url": final, "image": final}
        else:
            og = _OG()
            try:
                og.feed(_decode_html(body or b"", cs))
            except Exception:
                pass
            mt = og.meta
            title = _clip(mt.get("og:title") or mt.get("twitter:title") or og.title, 120)
            desc = _clip(mt.get("og:description") or mt.get("twitter:description") or mt.get("description"), 200)
            img = mt.get("og:image") or mt.get("og:image:url") or mt.get("twitter:image") or ""
            img = urljoin(final, img.strip()) if img else ""
            if not img.startswith(("http://", "https://")):
                img = ""
            res = {
                "ok": bool(title or desc or img),
                "url": final,
                "site": _clip(mt.get("og:site_name") or (urlparse(final).hostname or ""), 60),
                "title": title,
                "desc": desc,
                "image": img,
            }
    except Exception:
        res = {"ok": False}
    if len(_LP_CACHE) > 300:
        _LP_CACHE.pop(next(iter(_LP_CACHE)))
    _LP_CACHE[url] = (now + (3600 if res.get("ok") else 300), res)
    return res


@app.get("/api/linkpreview")
async def link_preview(url: str, u=Depends(current_user)):
    if len(url) > 2000:
        return {"ok": False}
    return await asyncio.to_thread(_link_preview, url)


@app.get("/api/linkimg")
async def link_img(url: str, u=Depends(current_user)):
    """プレビュー画像はサーバー経由で配る (相手サイトにIPが渡らない / http画像のmixed contentも避けられる)"""
    if len(url) > 2000:
        raise HTTPException(404, "画像を取得できません")
    try:
        _final, ctype, _cs, body = await asyncio.to_thread(_safe_fetch, url, _IMG_OK, _IMG_OK, 3 * 1024 * 1024)
    except Exception:
        raise HTTPException(404, "画像を取得できません")
    return Response(
        body,
        media_type=ctype,
        headers={
            "Cache-Control": "private, max-age=86400",
            "X-Content-Type-Options": "nosniff",
            "Content-Security-Policy": "default-src 'none'; sandbox",
        },
    )


# ------------------------------------------------------------------ 画面
PAGE = r"""<!doctype html><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name=theme-color content="#0f1015">
<meta name=mobile-web-app-capable content=yes>
<meta name=apple-mobile-web-app-capable content=yes>
<title>Motto</title>
<link rel=preconnect href=https://fonts.googleapis.com><link rel=preconnect href=https://fonts.gstatic.com crossorigin>
<link rel=stylesheet href="https://fonts.googleapis.com/css2?family=M+PLUS+Rounded+1c:wght@400;500;700;800&display=swap">
<script>(function(){var t=localStorage.theme||"system";var l=t==="light"||(t==="system"&&matchMedia("(prefers-color-scheme: light)").matches);document.documentElement.dataset.theme=l?"light":"dark"})()</script>
<link rel=icon type=image/png href=/icon-192.png>
<link rel=apple-touch-icon href=/icon-192.png>
<link rel=manifest href=/manifest.webmanifest>
<style>
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
[hidden]{display:none!important}
:root{--bg:#050507;--bg2:#0c0c0f;--bg3:#101014;--bg4:#1b1b21;--line:#202028;--tx:#eceef7;--tx2:#9a9eb6;--hi:#fff;--hov:#ffffff0d;--hov2:#ffffff16;--shadow:#0008;
 --acc:#6b7cff;--acc2:#e86bc3;--grad:linear-gradient(135deg,var(--acc),var(--acc2));--ok:#2fd27a;--bad:#ff5a6a;--rad:14px;
 --sat:env(safe-area-inset-top,0px);--sab:env(safe-area-inset-bottom,0px)}
:root[data-theme=light]{--bg:#eceef7;--bg2:#ffffff;--bg3:#f5f6fc;--bg4:#e4e7f3;--line:#dadef0;--tx:#1b1d2b;--tx2:#656b86;--hi:#10111a;--hov:#0000000a;--hov2:#00000012;--shadow:#3a3f6a22}
html,body{height:100%}
body{margin:0;font-family:"Noto Sans JP","Hiragino Sans","Yu Gothic UI","Meiryo",system-ui,sans-serif;background:var(--bg);color:var(--tx);font-size:15px;overflow:hidden}
button{font:inherit;border:0;border-radius:11px;padding:9px 14px;background:var(--acc);color:#fff;cursor:pointer;display:inline-flex;align-items:center;justify-content:center;gap:7px;transition:filter .15s,background .15s,transform .1s}
button:hover{filter:brightness(1.12)}button:active{transform:scale(.97)}
button:disabled{opacity:.5;cursor:default}
.btn{background:var(--grad)}
.ghost{background:var(--bg4);color:var(--tx)}
.sq{padding:8px}
.danger{background:var(--bad)}
input,select,textarea{font:inherit;font-size:16px;padding:11px 13px;border:1px solid var(--line);border-radius:11px;background:var(--bg);color:var(--tx);outline:none;width:100%}
input:focus,select:focus,textarea:focus{border-color:var(--acc)}
textarea{resize:vertical;min-height:88px;line-height:1.5}
input[type=checkbox]{width:auto}
.svg{display:block;flex:none}.ico{display:inline-flex;align-items:center}
.muted{color:var(--tx2);font-size:13px;margin:4px 0}
.mo{display:none}
.row{display:flex;gap:8px;justify-content:flex-end;margin-top:6px}
::-webkit-scrollbar{width:8px;height:8px}::-webkit-scrollbar-thumb{background:var(--bg4);border-radius:8px}

/* ---- ログイン ---- */
#auth{min-height:100dvh;display:flex;align-items:center;justify-content:center;padding:20px;
 background:radial-gradient(900px 500px at 15% 10%,#3b46b8 0,transparent 60%),radial-gradient(800px 500px at 90% 90%,#b4398f 0,transparent 60%),var(--bg)}
#authbox{width:100%;max-width:360px;display:flex;flex-direction:column;gap:11px;background:color-mix(in srgb,var(--bg2) 88%,transparent);backdrop-filter:blur(14px);padding:28px;border-radius:22px;border:1px solid var(--line);box-shadow:0 20px 60px #0008}
#authbox .logo{width:84px;height:84px;border-radius:22px;align-self:center;box-shadow:0 8px 30px #6b7cff55}
#authbox h1{margin:0;text-align:center;font-size:26px;letter-spacing:.5px}
#authbox .muted{text-align:center;margin:-6px 0 4px}
#aerr{color:var(--bad);min-height:1.2em;font-size:13px}

/* ---- アプリ全体 ---- */
#app{display:flex;height:100vh;height:100dvh}
#nav{display:flex;flex:none}
#rail{width:76px;background:var(--bg);display:flex;flex-direction:column;align-items:center;gap:10px;padding:12px 0;overflow-y:auto;border-right:1px solid var(--line)}
.rb{width:50px;height:50px;border-radius:25px;background:var(--bg3);padding:0;overflow:hidden;position:relative;font-weight:700;color:var(--tx);font-size:15px;transition:border-radius .18s,background .18s}
.rb:hover{border-radius:16px;background:var(--bg4);filter:none}
.rb.active{border-radius:16px;background:var(--grad)}
.rb.active::before{content:"";position:absolute;left:-13px;top:14px;width:5px;height:22px;border-radius:0 4px 4px 0;background:#fff}
.rb img{width:100%;height:100%;object-fit:cover;display:block}
.rb.home{padding:0}.rb.green{color:var(--ok)}.rb.green:hover{background:var(--ok);color:#fff}
.sep{width:30px;height:2px;border-radius:2px;background:var(--line);flex:none}
#side{width:256px;background:var(--bg2);display:flex;flex-direction:column;border-right:1px solid var(--line)}
#sidehead{padding:14px 14px 12px;font-weight:700;display:flex;align-items:center;justify-content:space-between;gap:8px;border-bottom:1px solid var(--line);min-height:56px}
#sidehead .t{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
#list{flex:1;overflow-y:auto;padding:8px}
.lbl{font-size:11px;color:var(--tx2);margin:12px 8px 4px;letter-spacing:.08em}
.ch{padding:7px 9px;border-radius:10px;cursor:pointer;color:var(--tx2);display:flex;align-items:center;gap:9px;min-height:38px}
.ch:hover{background:var(--bg3);color:var(--tx)}.ch.active{background:var(--bg4);color:var(--hi)}.ch.unread{color:var(--hi);font-weight:700}
.ch .cn{flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.ch .at{width:18px;text-align:center;font-weight:700}
.vu{display:flex;align-items:center;gap:6px;font-size:12px;color:var(--tx2);padding:2px 0 2px 36px}
.dot{width:9px;height:9px;border-radius:50%;background:#5b5f78;flex:none;border:2px solid var(--bg2);box-sizing:content-box}.dot.on{background:var(--ok)}
.badge{background:var(--bad);color:#fff;border-radius:10px;padding:0 7px;font-size:12px;line-height:18px}
.xb{background:none;color:var(--tx2);padding:4px;opacity:.6}.xb:hover{opacity:1;background:var(--bg4)}
#meb{padding:10px 12px;background:var(--bg);display:flex;align-items:center;gap:10px;border-top:1px solid var(--line);padding-bottom:calc(10px + var(--sab))}
#meinfo{flex:1;display:flex;align-items:center;gap:9px;min-width:0;font-weight:600}
#meinfo span{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.av{width:38px;height:38px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-weight:700;color:#fff;flex:none;user-select:none;position:relative;overflow:hidden}
.av.xs{width:26px;height:26px;font-size:12px}.av.sm2{width:32px;height:32px;font-size:14px}.av.lg{width:64px;height:64px;font-size:26px}.av.xl{width:96px;height:96px;font-size:38px}
.av img{position:absolute;inset:0;width:100%;height:100%;object-fit:cover}.av.hasimg{color:transparent;background:var(--bg4)!important}

#main{flex:1;display:flex;flex-direction:column;min-width:0;background:var(--bg3)}
#head{padding:calc(10px + var(--sat)) 14px 10px;border-bottom:1px solid var(--line);display:flex;align-items:center;gap:10px;min-height:56px;background:var(--bg3)}
#headt{flex:1;font-weight:700;display:flex;align-items:center;gap:8px;overflow:hidden;white-space:nowrap;text-overflow:ellipsis}
#callbtn{background:var(--ok);color:#06210f;font-weight:700}#callbtn.live{background:var(--bad);color:#fff}
#nbar,#ring{padding:9px 14px;display:flex;align-items:center;gap:10px;font-size:14px}
#nbar{background:linear-gradient(90deg,#33397a,#6b2a68)}#ring{background:#15683f;flex-direction:column;align-items:stretch}
#ring div{display:flex;align-items:center;justify-content:space-between;gap:8px}
#nbar span{flex:1}
#call{background:var(--bg);border-bottom:1px solid var(--line);padding:10px 14px 12px;max-height:58vh;overflow-y:auto}
#calltitle{font-size:13px;color:var(--tx2);margin-bottom:8px;display:flex;align-items:center;gap:6px}
#tiles{display:grid;grid-template-columns:repeat(auto-fill,minmax(190px,1fr));gap:10px}
.tile{position:relative;aspect-ratio:16/10;background:var(--bg3);border-radius:14px;overflow:hidden;display:flex;align-items:center;justify-content:center;border:1px solid var(--line)}
.tile video{position:absolute;inset:0;width:100%;height:100%;object-fit:cover;display:none;background:#000}
.tile.cam video,.tile.screen video{display:block}.tile.cam .av{display:none}
.tile.screen{grid-column:1/-1;aspect-ratio:16/9;max-height:46vh;cursor:pointer}.tile.screen video{object-fit:contain}
.tile.mirror video{transform:scaleX(-1)}
.nm{position:absolute;left:8px;bottom:7px;font-size:12px;background:#000a;padding:3px 9px;border-radius:8px;display:flex;align-items:center;gap:5px}
.tile.off .nm{color:#ffb3bb}
#ctl{display:flex;gap:10px;justify-content:center;margin-top:12px;flex-wrap:wrap}
#ctl button{width:52px;height:52px;border-radius:26px;padding:0;background:var(--bg4)}
#ctl button.off{background:var(--bad)}#ctl button.live{background:var(--ok);color:#06210f}
#ctl button.wide{width:auto;padding:0 16px;gap:7px;font-size:13px}
#ctl .hang{background:var(--bad)}

#msgs{flex:1;overflow-y:auto;padding:12px 14px;scroll-behavior:smooth}
.empty{height:100%;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:12px;color:var(--tx2);text-align:center;padding:20px}
.empty img{width:96px;height:96px;border-radius:24px;box-shadow:0 10px 40px #6b7cff44}
.m{display:flex;gap:12px;padding:3px 8px;border-radius:12px;position:relative}
.m:not(.g){margin-top:12px}.m:hover{background:var(--hov)}
.m.mme{background:#f0b23218;box-shadow:inset 3px 0 0 #f0b232}
.m.flash{background:#6b7cff30}
.ms{width:40px;flex:none;display:flex;justify-content:center}
.gt{font-size:10px;color:transparent;padding-top:4px}.m:hover .gt{color:var(--tx2)}
.mb{min-width:0;flex:1}
.mh2{display:flex;align-items:baseline;gap:8px}.mh2 b{color:var(--hi)}.mh2 small{color:var(--tx2);font-size:11px}
.mt{white-space:pre-wrap;word-break:break-word;line-height:1.55}
.mt.jumbo .cemoji{width:48px;height:48px}
.ref{display:flex;align-items:center;gap:6px;font-size:13px;color:var(--tx2);cursor:pointer;margin-bottom:2px;overflow:hidden;white-space:nowrap}
.ref b{color:var(--tx)}.ref span{overflow:hidden;text-overflow:ellipsis}.ref:hover{color:var(--tx)}
.m video,.m img.pic{max-width:min(420px,100%);max-height:320px;display:block;margin-top:6px;border-radius:12px}
.m img.stamp{max-width:150px;max-height:150px;display:block;margin-top:4px}
.fileln{display:inline-flex;align-items:center;gap:8px;margin-top:6px;padding:8px 12px;background:var(--bg4);border-radius:10px;color:var(--acc);text-decoration:none}
.mention{background:#6b7cff33;color:var(--acc);border-radius:5px;padding:0 3px}.mention.me{background:var(--acc);color:#fff}
.cemoji{width:22px;height:22px;object-fit:contain;vertical-align:middle}
.uemoji{font-family:"Apple Color Emoji","Segoe UI Emoji","Noto Color Emoji",sans-serif}
.rx{display:flex;gap:5px;flex-wrap:wrap;margin-top:5px}.rx:empty{display:none}
.chip{background:var(--bg4);border:1px solid transparent;padding:2px 9px;font-size:13px;border-radius:12px;color:var(--tx)}
.chip.mine{border-color:var(--acc);background:#6b7cff2a}
.act{position:absolute;top:-14px;right:10px;display:none;gap:2px;background:var(--bg4);border:1px solid var(--line);border-radius:11px;padding:2px}
.act button{background:none;padding:6px;color:var(--tx2)}.act button:hover{color:var(--hi);background:var(--bg3)}
.m:hover .act{display:flex}
#prog{font-size:12px;padding:0 16px;height:14px;color:var(--tx2)}
#replybar{margin:0 12px;padding:8px 12px;background:var(--bg4);border-radius:12px 12px 0 0;display:flex;align-items:center;gap:8px;font-size:13px;color:var(--tx2)}
#replybar span{flex:1}
#stampbox{display:flex;flex-wrap:wrap;gap:8px;padding:10px;margin:0 12px;background:var(--bg2);border:1px solid var(--line);border-radius:14px;max-height:210px;overflow-y:auto}
.st{position:relative}.st img{width:68px;height:68px;object-fit:contain;cursor:pointer;border-radius:10px}.st img:hover{background:var(--bg4)}
.st button{position:absolute;top:-4px;right:-4px;padding:3px;border-radius:50%;background:var(--bad)}
#bar{display:flex;align-items:center;gap:8px;padding:10px 12px calc(12px + var(--sab));background:var(--bg3)}
#bar .field{flex:1;display:flex;align-items:center;background:var(--bg4);border-radius:16px;padding:0 4px}
#txt{background:transparent;border:0;padding:12px 10px}
#bar .ghost{background:transparent;color:var(--tx2)}#bar .ghost:hover{color:var(--hi);background:var(--hov2)}
#bsend{border-radius:14px;padding:11px 16px}
#members{width:236px;background:var(--bg2);padding:14px;overflow-y:auto;border-left:1px solid var(--line);flex:none}
#members h4{margin:0 0 8px;font-size:12px;color:var(--tx2);letter-spacing:.08em}
.mem{padding:6px;border-radius:10px;cursor:pointer;display:flex;align-items:center;gap:10px}.mem:hover{background:var(--bg3)}
.mem .mi{min-width:0;display:flex;flex-direction:column}.mem small{color:var(--tx2);font-size:11px}
.tag{font-size:10px;background:var(--grad);padding:1px 6px;border-radius:6px;margin-left:6px}
.pf{display:flex;flex-direction:column;align-items:center;gap:8px;position:relative;isolation:isolate}.pf.hasbg{padding-bottom:12px;border-radius:14px}
.pfbn{height:96px;width:100%;border-radius:14px;background:var(--grad);position:relative;overflow:hidden}
.pfbg{position:absolute;inset:0;z-index:-1;border-radius:14px;overflow:hidden;background:var(--bg3)}
.pfm{position:absolute;inset:0;width:100%;height:100%;object-fit:cover;pointer-events:none}
.mpv{position:relative;height:90px;border-radius:12px;overflow:hidden;background:var(--grad);display:flex;align-items:center;justify-content:center;color:#fff;font-size:13px}
.mpv.bgp{background:var(--bg3);color:var(--tx2)}
input[type=range]{padding:0;height:28px;accent-color:var(--acc)}
.pf .av{margin-top:-52px;border:5px solid var(--bg2);box-sizing:content-box}.pfn{font-size:21px;font-weight:700}
.pfb{white-space:pre-wrap;word-break:break-word;background:var(--bg);border:1px solid var(--line);border-radius:12px;padding:12px;width:100%;min-height:64px;line-height:1.6}
.code{background:var(--bg);padding:10px;border-radius:10px;word-break:break-all;margin:8px 0;user-select:all;font-family:ui-monospace,Consolas,monospace;font-size:13px;border:1px dashed var(--line)}

/* ---- モーダル / トースト / 絵文字ピッカー ---- */
#modal{position:fixed;inset:0;background:#000a;backdrop-filter:blur(4px);display:flex;align-items:center;justify-content:center;z-index:100;padding:16px}
#mbox{width:100%;max-width:420px;max-height:88vh;overflow-y:auto;background:var(--bg2);border:1px solid var(--line);border-radius:20px;padding:18px;display:flex;flex-direction:column;gap:12px;box-shadow:0 30px 80px #000a}
.mh{display:flex;align-items:center;justify-content:space-between;font-size:17px}
.fld{display:flex;flex-direction:column;gap:6px}.fld label{font-size:13px;color:var(--tx2)}
.chk{display:flex!important;align-items:center;gap:9px;color:var(--tx)!important;font-size:14px!important}
.tabs{display:flex;gap:6px;background:var(--bg);padding:4px;border-radius:12px}.tabs button{flex:1;background:transparent;color:var(--tx2)}.tabs button.on{background:var(--bg4);color:var(--hi)}
.mi{justify-content:flex-start;background:var(--bg3);color:var(--tx);width:100%;padding:12px 14px}
.drow{display:flex;align-items:center;justify-content:space-between;gap:10px;padding:10px;background:var(--bg3);border-radius:12px}
.drow small{display:block;color:var(--tx2)}.dlist{display:flex;flex-direction:column;gap:8px;max-height:46vh;overflow-y:auto}
.pv{max-width:120px;max-height:120px;align-self:center;border-radius:12px;background:var(--bg)}
#toast{position:fixed;left:50%;bottom:calc(86px + var(--sab));transform:translateX(-50%);background:#000d;color:#fff;padding:11px 18px;border-radius:14px;z-index:200;max-width:90vw;text-align:center;border:1px solid var(--line)}
.pk{position:fixed;z-index:150;width:330px;background:var(--bg2);border:1px solid var(--line);border-radius:16px;box-shadow:0 20px 60px #000a;display:flex;flex-direction:column}
.pktabs{display:flex;gap:2px;padding:8px 8px 0;overflow-x:auto}.pktabs button{background:transparent;color:var(--tx2);padding:6px 10px;font-size:12px;border-radius:9px;white-space:nowrap}.pktabs button.on{background:var(--bg4);color:var(--hi)}
.pkgrid{display:flex;flex-wrap:wrap;gap:2px;padding:8px;height:230px;overflow-y:auto;align-content:flex-start}
.pe{background:transparent;padding:4px;font-size:24px;width:40px;height:40px;border-radius:10px}.pe:hover{background:var(--bg4)}
.pe .cemoji{width:28px;height:28px}
.pw{position:relative}.pex{position:absolute;top:-2px;right:-2px;background:var(--bad);border-radius:50%;padding:2px;cursor:pointer;display:flex}

/* ---- スマホ ---- */
@media(max-width:820px){
 .mo{display:inline-flex}
 #nav{position:fixed;left:0;top:0;bottom:0;z-index:60;transform:translateX(-102%);transition:transform .22s ease;box-shadow:8px 0 40px #000a}
 #nav.open{transform:none}
 #side{width:min(76vw,290px)}
 #rail{padding-top:calc(12px + var(--sat))}
 #members{position:fixed;right:0;top:0;bottom:0;z-index:60;width:min(80vw,300px);transform:translateX(102%);transition:transform .22s ease;padding-top:calc(14px + var(--sat));box-shadow:-8px 0 40px #000a}
 #members.open{transform:none}
 #scrim{position:fixed;inset:0;background:#0009;z-index:55}
 #msgs{padding:8px 6px}
 .m{gap:9px;padding:3px 4px}.ms{width:36px}.av{width:36px;height:36px}
 .act{display:flex;top:-12px;right:4px}
 #bsend .lbl,#callbtn .lbl{display:none}
 #bar{gap:4px;padding:8px 8px calc(10px + var(--sab))}
 #tiles{grid-template-columns:repeat(2,1fr);gap:8px}
 .tile.screen{max-height:36vh}
 #call{max-height:52vh;padding:8px 10px 10px}
 #ctl button{width:48px;height:48px}
 .pk{left:8px!important;right:8px!important;width:auto!important;top:auto!important;bottom:calc(66px + var(--sab))!important}
 .pkgrid{height:200px}
 #mbox{max-height:92vh;border-radius:20px 20px 16px 16px}
 #stampbox{margin:0 8px}#replybar{margin:0 8px}
}

/* ---- ステータス丸 / ホーム画面 / 公式アナウンス ---- */
.avw{position:relative;flex:none;display:inline-flex}
.sdot{position:absolute;right:-2px;bottom:-2px;width:13px;height:13px;border-radius:50%;background:#9aa0b4;border:2.5px solid var(--bg2);box-sizing:border-box}
.sdot.on{background:var(--ok);box-shadow:0 0 8px var(--ok)}
.addbar{width:100%;background:transparent;border:2px solid var(--acc);color:var(--acc);border-radius:16px;padding:12px;font-weight:700;margin-bottom:10px;box-shadow:0 0 16px color-mix(in srgb,var(--acc) 28%,transparent)}
.addbar:hover{background:color-mix(in srgb,var(--acc) 12%,transparent);filter:none}
#annb{position:relative;color:#ff9500}#annb.active{background:var(--bg4)}#annb .badge{position:absolute;top:-5px;right:-5px;font-size:10px;line-height:15px;padding:0 5px}#annb .udot{position:absolute;top:3px;right:3px;width:9px;height:9px;border-radius:50%;background:var(--bad);border:2px solid var(--bg)}
#lb{position:fixed;inset:0;z-index:300;background:#000e;display:flex;align-items:center;justify-content:center;touch-action:none;overflow:hidden}
#lb img,#lb video{max-width:100vw;max-height:100vh;transform-origin:center;user-select:none;-webkit-user-drag:none;touch-action:none}
#lb img{cursor:grab}
#lbbar{position:absolute;top:calc(10px + var(--sat));right:10px;display:flex;gap:8px;z-index:2}
#lbbar button{background:#000b;color:#fff;border:1px solid #fff3}
.m img.pic{cursor:zoom-in}
.fa{display:flex;gap:6px;margin-top:4px}.fa button{background:var(--bg4);color:var(--tx2);padding:5px 10px;font-size:12px;border-radius:9px}.fa button:hover{color:var(--hi)}
.annc{display:flex;align-items:center;gap:12px;padding:10px;border-radius:16px;background:color-mix(in srgb,#ff9500 14%,var(--bg2));border:1.5px solid #ff950099;cursor:pointer;font-weight:800;margin-bottom:10px}
.annc.active{box-shadow:0 0 0 2px #ff9500}.annc .annt{flex:1}
.annic{width:42px;height:42px;border-radius:12px;background:#ff9500;color:#fff;display:flex;align-items:center;justify-content:center;flex:none}
.seg{display:flex;background:var(--bg4);border-radius:15px;padding:4px;margin-bottom:8px}
.seg button{flex:1;background:transparent;color:var(--tx2);font-weight:800;border-radius:12px;padding:10px}
.seg button.on{background:var(--bg2);color:var(--acc);box-shadow:0 2px 8px var(--shadow)}
.frow{display:flex;align-items:center;gap:13px;padding:8px;border-radius:14px;cursor:pointer}
.frow:hover{background:var(--hov)}.frow .fn{flex:1;font-size:16px;font-weight:700;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.frow.off .fn{opacity:.6}.frow.unread .fn{text-decoration:underline}
.mem.off .mi span{opacity:.6}
.edited{font-size:10px;color:var(--tx2);margin-left:6px}
.pvc{display:flex;justify-content:center;padding:4px 0}
.swatches{display:flex;gap:8px;flex-wrap:wrap}.swt{width:34px;height:34px;border-radius:50%;padding:0;border:2px solid var(--line)}
input[type=color]{padding:3px;height:44px}
/* ---- ポップな文字 (写真のスタイル) ---- */
body.pop{font-family:"M PLUS Rounded 1c","Hiragino Maru Gothic ProN","Yu Gothic UI","Meiryo",system-ui,sans-serif}
body.pop .fn,body.pop .cn,body.pop .mh2 b,body.pop .pfn,body.pop #meinfo span,body.pop .mem .mi>span,body.pop .annt,body.pop #sidehead .t,body.pop #headt{font-weight:800}
body.pop .fn,body.pop .mh2 b,body.pop .pfn,body.pop #meinfo span,body.pop .mem .mi>span{color:var(--acc);text-shadow:0 0 12px color-mix(in srgb,var(--acc) 40%,transparent)}
body.pop .annt{color:#ff9500}
/* ---- 通話の拡大表示 ---- */
#call.big{position:fixed;inset:0;z-index:70;max-height:none;display:flex;flex-direction:column;padding:calc(12px + var(--sat)) 16px calc(16px + var(--sab));background:var(--bg)}
#call.big #tiles{flex:1;min-height:0;grid-template-columns:repeat(auto-fit,minmax(min(100%,320px),1fr));grid-auto-rows:minmax(0,1fr)}
#call.big .tile{aspect-ratio:auto;min-height:0}
#call.big .tile.screen{max-height:none;grid-column:1/-1;aspect-ratio:auto}
#call.big #ctl{padding-top:12px}
@media(max-width:820px){#call.big #tiles{grid-template-columns:repeat(auto-fit,minmax(min(100%,150px),1fr))}}

/* ---- 通話画面 (黒背景・丸いアイコン・ピル型のコントロール) ---- */
#call{background:#000;border-bottom:1px solid #ffffff14;padding:12px 14px 14px;max-height:56vh}
#calltitle{color:#8b8fa6}
#tiles{display:flex;flex-wrap:wrap;justify-content:center;align-items:center;gap:14px;min-height:130px;padding:6px 0}
.tile{width:150px;height:130px;aspect-ratio:auto;background:transparent;border:0;border-radius:16px}
.tile.cam{width:240px;height:150px;background:#101116}
.tile.screen{width:min(100%,680px);height:auto;aspect-ratio:16/9;max-height:42vh;background:#101116}
.tile:not(.cam):not(.screen) .nm{left:50%;transform:translateX(-50%);bottom:2px;background:transparent;font-size:13px;white-space:nowrap}
.av.cav{width:84px;height:84px;font-size:34px;margin-bottom:16px}
#ctl{gap:12px;margin-top:10px;align-items:center}
.cg{display:flex;align-items:center;background:#16171c;border:1px solid #ffffff14;border-radius:14px;padding:4px;gap:2px}
.cg:not(:has(button:not([hidden]))){display:none}
#ctl .cg button{width:44px;height:40px;border-radius:10px;padding:0;background:transparent;color:#fff}
#ctl .cg button:hover{background:#ffffff18;filter:none}
#ctl .cg button.caret{width:24px;color:#9a9eb6}
#ctl .cg button.off{color:#ff7683;background:#ff5a6a24}
#ctl .cg button.live{color:#2fd27a;background:#2fd27a22}
#ctl #hangb{width:64px;height:48px;border-radius:12px;background:#d63040;color:#fff}
#call.big{background:#000}
#call.big #tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,320px),1fr));grid-auto-rows:minmax(0,1fr);flex:1;min-height:0;gap:12px}
#call.big .tile,#call.big .tile.cam{width:auto;height:auto;min-height:0}
#call.big .tile.cam,#call.big .tile.screen{background:#101116}
#call.big .tile.screen{grid-column:1/-1;max-height:none;aspect-ratio:auto}
#call.big .av.cav{width:128px;height:128px;font-size:52px;margin-bottom:22px}
#call.big .tile:not(.cam):not(.screen) .nm{font-size:15px;bottom:14px}
@media(max-width:820px){.tile{width:120px;height:116px}.tile.cam{width:46vw;height:130px}.av.cav{width:68px;height:68px;font-size:28px}#call.big .av.cav{width:96px;height:96px;font-size:40px}}

/* ---- ヘッダー ---- */
#head{background:var(--bg);border-bottom:1px solid var(--line)}
#headt{font-size:15px;gap:10px}
#head .ghost{background:transparent;color:var(--tx2)}#head .ghost:hover{color:var(--hi);background:var(--hov2)}
#srch{position:relative;display:flex;align-items:center}
#srch input{width:210px;padding:8px 38px 8px 12px;border-radius:10px;background:var(--bg3);font-size:14px}
#srch #sqb{position:absolute;right:2px;padding:6px}
@media(max-width:820px){#srch input{display:none}#srch #sqb{position:static}}

/* ======== v5: 上部ヨコバー + かっこいいUI ======== */
:root{--glow:color-mix(in srgb,var(--acc) 45%,transparent);--glass:color-mix(in srgb,var(--bg) 78%,transparent)}
#rail{position:relative;flex-direction:row;width:auto;height:68px;padding:0 16px;gap:12px;align-items:center;justify-content:flex-start;
 overflow-x:auto;overflow-y:hidden;border-right:0;border-bottom:1px solid var(--line);
 background:linear-gradient(180deg,color-mix(in srgb,var(--acc) 9%,var(--bg)),var(--bg));backdrop-filter:blur(16px);scrollbar-width:none}
#rail::-webkit-scrollbar{display:none}
#rail::after{content:"";position:absolute;left:0;right:0;bottom:-1px;height:1px;background:var(--grad);opacity:.55;pointer-events:none}
.rb{flex:none;width:46px;height:46px;border-radius:15px;background:var(--bg3);border:1px solid var(--line);
 box-shadow:0 2px 10px #0004;transition:transform .18s cubic-bezier(.3,1.5,.5,1),border-radius .18s,box-shadow .18s,background .18s}
.rb:hover{transform:translateY(-3px) scale(1.06);border-radius:13px;background:var(--bg4);box-shadow:0 8px 20px var(--glow)}
.rb.active{border-radius:13px;background:var(--grad);border-color:transparent;box-shadow:0 6px 22px var(--glow)}
.rb.active::before{left:50%;top:auto;bottom:-11px;width:22px;height:4px;transform:translateX(-50%);border-radius:4px 4px 0 0;background:var(--acc);box-shadow:0 -2px 10px var(--glow)}
.rb.home{border-radius:15px}
.rb.green{border-style:dashed;border-color:color-mix(in srgb,var(--ok) 55%,transparent);background:transparent}
.rb.green:hover{background:var(--ok);border-style:solid;box-shadow:0 8px 20px color-mix(in srgb,var(--ok) 50%,transparent)}
.sep{width:2px;height:28px;border-radius:2px;background:linear-gradient(180deg,transparent,var(--line),transparent)}

/* 左のリスト・ヘッダー・メッセージ欄を少しリッチに */
#side{background:linear-gradient(180deg,var(--bg2),color-mix(in srgb,var(--bg2) 92%,var(--acc)))}
#sidehead{font-size:15px;letter-spacing:.03em;min-height:52px}
.ch{border-radius:12px;transition:background .15s,color .15s,transform .12s}
.ch:hover{transform:translateX(2px)}
.ch.active{background:linear-gradient(90deg,color-mix(in srgb,var(--acc) 26%,transparent),transparent);box-shadow:inset 3px 0 0 var(--acc)}
#head{backdrop-filter:blur(14px);background:var(--glass);min-height:52px}
#meb{background:color-mix(in srgb,var(--bg) 85%,var(--acc))}
.av{box-shadow:0 2px 8px #0005}
button.btn,.btn{box-shadow:0 4px 16px var(--glow)}
input:focus,select:focus,textarea:focus{box-shadow:0 0 0 3px color-mix(in srgb,var(--acc) 28%,transparent)}
.addbar{border-radius:14px}

/* PC: 横バーを最上段に、その下に サイド | メイン | メンバー */
@media(min-width:821px){
 #app{display:grid;grid-template-columns:256px minmax(0,1fr) auto;grid-template-rows:68px minmax(0,1fr)}
 #nav{display:contents}
 #rail{grid-column:1/-1;grid-row:1}
 #side{grid-column:1;grid-row:2;min-height:0;width:auto}
 #main{grid-column:2;grid-row:2;min-height:0}
 #members{grid-column:3;grid-row:2;min-height:0}
}

/* スマホ: ドロワーの一番上に横スクロールのバー */
@media(max-width:820px){
 #nav{flex-direction:column;width:min(84vw,320px)}
 #rail{width:100%;height:calc(68px + var(--sat));padding:var(--sat) 12px 0;flex:none}
 #side{width:100%;flex:1;min-height:0}
}

/* ======== v5.1: 発言リング / グループ / 設定画面 / アイコン切り抜き ======== */
.av{transition:box-shadow .15s}
.speaking>.av,.speaking>.avw>.av,.tile.speaking .av,.vu.speaking .av,.mem.speaking .av{box-shadow:0 0 0 3px var(--bg3),0 0 0 6px var(--ok),0 0 16px var(--ok)}
.vu.speaking .av,.mem.speaking .av{box-shadow:0 0 0 2px var(--bg2),0 0 0 4px var(--ok)}
.tile.speaking{border-color:var(--ok);box-shadow:0 0 0 2px var(--ok)}
.gav{background:var(--grad);color:#fff}
.addbar.sm{width:auto;flex:1;padding:6px 8px;font-size:12px;border-width:1.5px;border-radius:11px;margin-bottom:0;gap:5px;box-shadow:none;white-space:nowrap}
.homeact{display:flex;gap:6px;margin-bottom:10px}
.fpick{display:flex;flex-direction:column;gap:2px;max-height:260px;overflow-y:auto}
.fprow{display:flex;align-items:center;gap:10px;padding:8px 10px;border-radius:12px;cursor:pointer;color:var(--tx)!important;font-size:14px!important}
.fprow:hover{background:var(--hov)}
.m .mt,.mt{font-size:var(--mfs,15px)}
body.compact .m:not(.g){margin-top:3px}body.compact .m{padding:1px 8px}
body.reduce *{animation:none!important;transition:none!important}
body.hc{--tx2:#d5d8ea}:root[data-theme=light] body.hc{--tx2:#2b2e44}
body.big button{min-height:44px}

.crop{position:fixed;inset:0;z-index:250;background:#000c;backdrop-filter:blur(6px);display:flex;align-items:center;justify-content:center;padding:16px}
.cropbox{background:var(--bg2);border:1px solid var(--line);border-radius:20px;padding:18px;display:flex;flex-direction:column;align-items:center;gap:12px;max-width:100%;box-shadow:0 30px 80px #000a}
.cropstage{position:relative;border-radius:16px;overflow:hidden;touch-action:none;cursor:grab;background:#000}
.cropstage canvas{display:block;width:100%;height:100%}
.cropmask{position:absolute;inset:8%;border-radius:50%;box-shadow:0 0 0 999px #000a;border:2px solid #fff;pointer-events:none}
.cropbox input[type=range]{width:100%;padding:0}

.set{position:fixed;inset:0;z-index:120;background:var(--bg);display:flex}
.setnav{width:max(250px,28vw);flex:none;background:var(--bg2);border-right:1px solid var(--line);padding:calc(24px + var(--sat)) 12px 16px;display:flex;flex-direction:column;gap:3px;overflow-y:auto;align-items:stretch}
.setttl{font-size:12px;letter-spacing:.1em;color:var(--tx2);padding:4px 12px 10px}
.sitem{background:transparent;color:var(--tx);justify-content:flex-start;padding:10px 12px;border-radius:10px;font-weight:600;gap:11px}
.sitem:hover{background:var(--hov2);filter:none}
.sitem.on{background:color-mix(in srgb,var(--acc) 22%,transparent);color:var(--hi);box-shadow:inset 3px 0 0 var(--acc)}
.sitem.dng{color:var(--bad)}
.ssp{flex:1;min-height:12px}
.setbody{flex:1;min-width:0;overflow-y:auto;padding:0 28px 60px;position:relative}
.settop{position:sticky;top:0;z-index:2;display:flex;align-items:center;gap:10px;padding:calc(18px + var(--sat)) 0 12px;background:var(--bg);border-bottom:1px solid var(--line);margin-bottom:6px}
.setct{flex:1;font-size:20px}
.setin{max-width:680px}
.srow{display:flex;align-items:center;justify-content:space-between;gap:16px;padding:14px 0;border-bottom:1px solid var(--line);flex-wrap:wrap}
.srow .st{display:flex;flex-direction:column;gap:3px;min-width:0;flex:1 1 220px}
.srow small{color:var(--tx2);font-size:12.5px;line-height:1.4;word-break:break-all}
.srow select{width:auto;max-width:100%;flex:0 1 260px}
.srange{display:flex;align-items:center;gap:10px;flex:0 1 240px}.srange input{padding:0}.srange .muted{min-width:42px;margin:0;text-align:right}
.sh3{font-size:12px;letter-spacing:.08em;color:var(--tx2);margin:22px 0 2px}
.snote{margin:14px 0;padding:12px 14px;border-radius:12px;background:var(--bg3);border:1px solid var(--line);color:var(--tx2);font-size:13.5px;line-height:1.6}
.snote.ok{border-color:var(--ok);color:var(--ok)}.snote.warn{border-color:#f59e0b;color:#f59e0b}
.pcard{display:flex;align-items:center;gap:14px;padding:16px;border-radius:16px;background:linear-gradient(135deg,color-mix(in srgb,var(--acc) 22%,var(--bg3)),var(--bg3));border:1px solid var(--line);margin:10px 0}
.pcard .pci{display:flex;flex-direction:column;gap:3px;min-width:0}.pcard b{font-size:18px}.pcard small{color:var(--tx2)}
.sw{position:relative;display:inline-block;width:46px;height:26px;flex:none;cursor:pointer}
.sw input{position:absolute;opacity:0;inset:0;width:100%;height:100%;margin:0;cursor:pointer}
.sw .knob{position:absolute;inset:0;border-radius:26px;background:var(--bg4);transition:background .2s}
.sw .knob::after{content:"";position:absolute;left:3px;top:3px;width:20px;height:20px;border-radius:50%;background:#fff;transition:transform .2s;box-shadow:0 1px 4px #0005}
.sw input:checked+.knob{background:var(--ok)}.sw input:checked+.knob::after{transform:translateX(20px)}
.swatches input[type=color]{width:34px;height:34px;padding:0;border-radius:50%;border:2px solid var(--line)}
.setx,.setback{margin-left:auto}
@media(max-width:820px){
 .setnav{width:100%;border-right:0}
 .setbody{padding:0 16px 60px}
 .set[data-view=list] .setbody{display:none}
 .set[data-view=detail] .setnav{display:none}
 .setback{margin-left:0}
 .srow .st{flex-basis:100%}.srow select,.srange{flex:1 1 100%}
 .setnav .setttl{font-size:18px;color:var(--tx);letter-spacing:0;padding-right:48px}
 .setx{position:absolute;right:12px;top:calc(14px + var(--sat))}
 .setnav{position:relative}
}
@media(min-width:821px){.setback,.setx{display:none!important}}

/* 入力中 / ボイスメッセージ */
#typing{padding:2px 18px 4px;min-height:22px;font-size:13px;color:var(--tx2);display:flex;align-items:center;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#typing b{font-weight:800;color:var(--tx2)}#typing .tyt{color:var(--acc);font-weight:700}
#bvoice.rec{background:var(--bad)!important;color:#fff!important;animation:recpulse 1.2s infinite}
@keyframes recpulse{50%{box-shadow:0 0 0 6px color-mix(in srgb,var(--bad) 30%,transparent)}}
.vmsg audio{width:min(320px,100%);height:40px;display:block;margin-top:6px}
#bpaste{display:none}
@media(pointer:coarse),(max-width:820px){#bpaste{display:inline-flex}}
.pastebox{display:flex;flex-direction:column;align-items:center;gap:10px}

/* ---- ブロック / ミュート / フレンドメニュー (スマホ対応) ---- */
.ch .xb{flex:none;min-width:34px;min-height:34px;display:inline-flex;align-items:center;justify-content:center;border-radius:10px}
.ch .tg{flex:none;font-size:11px;line-height:1;white-space:nowrap;color:var(--tx2);background:var(--bg4);padding:3px 7px;border-radius:8px}
.ch .tg.bad{color:var(--bad)}
.srow .rm{min-height:38px}
@media(pointer:coarse),(max-width:820px){
 .ch{min-height:50px}
 .ch .xb{opacity:1;min-width:42px;min-height:42px}
 .mi{padding:15px 16px;font-size:15px;gap:12px}
 #modal.sheet{align-items:flex-end;padding:0}
 #modal.sheet #mbox{max-width:none;max-height:86vh;border-radius:22px 22px 0 0;padding:16px 14px calc(16px + var(--sab));animation:sheetUp .2s ease-out}
 .srow .rm{width:100%;min-height:46px;justify-content:center}
}
@keyframes sheetUp{from{transform:translateY(40px);opacity:0}to{transform:none;opacity:1}}

/* ---- 一番下へボタン / リンク / 埋め込み / ボイスメッセージ / 更新通知 ---- */
#bar{position:relative}
.bdown{position:absolute;right:14px;bottom:calc(100% + 10px);width:42px;height:42px;padding:0;border-radius:50%;border:1px solid var(--line);background:var(--bg4);color:var(--tx);display:grid;place-items:center;cursor:pointer;box-shadow:0 6px 18px #0006;z-index:5}
.bdown:hover{background:var(--hov2)}
.bdown.new{border-color:var(--acc);color:var(--acc)}
.bdown .bdn{position:absolute;top:-7px;right:-4px;min-width:20px;height:20px;padding:0 5px;border-radius:10px;background:var(--bad);color:#fff;font-size:11px;font-weight:800;display:grid;place-items:center}
.mt a.lnk{color:var(--acc);text-decoration:underline;text-underline-offset:2px;word-break:break-all}
.mt a.lnk:hover{filter:brightness(1.2)}
.lpc{display:block;max-width:min(420px,100%);margin-top:8px;padding:10px 12px;background:var(--bg4);border:1px solid var(--line);border-left:4px solid var(--acc);border-radius:12px;color:var(--tx);text-decoration:none;overflow:hidden}
.lpc:hover{background:var(--hov2)}
.lps{font-size:12px;color:var(--tx2);margin-bottom:2px}
.lph{font-weight:800;color:var(--acc);line-height:1.35;word-break:break-word;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.lpd{font-size:13px;color:var(--tx2);margin-top:4px;line-height:1.45;word-break:break-word;display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;overflow:hidden}
.lpc img.lpi{display:block;width:100%;max-height:220px;object-fit:cover;border-radius:8px;margin-top:8px;background:var(--bg3)}
.lpc.lpimg{padding:0;border:0;background:none}
.lpc.lpimg img{display:block;max-width:100%;max-height:320px;border-radius:12px}
.vp{display:flex;align-items:center;gap:12px;margin-top:6px;padding:10px 14px 10px 10px;width:min(340px,100%);background:var(--bg4);border:1px solid var(--line);border-radius:18px;transition:border-color .15s}
.vp.play{border-color:var(--acc)}
.vpb{flex:none;width:44px;height:44px;padding:0;border:0;border-radius:50%;display:grid;place-items:center;background:var(--grad);color:#fff;cursor:pointer;box-shadow:0 4px 14px #0004}
.vpb .svg{fill:currentColor;stroke-width:3}
.vcol{flex:1;min-width:0;display:flex;flex-direction:column;gap:5px}
.vwave{display:flex;align-items:center;gap:2px;height:30px;cursor:pointer;touch-action:none}
.vwave i{flex:1;min-width:2px;max-width:6px;border-radius:3px;background:var(--tx2);opacity:.38;transition:opacity .12s,background .12s}
.vwave i.on{background:var(--acc);opacity:1}
.vmeta{display:flex;align-items:center;gap:8px}
.vt{flex:1;font-size:12px;font-variant-numeric:tabular-nums;color:var(--tx2)}
.vsp{flex:none;border:0;border-radius:999px;background:var(--hov2);color:var(--tx2);font-size:11px;font-weight:800;padding:4px 9px;cursor:pointer}
.vdl{flex:none;width:28px;height:28px;padding:0;border:0;border-radius:50%;background:transparent;color:var(--tx2);display:grid;place-items:center;cursor:pointer}
.vdl:hover{background:var(--hov2);color:var(--hi)}
#upd{position:fixed;left:50%;top:calc(10px + var(--sat));transform:translateX(-50%);z-index:220;display:flex;align-items:center;gap:10px;max-width:92vw;padding:9px 10px 9px 18px;border-radius:999px;background:var(--grad);color:#fff;font-weight:700;font-size:14px;box-shadow:0 8px 30px #0007}
#upd button{border:0;cursor:pointer;border-radius:999px;font-weight:800}
#upd .go{background:#fff;color:#222;padding:7px 15px}
#upd .x{background:#fff3;color:#fff;width:30px;height:30px;padding:0;display:grid;place-items:center}
@media(max-width:820px){.bdown{right:10px}}
.setver{margin:10px 12px 0;font-size:12px;color:var(--tx2);text-align:center}
</style>

<div id=auth>
 <div id=authbox>
  <img class=logo src=/icon-192.png alt="">
  <h1>Motto</h1>
  <p class=muted>みんなともっと楽しい時間を</p>
  <input id=an placeholder="ユーザー名" autocomplete=username>
  <input id=ap type=password placeholder="パスワード (6文字以上)" autocomplete=current-password>
  <div id=aerr></div>
  <button class=btn onclick="doAuth('login')">ログイン</button>
  <button class=ghost onclick="doAuth('register')">新規登録</button>
 </div>
</div>

<div id=app hidden>
 <div id=nav>
  <div id=rail></div>
  <div id=side>
   <div id=sidehead></div><div id=list></div>
   <div id=meb><div id=meinfo></div><button id=annb class="ghost sq" onclick="openAnnounce()"></button><button id=gearb class="ghost sq" onclick="settingsModal()"></button></div>
  </div>
 </div>
 <div id=scrim hidden></div>
 <div id=main>
  <div id=head>
   <button id=navbtn class="ghost sq mo" onclick="openNav()"></button>
   <div id=headt></div>
   <button id=callbtn onclick="callBtn()" hidden></button>
   <button id=profbtn class="ghost sq" onclick="headProfile()" hidden></button>
   <div id=srch><input id=sq type=search autocomplete=off><button id=sqb class="ghost sq" onclick="doSearch(true)"></button></div>
   <button id=membtn class="ghost sq mo" onclick="openMem()"></button>
  </div>
  <div id=nbar hidden></div>
  <div id=ring hidden></div>
  <div id=call hidden>
   <div id=calltitle></div><div id=tiles></div>
   <div id=ctl>
    <div class=cg>
     <button id=micb onclick="toggleMic()"></button><button id=micc class=caret onclick="deviceMenu('audioinput')"></button>
     <button id=camb onclick="toggleCam()"></button><button id=camc class=caret onclick="deviceMenu('videoinput')"></button>
    </div>
    <div class=cg>
     <button id=scrb onclick="toggleScreen()" hidden></button>
     <button id=flipb onclick="switchCam()" hidden></button>
     <button id=bigb onclick="toggleBig()"></button>
    </div>
    <button id=hangb class=hang onclick="leaveCall()"></button>
   </div>
  </div>
  <div id=msgs></div><div id=prog></div><div id=typing></div>
  <div id=replybar hidden></div>
  <div id=stampbox hidden></div>
  <div id=ronote class=muted hidden style="text-align:center;padding:14px">公式アナウンスはオーナーだけが投稿できます</div>
  <div id=bar>
   <button id=bdown class=bdown onclick="toBottom(true)" hidden></button>
   <button id=bclip class=ghost onclick="f.click()"></button>
   <button id=bstamp class=ghost onclick="toggleStamps()"></button>
   <div class=field>
    <input id=txt placeholder="メッセージを送信" autocomplete=off>
    <button id=bemoji class=ghost></button>
   </div>
   <button id=bsend class=btn onclick="sendMsg()"></button>
  </div>
 </div>
 <div id=members></div>
</div>
<div id=modal hidden><div id=mbox></div></div>
<div id=toast hidden></div>
<input type=file id=f hidden>
<input type=file id=imgf accept="image/png,image/jpeg,image/gif,image/webp" hidden>

<script>
const $=id=>document.getElementById(id);
const el=(t,c,h)=>{const e=document.createElement(t);if(c)e.className=c;if(h!=null)e.innerHTML=h;return e};

/* ---------- アイコン (絵文字は使わずSVGで描画) ---------- */
const IC={
 plus:'<path d="M12 5v14M5 12h14"/>',
 compass:'<circle cx="12" cy="12" r="9"/><path d="M15.5 8.5l-2 5-5 2 2-5z"/>',
 hash:'<path d="M5 9h14M5 15h14M10 4L8 20M16 4l-2 16"/>',
 speaker:'<path d="M4 9v6h4l5 4V5L8 9z"/><path d="M16 9a4 4 0 0 1 0 6M18.5 6.5a8 8 0 0 1 0 11"/>',
 mic:'<rect x="9" y="3" width="6" height="11" rx="3"/><path d="M5 11a7 7 0 0 0 14 0M12 18v3"/>',
 micoff:'<path d="M9 9v2a3 3 0 0 0 5 2.2M15 9.5V6a3 3 0 0 0-6-.9"/><path d="M5 11a7 7 0 0 0 11 5.7M19 11a7 7 0 0 1-.6 2.8M12 18v3"/><path d="M4 4l16 16"/>',
 video:'<rect x="3" y="6" width="13" height="12" rx="2"/><path d="M16 10l5-3v10l-5-3z"/>',
 videooff:'<path d="M16 10l5-3v10l-5-3z"/><path d="M13 6H5a2 2 0 0 0-2 2v8a2 2 0 0 0 2 2h9a2 2 0 0 0 2-2v-1"/><path d="M4 4l16 16"/>',
 phone:'<path d="M5 4h4l2 5-2.5 1.5a11 11 0 0 0 5 5L15 13l5 2v4a2 2 0 0 1-2 2A16 16 0 0 1 3 6a2 2 0 0 1 2-2z"/>',
 hangup:'<path d="M2.5 14.5c5.5-5 13.5-5 19 0l-1.8 3.2-3.7-1.2.2-3a9.5 9.5 0 0 0-8.4 0l.2 3-3.7 1.2z"/>',
 screen:'<rect x="3" y="4" width="18" height="12" rx="2"/><path d="M8 20h8M12 16v4"/>',
 flip:'<path d="M4 9V7a2 2 0 0 1 2-2h3M20 15v2a2 2 0 0 1-2 2h-3"/><path d="M7 3L9.5 5 7 7M17 21l-2.5-2 2.5-2"/><circle cx="12" cy="12" r="3"/>',
 clip:'<path d="M20 11l-8 8a5 5 0 0 1-7-7l9-9a3.5 3.5 0 0 1 5 5l-9 9a2 2 0 0 1-3-3l8-8"/>',
 sticker:'<path d="M4 5a1 1 0 0 1 1-1h14a1 1 0 0 1 1 1v9l-6 6H5a1 1 0 0 1-1-1z"/><path d="M14 20v-5a1 1 0 0 1 1-1h5"/>',
 smile:'<circle cx="12" cy="12" r="9"/><path d="M8 14a5 5 0 0 0 8 0M9 9.5h.01M15 9.5h.01"/>',
 reply:'<path d="M9 8L4 13l5 5"/><path d="M4 13h10a6 6 0 0 1 6 6"/>',
 send:'<path d="M3 11l18-8-8 18-2-8z"/>',
 gear:'<circle cx="12" cy="12" r="3"/><path d="M12 2v3M12 19v3M2 12h3M19 12h3M5 5l2 2M17 17l2 2M19 5l-2 2M7 17l-2 2"/>',
 x:'<path d="M6 6l12 12M18 6L6 18"/>',
 users:'<circle cx="9" cy="8" r="3.5"/><path d="M2.5 20a6.5 6.5 0 0 1 13 0M16 4.5a3.5 3.5 0 0 1 0 7M18 14a6.5 6.5 0 0 1 3.5 6"/>',
 user:'<circle cx="12" cy="8" r="4"/><path d="M4 21a8 8 0 0 1 16 0"/>',
 pencil:'<path d="M4 20l4-1L19 8l-3-3L5 16z"/><path d="M14 7l3 3"/>',
 expand:'<path d="M4 9V4h5M20 9V4h-5M4 15v5h5M20 15v5h-5"/>',
 compress:'<path d="M9 4v5H4M15 4v5h5M9 20v-5H4M15 20v-5h5"/>',
 more:'<path d="M5 12h.01M12 12h.01M19 12h.01"/>',
 userplus:'<circle cx="9" cy="8" r="3.5"/><path d="M2.5 20a6.5 6.5 0 0 1 13 0M19 8v6M16 11h6"/>',
 copy:'<rect x="9" y="9" width="11" height="11" rx="2"/><path d="M5 15V6a2 2 0 0 1 2-2h8"/>',
 shield:'<path d="M12 3l8 3v6c0 5-3.5 8-8 9-4.5-1-8-4-8-9V6z"/>',
 globe:'<circle cx="12" cy="12" r="9"/><path d="M3 12h18M12 3a14 14 0 0 1 0 18M12 3a14 14 0 0 0 0 18"/>',
 menu:'<path d="M4 6h16M4 12h16M4 18h16"/>',
 bell:'<path d="M6 16V11a6 6 0 0 1 12 0v5l2 2H4zM10 21h4"/>',
 file:'<path d="M6 3h8l5 5v13H6z"/><path d="M14 3v5h5"/>',
 trash:'<path d="M4 7h16M10 11v6M14 11v6M6 7l1 13h10l1-13M9 7V4h6v3"/>',
 search:'<circle cx="11" cy="11" r="7"/><path d="M20 20l-4-4"/>',
 download:'<path d="M12 4v11M7 11l5 5 5-5M5 20h14"/>',
 zoomin:'<circle cx="11" cy="11" r="7"/><path d="M20 20l-4-4M11 8v6M8 11h6"/>',
 zoomout:'<circle cx="11" cy="11" r="7"/><path d="M20 20l-4-4M8 11h6"/>',
 chev:'<path d="M6 9l6 6 6-6"/>',
 logout:'<path d="M9 4H5a1 1 0 0 0-1 1v14a1 1 0 0 0 1 1h4M16 8l4 4-4 4M20 12H9"/>',
 play:'<path d="M7 4.5v15l12-7.5z"/>',pause:'<path d="M8 5v14M16 5v14"/>',
 refresh:'<path d="M21 12a9 9 0 1 1-9-9c2.52 0 4.93 1 6.74 2.74L21 8"/><path d="M21 3v5h-5"/>'
};
const ic=(n,s=18)=>`<svg class="svg" width="${s}" height="${s}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">${IC[n]}</svg>`;
const ico=(n,s)=>el("span","ico",ic(n,s));
function ibtn(n,label,fn,cls="",title=""){
  const b=el("button",cls,ic(n)+(label?`<span>${label}</span>`:""));
  if(title||label)b.title=title||label;
  if(fn)b.onclick=fn;
  return b;
}
const AVC=["#5c6ff6","#8b5cf6","#d946ef","#ec4899","#f97316","#14b8a6","#22a559","#0ea5e9"];
function colorOf(name){let h=0;for(const c of name)h=(h*31+c.charCodeAt(0))>>>0;return AVC[h%AVC.length]}
function statusAvatar(name,cls,online){
  const w=el("div","avw");w.append(avatar(name,cls),el("span","sdot"+(online?" on":"")));return w;
}
function avatar(name,cls="av"){
  const a=el("div",cls);a.textContent=name.slice(0,1).toUpperCase();a.style.background=colorOf(name);
  const v=S.av[name];
  if(v){
    const i=el("img");i.alt="";i.onload=()=>a.classList.add("hasimg");i.onerror=()=>i.remove();
    i.src=`/api/avatar?name=${encodeURIComponent(name)}&v=${v}&token=${S.token}`;a.append(i);
  }
  return a;
}
const hhmm=t=>new Date(t*1000).toLocaleTimeString([],{hour:"2-digit",minute:"2-digit"});
const fullTime=t=>new Date(t*1000).toLocaleString([],{month:"numeric",day:"numeric",hour:"2-digit",minute:"2-digit"});

/* ---------- 状態 ---------- */
const S={token:localStorage.token||"",me:null,servers:[],dms:[],friends:[],code:"",calls:{},stamps:[],emojis:[],emap:{},av:{},announce:0,owner:false,homeTab:"d",callBig:false,
 view:"dm",chan:null,unread:{},ment:{},ws:null,reply:null,notify:"mention",push:false,sound:localStorage.sound!=="0"};
let call=null,toastT=0,modalCancel=null,last=null;
const coarse=matchMedia("(pointer:coarse)").matches;
const canScreen=!!(navigator.mediaDevices&&navigator.mediaDevices.getDisplayMedia);
const AUDIO={echoCancellation:true,noiseSuppression:false,autoGainControl:false}; /* ノイズキャンセルなし */
const ICE={iceServers:[{urls:"stun:stun.l.google.com:19302"}]};

async function api(path,method="GET",body){
  const r=await fetch(path,{method,headers:{"Content-Type":"application/json",...(S.token?{Authorization:"Bearer "+S.token}:{})},body:body?JSON.stringify(body):undefined});
  const d=await r.json().catch(()=>({}));
  if(!r.ok)throw new Error(typeof d.detail==="string"?d.detail:r.statusText);
  return d;
}
const T=async f=>{try{return await f()}catch(e){toast(e.message||"エラーが発生しました")}};
const wsend=o=>{if(S.ws&&S.ws.readyState===1){S.ws.send(JSON.stringify(o));return true}return false};
function toast(m){const t=$("toast");t.textContent=m;t.hidden=false;clearTimeout(toastT);toastT=setTimeout(()=>t.hidden=true,3200)}
function upload(url,file,onprog){
  return new Promise((res,rej)=>{
    const x=new XMLHttpRequest();x.open("POST",url);x.setRequestHeader("Authorization","Bearer "+S.token);
    if(onprog)x.upload.onprogress=onprog;
    x.onload=()=>{if(x.status<300)res();else{let m="アップロードに失敗しました";try{const d=JSON.parse(x.responseText).detail;if(typeof d==="string")m=d}catch(e){}rej(new Error(m))}};
    x.onerror=()=>rej(new Error("アップロードに失敗しました"));
    x.send(file);
  });
}
function copyText(t){
  (navigator.clipboard?navigator.clipboard.writeText(t):Promise.reject()).then(()=>toast("コピーしました")).catch(()=>toast("コピーできませんでした。長押しで選択してコピーしてください"));
}

/* ---------- モーダル / フォーム ---------- */
$("modal").addEventListener("mousedown",e=>{if(e.target===$("modal"))closeModal()});
function form(title,fields,ok="OK",pre=[]){
  return new Promise(res=>{
    const get={};
    const nodes=fields.map(f=>{
      const w=el("div","fld");
      if(f.type==="checkbox"){
        const l=el("label","chk"),i=el("input");i.type="checkbox";i.checked=!!f.value;l.append(i,f.label);get[f.k]=()=>i.checked;w.append(l);
      }else if(f.type==="select"){
        const l=el("label");l.textContent=f.label;const s=el("select");
        f.options.forEach(([v,t])=>{const o=el("option");o.value=v;o.textContent=t;s.append(o)});s.value=f.value;get[f.k]=()=>s.value;w.append(l,s);
      }else if(f.type==="checks"){
        const l=el("label");l.textContent=f.label;w.append(l);const boxes=[];
        f.options.forEach(([v,t])=>{const c=el("label","chk"),i=el("input");i.type="checkbox";i.checked=(f.value||[]).includes(v);c.append(i,t);w.append(c);boxes.push([v,i])});
        get[f.k]=()=>boxes.filter(b=>b[1].checked).map(b=>b[0]);
      }else{
        const l=el("label");l.textContent=f.label;const i=el("input");i.value=f.value||"";i.placeholder=f.ph||"";
        i.onkeydown=e=>{if(e.key==="Enter"&&!e.isComposing)go.click()};get[f.k]=()=>i.value.trim();w.append(l,i);
      }
      return w;
    });
    const go=el("button","btn",ok);
    go.onclick=()=>{const v={};for(const k in get)v[k]=get[k]();modalCancel=null;closeModal();res(v)};
    modal(title,[...pre,...nodes,go],()=>res(null));
  });
}
function confirmBox(msg,ok="OK"){
  return new Promise(res=>{
    const p=el("p","muted");p.textContent=msg;
    const row=el("div","row"),c=el("button","ghost","キャンセル"),o=el("button","btn",ok);
    c.onclick=()=>closeModal();o.onclick=()=>{modalCancel=null;closeModal();res(true)};row.append(c,o);
    modal("確認",[p,row],()=>res(false));
  });
}

/* ---------- ログイン / 起動 ---------- */
function initIcons(){
  $("navbtn").innerHTML=ic("menu",22);$("membtn").innerHTML=ic("users",20);$("gearb").innerHTML=ic("gear",19);$("gearb").title="設定";$("annb").innerHTML=ic("speaker",19);$("annb").title="公式アナウンス";
  $("bclip").innerHTML=ic("clip",21);$("bclip").title="ファイルを送信";$("bstamp").innerHTML=ic("sticker",21);$("bstamp").title="スタンプ";
  $("bemoji").innerHTML=ic("smile",21);$("bemoji").title="絵文字";
  $("bdown").innerHTML=ic("chev",22)+'<span class=bdn hidden></span>';$("bdown").title="一番下へ";
  $("bsend").innerHTML=ic("send",18)+'<span class="lbl">送信</span>';$("bsend").title="送信";
  $("hangb").innerHTML=ic("hangup",22);$("hangb").title="通話を終了";$("micc").innerHTML=ic("chev",14);$("micc").title="マイクを選ぶ";$("camc").innerHTML=ic("chev",14);$("camc").title="カメラを選ぶ";
  $("profbtn").innerHTML=ic("user",20);$("profbtn").title="プロフィール";$("sqb").innerHTML=ic("search",18);$("sqb").title="検索";$("navbtn").title="メニュー";$("membtn").title="メンバー";
}
async function doAuth(kind){
  $("aerr").textContent="";
  try{
    const d=await api("/api/"+kind,"POST",{name:$("an").value.trim(),password:$("ap").value});
    S.token=d.token;localStorage.token=d.token;await boot();
  }catch(e){$("aerr").textContent=e.message}
}
async function logout(){
  try{await leaveCall()}catch(e){}
  try{await api("/api/logout","POST")}catch(e){}
  localStorage.removeItem("token");S.token="";if(S.ws)S.ws.close();location.reload();
}
async function load(){
  const d=await api("/api/me");
  S.me=d.user;S.servers=d.servers;S.dms=d.dms;S.friends=d.friends;S.blocked=d.blocked||[];S.mutedList=d.muted||[];S.quiet=new Set([...S.blocked,...S.mutedList].map(x=>x.name));S.code=d.friend_code;S.calls=d.calls||{};
  S.persistent=!!d.persistent;S.appver=d.app_version||"";S.notify=d.notify_mode;S.push=d.push_available;S.announce=d.announce;S.owner=d.is_owner;
  if(S.view!=="dm"&&!S.servers.find(s=>s.id===S.view))S.view="dm";
  const mi=$("meinfo");mi.innerHTML="";const nm=el("span");nm.textContent=S.me.name;mi.append(avatar(S.me.name,"av sm2"),nm);mi.style.cursor="pointer";mi.onclick=()=>profileModal(S.me.name);
  drawRail();drawList();drawMembers();drawRing();updateCallBtn();drawNbar();
}
async function boot(){
  try{await load()}catch(e){localStorage.removeItem("token");S.token="";$("auth").hidden=false;$("app").hidden=true;return}
  $("auth").hidden=true;$("app").hidden=false;
  initHistory();
  connect();
  await Promise.all([loadStamps(),loadEmojis(),loadAvatars()]);
  setView("dm");
  const c=Number(new URLSearchParams(location.search).get("c"));if(c)goChannel(c);
  ensurePush();
}

/* ---------- WebSocket ---------- */
function connect(){
  const old=S.ws;
  const w=new WebSocket(`${location.protocol==="https:"?"wss":"ws"}://${location.host}/ws?token=${S.token}`);
  S.ws=w;if(old)old.close();
  w.onopen=()=>{checkUpdate();if(S.chan)openChan(S.chan,true)};
  w.onclose=()=>{if(S.ws===w&&S.token){if(call)leaveCall(true);setTimeout(connect,2000)}};
  w.onmessage=onEvent;
}
function onEvent(e){
  const d=JSON.parse(e.data);
  if(d.type==="typing"){tyAdd(d.channel,d.name)}
  else if(d.type==="message"){
    const m=d.message;if(TY[m.channel]){delete TY[m.channel][m.user];tyDraw()}
    if(m.channel===S.chan){
      const mine=!!S.me&&m.user===S.me.name,near=msgsNearBottom();
      addMsg(m);
      /* 自分の送信 / すでに一番下を見ているときだけ追従。読み返し中は動かさず、ボタンに新着数を出す */
      if(mine||near)toBottom(false);else{S.below=(S.below||0)+1;updDown()}
      if(document.hidden||!document.hasFocus()){S.unread[m.channel]=1;updateTitle()}
    }else{
      S.unread[m.channel]=1;
      const inServer=S.servers.some(s=>s.channels.some(c=>c.id===m.channel));
      if(!inServer||m.mentions.includes(S.me.name)||(m.reply&&m.reply.user===S.me.name))S.ment[m.channel]=(S.ment[m.channel]||0)+1;
      drawList();
    }
    notifyMsg(m);
  }else if(d.type==="reactions"){
    const x=document.querySelector(`[data-mid="${d.message_id}"]`);if(x)renderReactions(x,d.reactions);
  }else if(d.type==="refresh"){load().catch(()=>{})}
  else if(d.type==="presence"){
    const f=S.friends.find(x=>x.id===d.id);if(f)f.online=d.online;
    S.dms.forEach(x=>{if(x.uid===d.id)x.online=d.online;if(x.group)x.members.forEach(m=>{if(m.id===d.id)m.online=d.online})});
    if(S.view==="dm")drawList();else drawMembers();
  }
  else if(d.type==="stamps"){loadStamps()}
  else if(d.type==="message_edit"){
    const m=d.message,old=document.querySelector(`[data-mid="${m.id}"]`);
    if(old)old.replaceWith(buildMsg(m,old.classList.contains("g")));
  }
  else if(d.type==="message_delete"){
    const x=document.querySelector(`[data-mid="${d.id}"]`);
    if(x){const nx=x.nextElementSibling,again=!x.classList.contains("g")&&nx&&nx.classList.contains("g");x.remove();if(again&&S.chan===d.channel)openChan(S.chan,true)}
  }
  else if(d.type==="profile"){loadAvatars().then(()=>{load().catch(()=>{});if(S.chan)openChan(S.chan,true)})}
  else if(d.type==="emojis"){loadEmojis().then(()=>{if(S.chan)openChan(S.chan,true)})}
  else if(d.type==="call_peers"){onPeers(d)}
  else if(d.type==="call_state"){onCallState(d)}
  else if(d.type==="rtc"){onRtc(d).catch(console.error)}
  else if(d.type==="error"){if(call&&!call.joined)leaveCall(true);toast(d.message)}
}
window.addEventListener("focus",()=>{if(S.chan){S.unread[S.chan]=0;S.ment[S.chan]=0;if(S.me)drawList()}});

/* ---------- 通知 ---------- */
function updateTitle(){const n=Object.keys(S.unread).filter(k=>S.unread[k]).length;document.title=(n?`(${n}) `:"")+"Motto"}
function beep(){
  if(!S.sound)return;
  try{
    const c=new (window.AudioContext||window.webkitAudioContext)(),o=c.createOscillator(),g=c.createGain();
    o.type="sine";o.frequency.value=880;o.connect(g);g.connect(c.destination);
    g.gain.setValueAtTime(.001,c.currentTime);g.gain.exponentialRampToValueAtTime(.12,c.currentTime+.02);g.gain.exponentialRampToValueAtTime(.001,c.currentTime+.3);
    o.start();o.stop(c.currentTime+.32);setTimeout(()=>c.close(),500);
  }catch(e){}
}
async function showNotif(title,body,cid){
  if(!("Notification" in window)||Notification.permission!=="granted")return;
  const opt={body,icon:"/icon-192.png",badge:"/icon-192.png",tag:"ch"+cid,data:{channel:cid}};
  try{
    const reg=navigator.serviceWorker&&await navigator.serviceWorker.getRegistration();
    if(reg){await reg.showNotification(title,opt);return}
  }catch(e){}
  try{const n=new Notification(title,opt);n.onclick=()=>{window.focus();goChannel(cid);n.close()}}catch(e){}
}
function notifyMsg(m){
  if(m.user===S.me.name||S.notify==="off"||(S.quiet&&S.quiet.has(m.user)))return;
  const srv=S.servers.find(s=>s.channels.some(c=>c.id===m.channel));
  const hit=!srv||m.mentions.includes(S.me.name)||(m.reply&&m.reply.user===S.me.name);
  if(S.notify==="mention"&&!hit)return;
  if(document.hasFocus()&&!document.hidden&&m.channel===S.chan)return;
  beep();
  let title=m.user;
  if(srv){const c=srv.channels.find(c=>c.id===m.channel);title=`${m.user} (${srv.name} / ${c?c.name:""})`}
  const body=m.text||(m.stamp?"スタンプを送信しました":"ファイルを送信しました");
  showNotif(title,body.slice(0,120),m.channel);
}
const b64u=s=>{const r=(s+"=".repeat((4-s.length%4)%4)).replace(/-/g,"+").replace(/_/g,"/"),raw=atob(r);return Uint8Array.from([...raw].map(c=>c.charCodeAt(0)))};
async function subscribePush(){
  if(!S.push||!("serviceWorker" in navigator)||!("PushManager" in window))return false;
  const k=await api("/api/push/key");if(!k.key)return false;
  const reg=await navigator.serviceWorker.ready;
  let sub=await reg.pushManager.getSubscription();
  if(!sub)sub=await reg.pushManager.subscribe({userVisibleOnly:true,applicationServerKey:b64u(k.key)});
  await api("/api/push/subscribe","POST",{subscription:sub.toJSON()});
  return true;
}
async function ensurePush(){
  try{if("Notification" in window&&Notification.permission==="granted")await subscribePush()}catch(e){console.warn(e)}
}
async function enableNotifications(){
  if(!("Notification" in window)){toast("このブラウザは通知に対応していません (iPhoneはホーム画面に追加すると使えます)");return}
  const p=await Notification.requestPermission();
  if(p!=="granted"){toast("通知が許可されませんでした。ブラウザの設定で許可してください");drawNbar();return}
  let pushed=false;try{pushed=await subscribePush()}catch(e){console.warn(e)}
  toast(pushed?"通知をオンにしました (アプリを閉じていても届きます)":"通知をオンにしました (このタブを開いている間に届きます)");
  drawNbar();
}
function drawNbar(){
  const b=$("nbar");b.innerHTML="";
  const need="Notification" in window&&Notification.permission==="default"&&!localStorage.nbarHide&&S.notify!=="off";
  b.hidden=!need;if(!need)return;
  const s=el("span");s.textContent="新しいメッセージを通知で受け取りますか？";
  const ok=el("button","btn","通知をオンにする"),no=ibtn("x","",()=>{localStorage.nbarHide="1";drawNbar()},"ghost sq","あとで");
  ok.onclick=enableNotifications;b.append(ico("bell",18),s,ok,no);
}
if("serviceWorker" in navigator){
  navigator.serviceWorker.register("/sw.js").catch(()=>{});
  navigator.serviceWorker.addEventListener("message",e=>{if(e.data&&e.data.type==="open"&&e.data.channel)goChannel(e.data.channel)});
}

/* ---------- ナビゲーション (ホーム / サーバー) ---------- */
const curServer=()=>S.servers.find(s=>s.id===S.view);
const chanList=()=>S.view==="dm"?S.dms:(curServer()?curServer().channels:[]);
const chanName=cid=>{
  const d=S.dms.find(c=>c.id===cid);if(d)return d.name;
  for(const s of S.servers){const c=s.channels.find(c=>c.id===cid);if(c)return s.name+" / "+c.name}
  return "";
};
function callable(cid){
  if(!cid)return false;
  if(S.dms.some(c=>c.id===cid))return true;
  return S.servers.some(s=>s.channels.some(c=>c.id===cid&&c.kind==="voice"));
}
$("scrim").onclick=closeDrawers;

function setView(v,cid){
  S.view=v;drawRail();drawList();drawMembers();
  const l=chanList();
  const t=cid&&l.some(c=>c.id===cid)?cid:(l[0]&&l[0].id);
  if(t)openChan(t,true);
  else{S.chan=null;last=null;clearReply();$("profbtn").hidden=true;$("bar").hidden=false;$("ronote").hidden=true;$("headt").textContent=v==="dm"?"ホーム":"";updateCallBtn();emptyState()}
}
function goChannel(cid){
  if(cid===S.announce){openAnnounce();return}
  if(S.dms.some(c=>c.id===cid))return setView("dm",cid);
  const s=S.servers.find(s=>s.channels.some(c=>c.id===cid));if(s)setView(s.id,cid);
}
function emptyState(){
  const m=$("msgs");m.innerHTML="";
  const e=el("div","empty");const i=el("img");i.src="/icon-192.png";i.alt="";
  const p=el("div");p.textContent=S.view==="dm"?"フレンドを選んでメッセージを送ろう":"チャンネルがありません";
  e.append(i,p,ibtn("menu","メニューを開く",openNav,"btn mo"));m.append(e);
}
function drawRail(){
  const r=$("rail");r.innerHTML="";
  const mk=(inner,title,fn,cls)=>{const b=el("button","rb "+cls,inner);b.title=title;b.onclick=fn;r.append(b);return b};
  mk('<img src="/icon-192.png" alt="">',"ホーム",()=>setView("dm"),"home"+(S.view==="dm"?" active":""));
  r.append(el("div","sep"));
  S.servers.forEach(s=>{
    const b=mk("",s.name,()=>setView(s.id),S.view===s.id?"active":"");
    if(s.icon){const i=el("img");i.alt="";i.src=`/api/servers/${s.id}/icon?v=${s.icon}&token=${S.token}`;b.append(i)}
    else b.textContent=s.name.slice(0,2);
  });
  mk(ic("plus",24),"サーバーを作成 / 参加",addServerModal,"green");
  mk(ic("compass",24),"サーバーを探す",discoverModal,"green");
}
const mitem=(icon,label,fn)=>{const b=el("button","mi ghost",ic(icon)+`<span>${label}</span>`);b.onclick=()=>{closeModal();fn()};return b};
function friendMenu(f){
  const act=(path,msg)=>T(async()=>{await api(path,"POST",{id:f.id});await load();toast(msg)});
  const items=[mitem("user","プロフィールを見る",()=>profileModal(f.name))];
  if(!f.blocked)items.push(mitem("send","メッセージを送る",()=>openDm(f.name)));
  if(f.friend)items.push(mitem("trash","フレンドを削除",async()=>{
    if(await confirmBox(f.name+" をフレンドから削除しますか？ (DMの履歴は残ります)","削除"))act("/api/friends/remove",f.name+" をフレンドから削除しました")
  }));
  items.push(f.muted
    ?mitem("bell","通知のミュートを解除",()=>act("/api/mutes/remove",f.name+" からの通知を再開しました"))
    :mitem("bell","この人からの通知をミュート",()=>act("/api/mutes/add",f.name+" からの通知をミュートしました")));
  items.push(f.blocked
    ?mitem("shield","ブロックを解除",()=>act("/api/blocks/remove",f.name+" のブロックを解除しました"))
    :mitem("shield","ブロック",async()=>{
      if(await confirmBox(f.name+" をブロックしますか？ フレンドから外れ、お互いにDMを送れなくなります","ブロック"))act("/api/blocks/add",f.name+" をブロックしました")
    }));
  $("modal").classList.add("sheet");  // スマホでは下から出るメニューにする
  modal(f.name,items,()=>$("modal").classList.remove("sheet"));
}
function openAnnounce(){S.view="dm";drawRail();drawMembers();openChan(S.announce)}
function updateAnnBtn(){
  const b=$("annb");if(!b)return;
  b.classList.toggle("active",S.chan===S.announce);
  b.querySelectorAll(".badge,.udot").forEach(x=>x.remove());
  if(S.ment[S.announce]){const x=el("span","badge");x.textContent=S.ment[S.announce];b.append(x)}
  else if(S.unread[S.announce])b.append(el("span","udot"));
}

/* ---------- メッセージ ---------- */
function emojiEl(v){
  const m=/^:([a-z0-9_]{2,12}):$/.exec(v);
  if(m&&S.emap[m[1]]){const i=el("img","cemoji");i.src=`/api/emojis/${S.emap[m[1]]}/img?token=${S.token}`;i.alt=v;i.title=v;return i}
  const s=el("span","uemoji");s.textContent=v;return s;
}
function cleanUrl(u){
  for(;;){
    const c=u.slice(-1);
    if(/[.,;:!?\]]/.test(c)){u=u.slice(0,-1);continue}
    if(c===")"&&(u.match(/\(/g)||[]).length<(u.match(/\)/g)||[]).length){u=u.slice(0,-1);continue}
    return u;
  }
}
const URL_RE=/https?:\/\/[\w\-.~:\/?#\[\]@!$&()*+,;=%]+/;
function renderText(box,text){
  let n=0,custom=0;
  text.split(/(https?:\/\/[\w\-.~:\/?#\[\]@!$&()*+,;=%]+|@[^\s@]+|:[a-z0-9_]{2,12}:)/).forEach(p=>{
    if(!p)return;
    if(/^https?:\/\//.test(p)){
      const u=cleanUrl(p),tail=p.slice(u.length);
      const a=el("a","lnk");a.href=u;a.target="_blank";a.rel="noopener noreferrer nofollow";a.textContent=u;
      box.append(a);if(tail)box.append(tail);n+=2;
    }
    else if(p[0]==="@"&&p.length>1){const s=el("span","mention"+(p.slice(1)===S.me.name?" me":""));s.textContent=p;box.append(s);n++}
    else if(/^:[a-z0-9_]{2,12}:$/.test(p)&&S.emap[p.slice(1,-1)]){box.append(emojiEl(p));custom++;n++}
    else{box.append(p);if(p.trim())n+=2}
  });
  if(custom===1&&n===1)box.classList.add("jumbo");
}
/* ---------- URLの埋め込み (リンクプレビュー) ---------- */
const LP=new Map();
const lpImg=u=>"/api/linkimg?url="+encodeURIComponent(u)+"&token="+encodeURIComponent(S.token);
function getPreview(url){
  if(!LP.has(url))LP.set(url,fetch("/api/linkpreview?url="+encodeURIComponent(url),{headers:{Authorization:"Bearer "+S.token}})
    .then(r=>r.ok?r.json():{ok:false}).catch(()=>({ok:false})));
  return LP.get(url);
}
function lpCard(pv,url){
  const a=el("a","lpc"),href=/^https?:\/\//.test(pv.url||"")?pv.url:url;
  a.href=href;a.target="_blank";a.rel="noopener noreferrer nofollow";
  if(pv.kind==="image"){
    a.classList.add("lpimg");
    const im=el("img");im.alt="";im.loading="lazy";im.src=lpImg(pv.image);im.onerror=()=>a.remove();a.append(im);
    return a;
  }
  if(pv.site){const s=el("div","lps");s.textContent=pv.site;a.append(s)}
  const h=el("div","lph");h.textContent=pv.title||href;a.append(h);
  if(pv.desc){const d=el("div","lpd");d.textContent=pv.desc;a.append(d)}
  if(pv.image){const im=el("img","lpi");im.alt="";im.loading="lazy";im.src=lpImg(pv.image);im.onerror=()=>im.remove();a.append(im)}
  return a;
}
function attachPreview(box,text){
  const x=URL_RE.exec(text||"");if(!x)return;
  const url=cleanUrl(x[0]),ph=el("div","lpw");box.append(ph);
  getPreview(url).then(pv=>{
    if(!pv||!pv.ok)return;
    const near=msgsNearBottom();
    ph.append(lpCard(pv,url));
    if(near&&$("msgs").contains(ph))toBottom(false);
  });
}

/* ---------- ボイスメッセージのプレイヤー ---------- */
let curAu=null;
function fmtT(s){if(!isFinite(s)||s<0)s=0;s=Math.round(s);return Math.floor(s/60)+":"+String(s%60).padStart(2,"0")}
function voiceEl(m,url){
  const w=el("div","vp"),au=new Audio();au.preload="metadata";au.src=url;
  const pb=el("button","vpb",ic("play",20));pb.type="button";pb.title="再生";
  const col=el("div","vcol"),wave=el("div","vwave"),meta=el("div","vmeta");
  const N=matchMedia("(max-width:520px)").matches?28:38;
  let seed=0;for(const ch of String(m.file_id||m.id))seed=(seed*31+ch.charCodeAt(0))>>>0;
  const rnd=()=>{seed=(seed*1664525+1013904223)>>>0;return seed/4294967296};
  const bars=[];for(let i=0;i<N;i++){const b=el("i");b.style.height=Math.round(20+rnd()*70)+"%";wave.append(b);bars.push(b)}
  const tm=el("span","vt");tm.textContent="0:00";
  const sp=el("button","vsp","1×");sp.type="button";sp.title="再生速度";
  const dl=ibtn("download","",()=>downloadFile(m),"vdl","ダウンロード");
  let dur=0,drag=false,raf=0;
  const total=()=>isFinite(au.duration)&&au.duration>0?au.duration:dur;
  const paint=()=>{
    const t=total(),p=t?au.currentTime/t:0,on=Math.round(p*N);
    bars.forEach((b,i)=>b.classList.toggle("on",i<on));
    tm.textContent=(!au.paused||au.currentTime>0)?fmtT(au.currentTime)+" / "+fmtT(t):fmtT(t);
  };
  const tick=()=>{paint();if(!au.paused)raf=requestAnimationFrame(tick)};
  au.onloadedmetadata=()=>{
    if(!isFinite(au.duration)){ /* 録音データは長さが無限と出ることがあるので、末尾へ飛ばして長さを求める */
      const fix=()=>{au.removeEventListener("timeupdate",fix);dur=au.duration;au.currentTime=0;paint()};
      au.addEventListener("timeupdate",fix);au.currentTime=1e7;
    }else{dur=au.duration;paint()}
  };
  au.ondurationchange=()=>{if(isFinite(au.duration)){dur=au.duration;paint()}};
  au.onplay=()=>{pb.innerHTML=ic("pause",20);pb.title="一時停止";w.classList.add("play");cancelAnimationFrame(raf);tick()};
  au.onpause=()=>{pb.innerHTML=ic("play",20);pb.title="再生";w.classList.remove("play");paint()};
  au.onended=()=>{au.currentTime=0;paint()};
  au.onerror=()=>{tm.textContent="読み込めませんでした"};
  pb.onclick=()=>{
    if(au.paused){if(curAu&&curAu!==au)curAu.pause();curAu=au;au.play().catch(()=>toast("再生できませんでした"))}
    else au.pause();
  };
  const rates=[1,1.5,2,0.75];
  sp.onclick=()=>{const r=rates[(rates.indexOf(au.playbackRate)+1)%rates.length];au.playbackRate=r;sp.textContent=r+"×"};
  const seek=e=>{const t=total();if(!t)return;const r=wave.getBoundingClientRect();au.currentTime=Math.min(1,Math.max(0,(e.clientX-r.left)/r.width))*t;paint()};
  wave.onpointerdown=e=>{wave.setPointerCapture(e.pointerId);drag=true;seek(e)};
  wave.onpointermove=e=>{if(drag)seek(e)};
  wave.onpointerup=wave.onpointercancel=()=>{drag=false};
  meta.append(tm,sp,dl);col.append(wave,meta);w.append(pb,col);
  return w;
}

function jumpTo(id){
  const t=document.querySelector(`[data-mid="${id}"]`);
  if(t){t.scrollIntoView({block:"center"});t.classList.add("flash");setTimeout(()=>t.classList.remove("flash"),1400)}
  else toast("元のメッセージは読み込まれていません");
}
function addMsg(m){
  const grouped=!!(last&&last.chan===m.channel&&last.user===m.user&&m.ts-last.ts<300&&!m.reply);
  last={user:m.user,ts:m.ts,chan:m.channel};
  $("msgs").append(buildMsg(m,grouped));
}
async function editMsg(m){
  const f=await form("メッセージを編集",[{k:"text",label:"メッセージ",value:m.text}],"保存する");
  if(f&&f.text&&f.text!==m.text)T(()=>api(`/api/messages/${m.id}/edit`,"POST",{text:f.text}));
}
async function delMsg(m){
  if(await confirmBox("このメッセージを削除しますか？","削除"))T(()=>api("/api/messages/"+m.id,"DELETE"));
}
const dlUrl=m=>`/files/${m.file_id}?dl=1&token=${S.token}`;
function downloadFile(m){const a=document.createElement("a");a.href=dlUrl(m);a.download=m.file_name||"file";document.body.append(a);a.click();a.remove()}
function closeLightbox(){const l=$("lb");if(l)l.remove();document.removeEventListener("keydown",lbKey)}
function lbKey(e){if(e.key==="Escape")closeLightbox()}
function openLightbox(m,url){
  closeLightbox();
  const L=el("div");L.id="lb";
  const isV=(m.file_type||"").startsWith("video/");
  const md=el(isV?"video":"img");md.src=url;
  let sc=1,x=0,y=0;
  const apply=()=>{md.style.transform=`translate(${x}px,${y}px) scale(${sc})`};
  const setZ=v=>{sc=Math.min(8,Math.max(1,v));if(sc===1){x=0;y=0}apply()};
  const bar=el("div");bar.id="lbbar";
  if(isV){md.controls=true;md.autoplay=true;md.playsInline=true}
  else{
    md.alt=m.file_name||"";
    bar.append(ibtn("zoomout","",()=>setZ(sc/1.4),"sq","縮小"),ibtn("zoomin","",()=>setZ(sc*1.4),"sq","拡大"));
    L.addEventListener("wheel",e=>{e.preventDefault();setZ(e.deltaY<0?sc*1.15:sc/1.15)},{passive:false});
    md.ondblclick=()=>setZ(sc>1?1:2.5);
    md.ondragstart=e=>e.preventDefault();
    const ptr=new Map();let d0=0,s0=1;
    const dist=()=>{const [a,b]=[...ptr.values()];return Math.hypot(a[0]-b[0],a[1]-b[1])};
    md.onpointerdown=e=>{md.setPointerCapture(e.pointerId);ptr.set(e.pointerId,[e.clientX,e.clientY]);if(ptr.size===2){d0=dist();s0=sc}};
    md.onpointermove=e=>{
      const p=ptr.get(e.pointerId);if(!p)return;
      if(ptr.size===2){ptr.set(e.pointerId,[e.clientX,e.clientY]);if(d0)setZ(s0*dist()/d0)}
      else{if(sc>1){x+=e.clientX-p[0];y+=e.clientY-p[1];apply()}ptr.set(e.pointerId,[e.clientX,e.clientY])}
    };
    md.onpointerup=md.onpointercancel=e=>{ptr.delete(e.pointerId)};
  }
  bar.append(ibtn("download","",()=>downloadFile(m),"sq","ダウンロード"),ibtn("x","",closeLightbox,"sq","閉じる"));
  L.onclick=e=>{if(e.target===L)closeLightbox()};
  L.append(md,bar);document.body.append(L);
  document.addEventListener("keydown",lbKey);
}
function buildMsg(m,grouped){
  const hit=m.mentions.includes(S.me.name)||(m.reply&&m.reply.user===S.me.name);
  const d=el("div","m"+(grouped?" g":"")+(hit?" mme":""));d.dataset.mid=m.id;
  const side=el("div","ms");
  if(grouped){const g=el("span","gt");g.textContent=hhmm(m.ts);side.append(g)}
  else{const av=avatar(m.user);av.style.cursor="pointer";av.onclick=()=>profileModal(m.user);side.append(av)}
  const body=el("div","mb");
  if(m.reply){
    const r=el("div","ref");r.append(ico("reply",14));
    const u=el("b");u.textContent=m.reply.user;
    const t=el("span");t.textContent=m.reply.kind==="stamp"?"スタンプ":m.reply.kind==="file"?"ファイル":m.reply.text;
    r.append(u,t);r.onclick=()=>jumpTo(m.reply.id);body.append(r);
  }
  if(!grouped){
    const hd=el("div","mh2"),u=el("b"),t=el("small");u.textContent=m.user;u.style.cursor="pointer";u.onclick=()=>profileModal(m.user);t.textContent=fullTime(m.ts);hd.append(u,t);body.append(hd);
  }
  if(m.text){
    const p=el("div","mt");renderText(p,m.text);
    if(m.edited){const e=el("small","edited");e.textContent="編集済み";p.append(e)}
    body.append(p);
    attachPreview(body,m.text);
  }
  if(m.stamp){const i=el("img","stamp");i.alt="スタンプ";i.src=`/api/stamps/${m.stamp}/img?token=${S.token}`;body.append(i)}
  if(m.file_id){
    const url=`/files/${m.file_id}?token=${S.token}`,ty=m.file_type||"";let e;
    if(ty.startsWith("video/")){e=el("video");e.controls=true;e.preload="metadata";e.playsInline=true;e.src=url}
    else if(ty.startsWith("image/")){e=el("img","pic");e.src=url;e.title="クリックで拡大";e.onclick=()=>openLightbox(m,url)}
    else if(ty.startsWith("audio/")){e=voiceEl(m,url)}
    else{e=el("a","fileln");e.href=dlUrl(m);e.download=m.file_name||"file";e.append(ico("download",18));const s=el("span");s.textContent=m.file_name;e.append(s)}
    body.append(e);
    if(ty.startsWith("video/")||ty.startsWith("image/")){
      const fa=el("div","fa");
      fa.append(ibtn("zoomin","拡大",()=>openLightbox(m,url),"","拡大して見る"),ibtn("download","保存",()=>downloadFile(m),"","ダウンロード"));
      body.append(fa);
    }
  }
  body.append(el("div","rx"));
  const act=el("div","act");
  const readOnly=S.chan===S.announce&&!S.owner;
  if(!readOnly){
    const sm=ibtn("smile","",()=>openPicker(sm,v=>react(m.id,v)),"","リアクション");
    act.append(sm,ibtn("reply","",()=>setReply(m),"","返信"));
  }
  const mine=m.user===S.me.name,srv=curServer();
  if(mine&&m.text)act.append(ibtn("pencil","",()=>editMsg(m),"","編集"));
  if(mine||(S.chan===S.announce?S.owner:(srv&&srv.admin)))act.append(ibtn("trash","",()=>delMsg(m),"","削除"));
  d.append(side,body,act);
  renderReactions(d,m.reactions);
  return d;
}
function renderReactions(d,r){
  const rx=d.querySelector(".rx");rx.innerHTML="";
  for(const [e,names] of Object.entries(r)){
    const b=el("button","chip"+(names.includes(S.me.name)?" mine":""));
    b.append(emojiEl(e),document.createTextNode(" "+names.length));b.title=names.join(", ");
    b.onclick=()=>{if(!(S.chan===S.announce&&!S.owner))react(Number(d.dataset.mid),e)};rx.append(b);
  }
}
const react=(mid,emoji)=>wsend({type:"react",message_id:mid,emoji});
async function doSearch(fromBtn){
  if(!S.chan)return;
  let q=$("sq").value.trim();
  if(!q||(fromBtn&&matchMedia("(max-width:820px)").matches)){
    const f=await form("メッセージを検索",[{k:"q",label:"キーワード",value:q}],"検索");
    if(!f||!f.q)return;q=f.q;
  }
  const cid=S.chan;
  const d=await T(()=>api(`/api/channels/${cid}/search?q=${encodeURIComponent(q)}`));
  if(!d)return;
  const list=el("div","dlist");
  if(!d.messages.length){const p=el("p","muted");p.textContent="見つかりませんでした";list.append(p)}
  d.messages.forEach(m=>{
    const row=el("div","drow"),box=el("div"),b=el("b"),sm=el("small"),t=el("div");
    b.textContent=m.user;sm.textContent=fullTime(m.ts);t.textContent=m.text.slice(0,120);t.style.wordBreak="break-word";
    box.append(b,sm,t);row.append(box);row.style.cursor="pointer";
    row.onclick=()=>{closeModal();jumpTo(m.id)};list.append(row);
  });
  modal("「"+q+"」の検索結果",[list]);
}
$("sq").addEventListener("keydown",e=>{if(e.key==="Enter"&&!e.isComposing)doSearch(false)});
function setReply(m){
  S.reply={id:m.id,user:m.user};
  const b=$("replybar");b.innerHTML="";b.hidden=false;
  const t=el("span");t.textContent=m.user+" に返信中";
  b.append(ico("reply",16),t,ibtn("x","",clearReply,"xb","返信をやめる"));$("txt").focus();
}
function clearReply(){S.reply=null;$("replybar").hidden=true;$("replybar").innerHTML=""}
function sendMsg(){
  const v=$("txt").value.trim();
  if(!v||!S.chan)return;
  if(wsend({type:"send",channel:S.chan,text:v,reply_to:S.reply?S.reply.id:null})){$("txt").value="";clearReply()}
  else toast("接続中です。少し待ってからもう一度送ってください");
}
$("txt").addEventListener("keydown",e=>{if(e.key!=="Enter"||e.isComposing)return;if(P("enter",true)||e.ctrlKey||e.metaKey)sendMsg()});
$("ap").addEventListener("keydown",e=>{if(e.key==="Enter")doAuth("login")});
$("f").onchange=async()=>{
  const file=$("f").files[0];$("f").value="";if(!file||!S.chan)return;
  try{
    await upload(`/api/channels/${S.chan}/upload?name=${encodeURIComponent(file.name)}&type=${encodeURIComponent(file.type||"application/octet-stream")}`,file,
      e=>{$("prog").textContent=`アップロード中 ${(e.loaded/e.total*100).toFixed(1)}%`});
  }catch(e){toast(e.message)}
  $("prog").textContent="";
};

/* ---------- 絵文字ピッカー / オリジナル絵文字 / スタンプ ---------- */
const EG=[
["顔","😀 😃 😄 😁 😆 😅 🤣 😂 🙂 🙃 😉 😊 😇 🥰 😍 🤩 😘 😗 😚 😋 😛 😜 🤪 😝 🤗 🤭 🤫 🤔 🤐 😐 😑 😶 😏 😒 🙄 😬 😌 😔 😪 😴 😷 🤒 🤕 🤢 🤮 🥵 🥶 😵 🤯 🤠 😎 🤓 😕 😟 🙁 😮 😲 😳 🥺 😢 😭 😱 😖 😞 😤 😡 😠 🤬 😈 💀 💩 🤡 👻 👽 🤖"],
["手・人","👍 👎 👌 🤞 🤟 🤘 🤙 👈 👉 👆 👇 👋 🤚 ✋ 🖖 👏 🙌 👐 🤝 🙏 💪 🧠 👀 👂 👃 👄 💅 🤳 🙋 🙇 🤦 🤷 💃 🕺 🏃 👶 🧑 👨 👩 👴 👵"],
["動物・自然","🐶 🐱 🐭 🐹 🐰 🦊 🐻 🐼 🐨 🐯 🦁 🐮 🐷 🐸 🐵 🐔 🐧 🐦 🐤 🦆 🦉 🐺 🐴 🦄 🐝 🐛 🦋 🐌 🐞 🐢 🐍 🐙 🦀 🐠 🐬 🐳 🦈 🐘 🦒 🌸 🌹 🌻 🌲 🌴 🍀 🍁 🌈 ⭐ 🌙 🔥 💧 🌊 ⚡"],
["食べ物","🍎 🍊 🍋 🍌 🍉 🍇 🍓 🍒 🍑 🥭 🍍 🥝 🍅 🥑 🍆 🥕 🌽 🥔 🍞 🧀 🍳 🥓 🍔 🍟 🍕 🌭 🌮 🍣 🍜 🍙 🍘 🍡 🍦 🍩 🍪 🎂 🍰 🍫 🍬 🍭 ☕ 🍵 🍺 🍻 🍷 🥤"],
["活動・物","⚽ 🏀 🏈 ⚾ 🎾 🏐 🎮 🎯 🎲 🎨 🎬 🎤 🎧 🎸 🎹 🥁 📷 📱 💻 💡 📚 📌 🔑 🔒 🎁 🎈 🎉 🎊 🏆 🥇 🚀 🚗 🚲 🏠 ⏰ 💰 💎"],
["記号","❤️ 🧡 💛 💚 💙 💜 🖤 🤍 💔 💕 💖 💗 💯 ✅ ❌ ⭕ ❗ ❓ ⚠️ 🚫 💢 💥 💫 💤 ✨ 🔔 🎵 🎶 ➕ ➖ ✔️ 🆗 🆕 🆒 🔴 🟠 🟡 🟢 🔵 🟣 ⚪ ⚫"]
];
let recent=[];try{recent=JSON.parse(localStorage.recent||"[]")}catch(e){}
function pushRecent(v){recent=[v,...recent.filter(x=>x!==v)].slice(0,24);localStorage.recent=JSON.stringify(recent)}
let pkAnchor=null;
function closePicker(){const p=$("picker");if(p)p.remove();document.removeEventListener("mousedown",pkOutside,true)}
function pkOutside(e){const p=$("picker");if(p&&!p.contains(e.target)&&!(pkAnchor&&pkAnchor.contains(e.target)))closePicker()}
function openPicker(anchor,cb){
  closePicker();pkAnchor=anchor;
  const p=el("div","pk");p.id="picker";
  const tabs=el("div","pktabs"),grid=el("div","pkgrid");
  const names=["最近",...EG.map(g=>g[0]),"オリジナル"];
  const choose=v=>{pushRecent(v);closePicker();cb(v)};
  const pe=v=>{const b=el("button","pe");b.append(emojiEl(v));b.onclick=()=>choose(v);return b};
  const show=i=>{
    [...tabs.children].forEach((b,j)=>b.classList.toggle("on",j===i));
    grid.innerHTML="";
    if(i===0){
      if(!recent.length){const q=el("p","muted");q.textContent="まだ使った絵文字はありません";grid.append(q)}
      recent.forEach(v=>grid.append(pe(v)));
    }else if(i<=EG.length){EG[i-1][1].split(" ").forEach(v=>grid.append(pe(v)))}
    else{
      S.emojis.forEach(c=>{
        const w=el("div","pw");w.append(pe(":"+c.name+":"));
        if(c.mine){
          const x=el("span","pex",ic("x",10));
          x.onclick=async e=>{e.stopPropagation();if(await confirmBox(`:${c.name}: を削除しますか？`,"削除"))T(()=>api("/api/emojis/"+c.id,"DELETE"))};
          w.append(x);
        }
        grid.append(w);
      });
      grid.append(ibtn("plus","オリジナル絵文字を追加",()=>{closePicker();pickImage("emoji")},"ghost"));
    }
  };
  names.forEach((n,i)=>{const b=el("button");b.textContent=n;b.onclick=()=>show(i);tabs.append(b)});
  p.append(tabs,grid);document.body.append(p);
  show(recent.length?0:1);
  const r=anchor.getBoundingClientRect(),w=p.offsetWidth,h=p.offsetHeight;
  p.style.left=Math.min(Math.max(8,r.right-w),innerWidth-w-8)+"px";
  let y=r.top-h-8;if(y<8)y=Math.min(r.bottom+8,innerHeight-h-8);p.style.top=y+"px";
  setTimeout(()=>document.addEventListener("mousedown",pkOutside,true),0);
}
$("bemoji").onclick=()=>{
  const t=$("txt");
  openPicker($("bemoji"),v=>{t.value+=v;t.focus()});
};
function pickImage(kind){$("imgf").dataset.kind=kind;$("imgf").click()}
$("imgf").onchange=()=>{const f=$("imgf").files[0];$("imgf").value="";if(f)imageModal($("imgf").dataset.kind,f)};
async function imageModal(kind,file){
  const isE=kind==="emoji",url=URL.createObjectURL(file);
  const pv=el("img","pv");pv.src=url;
  const hint=el("p","muted");hint.textContent=isE?"チャットで :名前: と入力するか、絵文字ピッカーから使えます (512KBまで)":"全員が使えるスタンプになります (5MBまで)";
  const f=await form(isE?"オリジナル絵文字を追加":"スタンプを追加",
    [{k:"name",label:isE?"名前 (英小文字・数字・_ の2〜12文字)":"名前",value:file.name.replace(/\.[^.]+$/,"").toLowerCase().replace(/[^a-z0-9_]/g,"").slice(0,12)}],"追加する",[pv,hint]);
  URL.revokeObjectURL(url);
  if(!f)return;
  try{await upload(`${isE?"/api/emojis":"/api/stamps"}?name=${encodeURIComponent(f.name)}`,file);toast("追加しました")}
  catch(e){toast(e.message)}
}
async function loadEmojis(){
  const d=await api("/api/emojis").catch(()=>null);
  if(d){S.emojis=d.emojis;S.emap={};d.emojis.forEach(e=>S.emap[e.name]=e.id)}
}
async function loadStamps(){
  const d=await api("/api/stamps").catch(()=>null);
  if(d){S.stamps=d.stamps;drawStamps()}
}
function toggleStamps(){$("stampbox").hidden=!$("stampbox").hidden}
function drawStamps(){
  const b=$("stampbox");b.innerHTML="";
  S.stamps.forEach(s=>{
    const w=el("div","st"),i=el("img");i.src=`/api/stamps/${s.id}/img?token=${S.token}`;i.title=s.name;i.alt=s.name;
    i.onclick=()=>{if(S.chan){wsend({type:"stamp",channel:S.chan,stamp_id:s.id,reply_to:S.reply?S.reply.id:null});clearReply()}b.hidden=true};
    w.append(i);
    if(s.mine)w.append(ibtn("x","",()=>{confirmBox("このスタンプを削除しますか？","削除").then(ok=>{if(ok)T(()=>api("/api/stamps/"+s.id,"DELETE"))})},"","削除"));
    b.append(w);
  });
  b.append(ibtn("plus","スタンプを追加",()=>pickImage("stamp"),"ghost"));
}

/* ---------- 通話 (WebRTC / 参加者どうしを直接つなぐ方式) ---------- */
const rtc=(to,data)=>wsend({type:"rtc",channel:call.channel,to,data});
async function joinCall(cid){
  if(call)await leaveCall();
  let stream=null;
  try{stream=await navigator.mediaDevices.getUserMedia({audio:AUDIO})}catch(e){stream=null}
  const atrack=stream?stream.getAudioTracks()[0]:null;
  call={channel:cid,stream:stream||new MediaStream(),atrack,vtrack:null,preview:null,strack:null,sprev:null,
    mic:!!atrack,cam:false,screen:false,facing:"user",devId:null,multi:false,peers:{},joined:false};
  if(!wsend({type:"call_join",channel:cid})){leaveCall(true);return}
  if(!atrack){sendMedia();toast("マイクが使えないので、聞くだけで参加しました。マイクをつないだら、マイクのボタンを押してください")}
  drawCall();
}
async function leaveCall(local){
  if(!call)return;
  if(!local)wsend({type:"call_leave",channel:call.channel});
  Object.values(call.peers).forEach(p=>p.pc.close());
  call.stream.getTracks().forEach(t=>t.stop());
  if(call.vtrack)call.vtrack.stop();
  if(call.strack){call.strack.onended=null;call.strack.stop()}
  call=null;S.callBig=false;
  try{if(document.fullscreenElement)document.exitFullscreen().catch(()=>{})}catch(e){}
  drawCall();drawList();drawRing();
}
function newPeer(id,name){
  const pc=new RTCPeerConnection(ICE);
  const p={id,name,pc,stream:new MediaStream(),sstream:new MediaStream(),pending:[],vs:null,ss:null};
  pc.onicecandidate=e=>{if(e.candidate&&call)rtc(id,{candidate:e.candidate})};
  pc.ontrack=e=>{
    if(e.track.kind==="audio")p.stream.addTrack(e.track);
    else{
      const vts=pc.getTransceivers().filter(t=>t.receiver.track.kind==="video").sort((a,b)=>Number(a.mid)-Number(b.mid));
      (vts.indexOf(e.transceiver)===1?p.sstream:p.stream).addTrack(e.track);
    }
    drawCall();
  };
  call.peers[id]=p;return p;
}
async function flush(p){for(const c of p.pending.splice(0))await p.pc.addIceCandidate(c).catch(()=>{})}
/* 後から入った人が、先にいる人へ接続を申し込む (音声 + カメラ用ビデオ + 画面共有用ビデオ) */
async function callPeer(id,name){
  const p=newPeer(id,name),pc=p.pc;
  p.as=call.atrack?pc.addTrack(call.atrack,call.stream):pc.addTransceiver("audio",{direction:"sendrecv"}).sender;
  const cam=pc.addTransceiver("video",{direction:"sendrecv"}),scr=pc.addTransceiver("video",{direction:"sendrecv"});
  p.vs=cam.sender;p.ss=scr.sender;
  if(call.vtrack)await cam.sender.replaceTrack(call.vtrack);
  if(call.strack)await scr.sender.replaceTrack(call.strack);
  await pc.setLocalDescription(await pc.createOffer());
  rtc(id,{description:pc.localDescription});
}
async function onPeers(d){
  if(!call||call.channel!==d.channel)return;
  call.joined=true;
  for(const p of d.peers)await callPeer(p.id,p.name).catch(console.error);
}
async function onRtc(d){
  if(!call||d.channel!==call.channel)return;
  const x=d.data;let p=call.peers[d.from];
  if(x.description){
    if(x.description.type==="offer"){
      if(p)p.pc.close();
      p=newPeer(d.from,d.name);
      await p.pc.setRemoteDescription(x.description);
      let vi=0;
      for(const t of p.pc.getTransceivers().sort((a,b)=>Number(a.mid)-Number(b.mid))){
        t.direction="sendrecv";
        if(t.receiver.track.kind==="audio"){await t.sender.replaceTrack(call.atrack);p.as=t.sender}
        else if(vi++===0){await t.sender.replaceTrack(call.vtrack);p.vs=t.sender}
        else{await t.sender.replaceTrack(call.strack);p.ss=t.sender}
      }
      await flush(p);
      await p.pc.setLocalDescription(await p.pc.createAnswer());
      rtc(d.from,{description:p.pc.localDescription});
    }else if(p){await p.pc.setRemoteDescription(x.description);await flush(p)}
  }else if(x.candidate&&p){
    if(p.pc.remoteDescription)await p.pc.addIceCandidate(x.candidate).catch(()=>{});
    else p.pending.push(x.candidate);
  }
}
function onCallState(d){
  if(d.users.length)S.calls[d.channel]=d.users;else delete S.calls[d.channel];
  if(call&&call.channel===d.channel){
    const ids=new Set(d.users.map(u=>u.id));
    for(const [id,p] of Object.entries(call.peers))if(!ids.has(Number(id))){p.pc.close();delete call.peers[id]}
  }
  drawCall();drawList();drawRing();
}
const sendMedia=()=>wsend({type:"call_media",mic:call.mic,cam:call.cam,screen:call.screen});
async function toggleMic(){
  if(!call)return;
  if(!call.atrack){  /* マイク無しで入った場合: もう一度マイクを試す */
    let st;
    try{st=await navigator.mediaDevices.getUserMedia({audio:AUDIO})}
    catch(e){toast("マイクを使えません。マイクをつないで、ブラウザで許可してください");return}
    call.stream=st;call.atrack=st.getAudioTracks()[0];call.mic=true;
    for(const p of Object.values(call.peers))if(p.as)await p.as.replaceTrack(call.atrack);
    sendMedia();drawCall();return;
  }
  call.mic=!call.mic;call.atrack.enabled=call.mic;sendMedia();drawCall();
}
async function camStream(){
  const c={width:{ideal:1280},height:{ideal:720}};
  if(call.devId)c.deviceId={exact:call.devId};else if(coarse)c.facingMode=call.facing;
  return navigator.mediaDevices.getUserMedia({video:c});
}
async function applyVideo(t){
  call.vtrack=t;call.preview=t?new MediaStream([t]):null;call.cam=!!t;
  for(const p of Object.values(call.peers))if(p.vs)await p.vs.replaceTrack(t);
  sendMedia();drawCall();
}
async function toggleCam(){
  if(!call)return;
  if(call.cam){call.vtrack.stop();await applyVideo(null);return}
  let s;
  try{s=await camStream()}catch(e){toast("カメラを使えません。許可を確認してください");return}
  try{call.multi=coarse||(await navigator.mediaDevices.enumerateDevices()).filter(d=>d.kind==="videoinput").length>1}catch(e){}
  await applyVideo(s.getVideoTracks()[0]);
}
/* インカメラ / アウトカメラ切替 (パソコンでは接続中のカメラを順番に切替) */
async function switchCam(){
  if(!call||!call.cam)return;
  const old=call.vtrack;
  if(coarse){call.facing=call.facing==="user"?"environment":"user";call.devId=null}
  else{
    const ds=(await navigator.mediaDevices.enumerateDevices()).filter(d=>d.kind==="videoinput");
    if(ds.length<2)return;
    const i=ds.findIndex(d=>d.deviceId===old.getSettings().deviceId);
    call.devId=ds[(i+1)%ds.length].deviceId;
  }
  old.stop();
  try{await applyVideo((await camStream()).getVideoTracks()[0])}
  catch(e){toast("カメラを切り替えられませんでした");await applyVideo(null)}
}
async function toggleScreen(){
  if(!call)return;
  if(call.screen){await stopScreen();return}
  let s;
  try{s=await navigator.mediaDevices.getDisplayMedia({video:{frameRate:{ideal:30}},audio:false})}
  catch(e){if(e.name!=="NotAllowedError")toast("画面共有を開始できませんでした");return}
  call.strack=s.getVideoTracks()[0];call.strack.contentHint="detail";
  call.sprev=new MediaStream([call.strack]);call.screen=true;
  call.strack.onended=()=>{if(call&&call.screen)stopScreen()};
  for(const p of Object.values(call.peers))if(p.ss)await p.ss.replaceTrack(call.strack);
  sendMedia();drawCall();
}
async function stopScreen(){
  if(!call||!call.screen)return;
  call.strack.onended=null;call.strack.stop();call.strack=null;call.sprev=null;call.screen=false;
  for(const p of Object.values(call.peers))if(p.ss)await p.ss.replaceTrack(null);
  sendMedia();drawCall();
}
function getTile(tiles,id,cls,muted){
  let t=document.getElementById(id);
  if(!t){
    t=el("div","tile "+cls);t.id=id;
    const v=el("video");v.autoplay=true;v.playsInline=true;v.muted=!!muted;
    t.append(v,el("div","nm"));tiles.append(t);
  }
  return t;
}
function setSrc(t,src){
  const v=t.querySelector("video");
  if(v.srcObject!==src){v.srcObject=src;if(src)v.play().catch(()=>{})}
}
function drawCall(){
  const box=$("call"),tiles=$("tiles");
  if(!call){box.hidden=true;box.classList.remove("big");tiles.innerHTML="";updateCallBtn();return}
  box.hidden=false;box.classList.toggle("big",!!S.callBig);
  const bg=$("bigb");bg.innerHTML=ic(S.callBig?"compress":"expand",22);bg.title=S.callBig?"小さく表示":"大きく表示";
  const ct=$("calltitle");ct.innerHTML="";ct.append(ico("speaker",15),document.createTextNode(chanName(call.channel)));
  const setB=(id,icon,state,title)=>{const b=$(id);b.innerHTML=ic(icon,22);b.classList.toggle("off",state===false);b.classList.toggle("live",state==="live");b.title=title};
  setB("micb",call.mic?"mic":"micoff",call.mic,!call.atrack?"マイクをオンにする":call.mic?"ミュートする":"ミュートを解除");
  setB("camb",call.cam?"video":"videooff",call.cam,call.cam?"カメラをオフ":"カメラをオン");
  const fb=$("flipb");fb.hidden=!(call.cam&&call.multi);
  if(!fb.hidden){fb.innerHTML=ic("flip",20);fb.title=coarse?(call.facing==="user"?"アウトカメラに切り替え":"インカメラに切り替え"):"カメラを切り替え"}
  const sb=$("scrb");sb.hidden=!canScreen;
  if(canScreen)setB("scrb","screen",call.screen?"live":true,call.screen?"画面共有を停止":"画面を共有");
  const users=S.calls[call.channel]||[],keep=new Set();
  users.forEach(u=>{
    const me=u.id===S.me.id,id="t"+u.id;keep.add(id);
    const t=getTile(tiles,id,"",me);t.dataset.uid=u.id;
    if(!t.querySelector(".av"))t.insertBefore(avatar(u.name,"av cav"),t.querySelector(".nm"));
    t.classList.toggle("cam",!!u.cam);t.classList.toggle("off",!u.mic);
    t.classList.toggle("mirror",me&&(!coarse||call.facing==="user"));
    const nm=t.querySelector(".nm");nm.innerHTML="";if(!u.mic)nm.append(ico("micoff",13));nm.append(document.createTextNode(me?u.name+" (あなた)":u.name));
    setSrc(t,me?(call.cam?call.preview:null):(call.peers[u.id]?call.peers[u.id].stream:null));
    if(u.screen){
      const sid="s"+u.id;keep.add(sid);
      const s=getTile(tiles,sid,"screen",true);
      s.onclick=()=>{const v=s.querySelector("video");if(v.requestFullscreen)v.requestFullscreen()};
      const sn=s.querySelector(".nm");sn.innerHTML="";sn.append(ico("screen",13),document.createTextNode(me?"あなたの画面共有":u.name+" の画面共有"));
      setSrc(s,me?call.sprev:(call.peers[u.id]?call.peers[u.id].sstream:null));
    }
  });
  [...tiles.children].forEach(t=>{if(!keep.has(t.id))t.remove()});
  updateCallBtn();
}
function updateCallBtn(){
  const b=$("callbtn"),inHere=!!(call&&call.channel===S.chan);
  b.hidden=!callable(S.chan);
  b.innerHTML=ic(inHere?"hangup":"phone",18)+`<span class="lbl">${inHere?"退出":"通話"}</span>`;
  b.classList.toggle("live",inHere);b.title=inHere?"通話から退出":"通話に参加";
}
function toggleBig(){
  S.callBig=!S.callBig;
  try{
    if(S.callBig&&document.documentElement.requestFullscreen)document.documentElement.requestFullscreen().catch(()=>{});
    else if(!S.callBig&&document.fullscreenElement)document.exitFullscreen().catch(()=>{});
  }catch(e){}
  drawCall();
}
document.addEventListener("fullscreenchange",()=>{if(!document.fullscreenElement&&S.callBig){S.callBig=false;if(call)drawCall()}});
async function deviceMenu(kind){
  if(!call)return;
  const isA=kind==="audioinput";
  let ds=[];
  try{ds=(await navigator.mediaDevices.enumerateDevices()).filter(d=>d.kind===kind)}catch(e){}
  if(!ds.length){toast(isA?"マイクが見つかりません":"カメラが見つかりません");return}
  const t=isA?call.atrack:call.vtrack,cur=t&&t.getSettings().deviceId;
  modal(isA?"マイクを選ぶ":"カメラを選ぶ",ds.map((d,i)=>{
    const b=el("button","mi ghost");b.textContent=(d.label||(isA?"マイク ":"カメラ ")+(i+1))+(d.deviceId===cur?"  ✓":"");
    b.onclick=()=>{closeModal();isA?pickMic(d.deviceId):pickCam(d.deviceId)};return b;
  }));
}
async function pickMic(id){
  if(!call)return;
  let st;
  try{st=await navigator.mediaDevices.getUserMedia({audio:{...AUDIO,deviceId:{exact:id}}})}
  catch(e){toast("このマイクに切り替えられませんでした");return}
  const t=st.getAudioTracks()[0],on=call.atrack?call.mic:true,old=call.stream;
  t.enabled=on;call.mic=on;call.stream=st;call.atrack=t;
  for(const p of Object.values(call.peers))if(p.as)await p.as.replaceTrack(t);
  old.getTracks().forEach(x=>x.stop());
  sendMedia();drawCall();
}
async function pickCam(id){
  if(!call)return;
  call.devId=id;
  if(!call.cam){toggleCam();return}
  call.vtrack.stop();
  try{await applyVideo((await camStream()).getVideoTracks()[0])}
  catch(e){toast("このカメラに切り替えられませんでした");await applyVideo(null)}
}
const callBtn=()=>{if(call&&call.channel===S.chan)leaveCall();else joinCall(S.chan)};
function drawRing(){
  const r=$("ring");r.innerHTML="";
  S.dms.forEach(c=>{
    const u=S.calls[c.id];
    if(u&&u.length&&!(call&&call.channel===c.id)){
      const d=el("div"),s=el("span");s.textContent=c.name+" さんが通話中です";
      const b=ibtn("phone","参加する",async()=>{setView("dm",c.id);joinCall(c.id)},"btn");
      d.append(s,b);r.append(d);
    }
  });
  r.hidden=!r.children.length;
}

/* ---------- フレンド / サーバー / ロール の操作 ---------- */
function addServerModal(){
  const tabs=el("div","tabs"),tc=el("button","on","サーバーを作成"),tj=el("button","","サーバーに参加");
  const c=el("div","fld"),j=el("div","fld");
  const sw=k=>{c.hidden=k!=="c";j.hidden=k!=="j";tc.classList.toggle("on",k==="c");tj.classList.toggle("on",k==="j")};
  tc.onclick=()=>sw("c");tj.onclick=()=>sw("j");tabs.append(tc,tj);
  const n=el("input");n.placeholder="サーバー名";
  const pl=el("label","chk"),pc=el("input");pc.type="checkbox";pl.append(pc,"公開する (サーバー検索に表示されます)");
  const cb=el("button","btn","作成する");
  cb.onclick=async()=>{
    const name=n.value.trim();if(!name)return;
    const d=await T(()=>api("/api/servers","POST",{name,public:pc.checked}));
    if(d){closeModal();await load();setView(d.id)}
  };
  c.append(n,pl,cb);
  const ji=el("input");ji.placeholder="招待コード";
  const jb=el("button","btn","参加する");
  jb.onclick=async()=>{
    const code=ji.value.trim();if(!code)return;
    const d=await T(()=>api("/api/servers/join","POST",{invite:code}));
    if(d){closeModal();await load();setView(d.id)}
  };
  const hint=el("p","muted");hint.textContent="友達から受け取った招待コードを入力してください。公開サーバーは左の探すボタンから探せます。";
  j.append(ji,jb,hint);
  modal("サーバーを追加",[tabs,c,j]);sw("c");
}
function discoverModal(){
  const q=el("input");q.placeholder="サーバー名で検索";q.type="search";
  const list=el("div","dlist");let timer=0;
  const run=async()=>{
    const d=await api("/api/servers/discover?q="+encodeURIComponent(q.value.trim())).catch(()=>null);
    if(!d)return;
    list.innerHTML="";
    if(!d.servers.length){const p=el("p","muted");p.textContent="公開されているサーバーが見つかりません";list.append(p)}
    d.servers.forEach(s=>{
      const row=el("div","drow"),box=el("div"),b1=el("b"),sm=el("small");
      b1.textContent=s.name;sm.textContent=`メンバー ${s.members}人`;box.append(b1,sm);
      const bt=el("button",s.joined?"ghost":"btn",s.joined?"参加済み":"参加する");bt.disabled=s.joined;
      bt.onclick=async()=>{const r=await T(()=>api(`/api/servers/${s.id}/join_public`,"POST"));if(r){closeModal();await load();setView(s.id)}};
      row.append(box,bt);list.append(row);
    });
  };
  q.oninput=()=>{clearTimeout(timer);timer=setTimeout(run,250)};
  modal("サーバーを探す",[q,list]);run();
}
function serverMenu(){
  const s=curServer();if(!s)return;
  const item=(icon,label,fn)=>{const b=el("button","mi ghost",ic(icon)+`<span>${label}</span>`);b.onclick=()=>{closeModal();fn()};return b};
  modal(s.name,[
    item("copy","招待コードを表示",()=>{
      const c=el("div","code");c.textContent=s.invite;
      modal("招待コード",[c,ibtn("copy","コピー",()=>copyText(s.invite),"btn")]);
    }),
    ...(s.admin?[item("gear","サーバー名とアイコンを変更",()=>editServerModal(s))]:[]),
    item("hash","テキストチャンネルを追加",()=>newChannel("text")),
    item("speaker","ボイスチャンネルを追加",()=>newChannel("voice")),
    item("shield","ロールを作成",newRole),
    item("users","ロールを付与 / 解除",assignRole),
    item("globe","公開設定 (現在: "+(s.public?"公開":"非公開")+")",()=>T(()=>api(`/api/servers/${s.id}/public`,"POST",{public:!s.public})))
  ]);
}
async function newChannel(kind){
  const f=await form(kind==="voice"?"ボイスチャンネルを追加":"テキストチャンネルを追加",[{k:"name",label:"チャンネル名"}],"追加する");
  if(f&&f.name)T(()=>api(`/api/servers/${S.view}/channels`,"POST",{name:f.name,kind}));
}
async function newRole(){
  const f=await form("ロールを作成",[
    {k:"name",label:"ロール名"},
    {k:"perms",type:"checks",label:"権限",value:["send","files","react"],options:[["send","発言"],["files","ファイル送信"],["react","リアクション"],["channels","チャンネル管理"],["roles","ロール管理"],["admin","管理者 (すべて)"]]}
  ],"作成する");
  if(f&&f.name){const d=await T(()=>api(`/api/servers/${S.view}/roles`,"POST",{name:f.name,perms:f.perms}));if(d)toast("ロールを作成しました")}
}
async function assignRole(){
  const f=await form("ロールを付与 / 解除",[
    {k:"user",label:"ユーザー名"},{k:"role",label:"ロール名"},
    {k:"act",type:"select",label:"操作",value:"add",options:[["add","付与する"],["remove","解除する"]]}
  ],"実行する");
  if(f&&f.user&&f.role){const d=await T(()=>api(`/api/servers/${S.view}/assign`,"POST",{user:f.user,role:f.role,remove:f.act==="remove"}));if(d)toast("完了しました")}
}
async function openDm(name){
  const d=await T(()=>api("/api/dm","POST",{name}));
  if(d){await load();setView("dm",d.id)}
}
function shiftHue(hex,d){
  const r=parseInt(hex.slice(1,3),16)/255,g=parseInt(hex.slice(3,5),16)/255,b=parseInt(hex.slice(5,7),16)/255;
  const mx=Math.max(r,g,b),mn=Math.min(r,g,b),l=(mx+mn)/2;let h=0,s=0;
  if(mx!==mn){const k=mx-mn;s=l>.5?k/(2-mx-mn):k/(mx+mn);h=(mx===r?(g-b)/k+(g<b?6:0):mx===g?(b-r)/k+2:(r-g)/k+4)*60}
  h=(h+d)%360;
  const a=s*Math.min(l,1-l),f=n=>{const k=(n+h/30)%12;return Math.round(255*(l-a*Math.max(-1,Math.min(k-3,9-k,1))))};
  return "#"+[f(0),f(8),f(4)].map(x=>x.toString(16).padStart(2,"0")).join("");
}
function applyTheme(){
  const t=localStorage.theme||"system",root=document.documentElement;
  const light=t==="light"||(t==="system"&&matchMedia("(prefers-color-scheme: light)").matches);
  root.dataset.theme=light?"light":"dark";
  const a=localStorage.accent;
  if(a&&/^#[0-9a-f]{6}$/i.test(a)){root.style.setProperty("--acc",a);root.style.setProperty("--acc2",shiftHue(a,50))}
  else{root.style.removeProperty("--acc");root.style.removeProperty("--acc2")}
  document.body.classList.toggle("pop",localStorage.pop!=="0");
  const m=document.querySelector("meta[name=theme-color]");if(m)m.content=light?"#eceef7":"#0e0f14";
}
matchMedia("(prefers-color-scheme: light)").addEventListener("change",()=>{if((localStorage.theme||"system")==="system")applyTheme()});
async function editServerModal(s){
  let blob=null,remove=false;
  const prev=el("div","pvc");
  const draw=url=>{
    prev.innerHTML="";
    const a=el("div","av xl");a.style.borderRadius="28px";a.textContent=s.name.slice(0,2);a.style.background=colorOf(s.name);
    if(url){const i=el("img");i.alt="";i.src=url;a.classList.add("hasimg");a.append(i)}
    prev.append(a);
  };
  draw(s.icon?`/api/servers/${s.id}/icon?v=${s.icon}&token=${S.token}`:null);
  const fi=el("input");fi.type="file";fi.accept="image/*";fi.hidden=true;
  fi.onchange=async()=>{
    const f=fi.files[0];fi.value="";if(!f)return;
    const b=await cropImage(f);
    if(b===undefined)return;
    if(!b){toast("画像を読み込めませんでした");return}
    blob=b;remove=false;draw(URL.createObjectURL(b));
  };
  const row=el("div","row");row.style.justifyContent="center";
  row.append(ibtn("plus","アイコンを選ぶ",()=>fi.click(),"ghost"),ibtn("trash","アイコンを削除",()=>{blob=null;remove=true;draw(null)},"ghost"),fi);
  const w1=el("div","fld"),l1=el("label"),ni=el("input");
  l1.textContent="サーバー名";ni.value=s.name;ni.maxLength=30;w1.append(l1,ni);
  const go=el("button","btn","保存する");
  go.onclick=async()=>{
    try{
      await api(`/api/servers/${s.id}/edit`,"POST",{name:ni.value.trim()});
      if(blob)await upload(`/api/servers/${s.id}/icon`,blob);
      else if(remove)await api(`/api/servers/${s.id}/icon`,"DELETE");
      closeModal();toast("サーバーを更新しました");await load();
    }catch(e){toast(e.message)}
  };
  modal("サーバー名とアイコン",[prev,row,w1,go]);
}

/* ---------- プロフィール (アイコン / 名前 / 自己紹介) ---------- */
async function loadAvatars(){const d=await api("/api/avatars").catch(()=>null);if(d)S.av=d.avatars}
async function squareBlob(file,size=256){
  try{
    const bmp=await createImageBitmap(file),s=Math.min(bmp.width,bmp.height),c=document.createElement("canvas");
    c.width=c.height=size;
    c.getContext("2d").drawImage(bmp,(bmp.width-s)/2,(bmp.height-s)/2,s,s,0,0,size,size);
    return await new Promise(r=>c.toBlob(r,"image/webp",.9));
  }catch(e){return file.size<=2*1024*1024?file:null}
}
function mkMedia(url,mime,opacity){
  const isV=(mime||"").startsWith("video/");
  const e=el(isV?"video":"img","pfm");
  if(isV){e.autoplay=true;e.loop=true;e.muted=true;e.playsInline=true}else e.alt="";
  e.src=url;e.style.opacity=(opacity==null?100:opacity)/100;e.onerror=()=>e.remove();
  return e;
}
function mediaLayer(spec,name,kind){
  if(!spec||!spec.has)return null;
  return mkMedia(`/api/profile/media?name=${encodeURIComponent(name)}&kind=${kind}&v=${spec.v}&token=${S.token}`,spec.mime,spec.opacity);
}
function mediaEditor(kind,label,spec){
  let blob=null,remove=false,mime=spec&&spec.has?spec.mime:"",url=spec&&spec.has?`/api/profile/media?name=${encodeURIComponent(S.me.name)}&kind=${kind}&v=${spec.v}&token=${S.token}`:null;
  const w=el("div","fld"),l=el("label");l.textContent=label;
  const box=el("div","mpv"+(kind==="bg"?" bgp":"")),op=el("input");op.type="range";op.min=0;op.max=100;op.value=spec?spec.opacity:100;
  const ot=el("span","muted");
  const label2=()=>{ot.textContent="不透明度 "+op.value+"%"};
  const draw=()=>{box.innerHTML="";if(url)box.append(mkMedia(url,mime,op.value));else box.textContent="未設定";label2()};
  op.oninput=()=>{label2();const m=box.querySelector(".pfm");if(m)m.style.opacity=op.value/100};
  const fi=el("input");fi.type="file";fi.accept="image/png,image/jpeg,image/gif,image/webp,video/mp4,video/webm";fi.hidden=true;
  fi.onchange=()=>{
    const f=fi.files[0];fi.value="";if(!f)return;
    blob=f;remove=false;mime=f.type;url=URL.createObjectURL(f);draw();
  };
  const row=el("div","row");row.style.justifyContent="flex-start";
  row.append(ibtn("plus","画像 / 動画を選ぶ",()=>fi.click(),"ghost"),ibtn("trash","削除",()=>{blob=null;remove=true;url=null;draw()},"ghost"),fi);
  w.append(l,box,ot,op,row);draw();
  return {node:w,opacity:()=>Number(op.value),
    save:async()=>{if(blob)await upload(`/api/profile/media/${kind}`,blob);else if(remove)await api(`/api/profile/media/${kind}`,"DELETE")}};
}
async function profileModal(name){
  const p=await T(()=>api("/api/profile/get","POST",{name}));if(!p)return;
  const top=el("div","pf");
  const nm=el("div","pfn");nm.textContent=p.name;
  const st=el("div","muted");st.textContent=(p.online?"オンライン":"オフライン")+" / 登録日 "+new Date(p.created*1000).toLocaleDateString();
  const bio=el("div","pfb");bio.textContent=p.bio||"自己紹介はまだありません";if(!p.bio)bio.style.color="var(--tx2)";
  const bn=el("div","pfbn"),bm=mediaLayer(p.banner,p.name,"banner");if(bm)bn.append(bm);
  top.append(bn,avatar(p.name,"av xl"),nm,st,bio);
  const bgm=mediaLayer(p.bg,p.name,"bg");
  if(bgm){const w=el("div","pfbg");w.append(bgm);top.prepend(w);top.classList.add("hasbg")}
  const act=p.me?ibtn("user","プロフィールを編集",()=>{closeModal();editProfileModal()},"btn")
                :ibtn("send","メッセージを送る",()=>{closeModal();openDm(p.name)},"btn");
  modal("プロフィール",[top,act]);
}
async function editProfileModal(){
  const p=await T(()=>api("/api/profile/get","POST",{name:S.me.name}));if(!p)return;
  let blob=null,remove=false;
  const prev=el("div","pvc");
  const draw=url=>{
    prev.innerHTML="";
    const a=el("div","av xl");a.textContent=S.me.name.slice(0,1).toUpperCase();a.style.background=colorOf(S.me.name);
    if(url){const i=el("img");i.alt="";i.src=url;a.classList.add("hasimg");a.append(i)}
    prev.append(a);
  };
  draw(S.av[S.me.name]?`/api/avatar?name=${encodeURIComponent(S.me.name)}&v=${S.av[S.me.name]}&token=${S.token}`:null);
  const fi=el("input");fi.type="file";fi.accept="image/*";fi.hidden=true;
  fi.onchange=async()=>{
    const f=fi.files[0];fi.value="";if(!f)return;
    const b=await cropImage(f);
    if(b===undefined)return;
    if(!b){toast("画像を読み込めませんでした");return}
    blob=b;remove=false;draw(URL.createObjectURL(b));
  };
  const row=el("div","row");row.style.justifyContent="center";
  row.append(ibtn("plus","アイコンを選ぶ",()=>fi.click(),"ghost"),ibtn("trash","アイコンを削除",()=>{blob=null;remove=true;draw(null)},"ghost"),fi);
  const w1=el("div","fld"),l1=el("label"),ni=el("input");
  l1.textContent="名前 (ログインにも使います / 2〜20文字)";ni.value=S.me.name;ni.maxLength=20;w1.append(l1,ni);
  const w2=el("div","fld"),l2=el("label"),ta=el("textarea");
  l2.textContent="自己紹介 (200文字まで)";ta.value=p.bio;ta.maxLength=200;ta.placeholder="好きなことや、ひとことをどうぞ";w2.append(l2,ta);
  const be=mediaEditor("banner","バナー (画像・動画 / 容量無制限)",p.banner),ge=mediaEditor("bg","プロフィールの背景 (画像・動画 / 容量無制限)",p.bg);
  const go=el("button","btn","保存する");
  go.onclick=async()=>{
    try{
      toast("保存中です。動画は時間がかかることがあります");
      await api("/api/profile","POST",{name:ni.value.trim(),bio:ta.value.trim(),banner_opacity:be.opacity(),bg_opacity:ge.opacity()});
      await be.save();await ge.save();
      if(blob)await upload("/api/profile/avatar",blob);
      else if(remove)await api("/api/profile/avatar","DELETE");
      closeModal();toast("プロフィールを保存しました");
      await loadAvatars();await load();if(S.chan)openChan(S.chan,true);
    }catch(e){toast(e.message)}
  };
  modal("プロフィールを編集",[prev,row,w1,w2,be.node,ge.node,go]);
}


/* ======================================================================
   v5 追加機能: 設定の保存 / 戻るボタン / アイコン切り抜き / 発言リング / グループ / 設定画面
   ====================================================================== */

/* ---------- 端末ごとの設定 (localStorage.prefs) ---------- */
function P(k,d){try{const o=JSON.parse(localStorage.prefs||"{}");return k in o?o[k]:d}catch(e){return d}}
function setP(k,v){let o={};try{o=JSON.parse(localStorage.prefs||"{}")}catch(e){}o[k]=v;localStorage.prefs=JSON.stringify(o);applyPrefs()}
function applyPrefs(){
  const b=document.body;
  b.classList.toggle("compact",!!P("compact",false));
  b.classList.toggle("reduce",!!P("reduce",false));
  b.classList.toggle("hc",!!P("contrast",false));
  b.classList.toggle("big",!!P("big",false));
  document.documentElement.style.setProperty("--mfs",(P("fs",100)/100*15)+"px");
  AUDIO.noiseSuppression=!!P("ns",false);
  AUDIO.echoCancellation=P("ec",true)!==false;
  AUDIO.autoGainControl=!!P("agc",false);
  const id=P("micId","");if(id)AUDIO.deviceId={ideal:id};else delete AUDIO.deviceId;
}

/* ---------- 戻るボタン対応 (画面を1つずつ戻る / スマホは一覧→アプリを抜ける) ---------- */
const UIS=[];let uiN=0,pendingBack=0,deferred=[];
const MOB=()=>matchMedia("(max-width:820px)").matches;
function uiOpen(close){const id=++uiN;UIS.push({id,close});if(pendingBack)deferred.push(id);else history.pushState({ui:id},"");return id}
function uiClosed(id){
  const i=UIS.findIndex(x=>x.id===id);if(i<0)return;
  const top=i===UIS.length-1;UIS.splice(i,1);
  const di=deferred.indexOf(id);if(di>=0){deferred.splice(di,1);return}
  if(top){pendingBack++;history.back()}
}
const atGuard=()=>!!(history.state&&history.state.g===0);
function fixGuard(){if(MOB()&&S.me&&atGuard())history.pushState({g:1},"")}
addEventListener("popstate",()=>{
  if(pendingBack){pendingBack--;if(!pendingBack){fixGuard();deferred.splice(0).forEach(id=>history.pushState({ui:id},""))}return}
  const t=UIS.pop();
  if(t){t.close();fixGuard();return}
  if(MOB()&&S.me&&atGuard()){
    if(S.chan&&!$("nav").classList.contains("open"))openNav();
    else history.back();
  }
});
function initHistory(){if(MOB()&&!(history.state&&history.state.g!=null)){history.replaceState({g:0},"");history.pushState({g:1},"")}}

let modalUi=0,navUi=0,memUi=0,lbUi=0,setUi=0;
function closeModal(fromPop){
  const f=modalCancel;modalCancel=null;$("modal").hidden=true;$("mbox").innerHTML="";
  const id=modalUi;modalUi=0;if(id&&fromPop!==true)uiClosed(id);
  if(f)f();
}
function modal(title,nodes,onCancel){
  const had=modalUi;modalUi=0;
  const f0=modalCancel;modalCancel=null;$("mbox").innerHTML="";if(f0)f0();
  const b=$("mbox"),h=el("div","mh"),t=el("b");t.textContent=title;
  h.append(t,ibtn("x","",closeModal,"ghost sq","閉じる"));
  b.append(h,...nodes);modalCancel=onCancel||null;$("modal").hidden=false;
  modalUi=had||uiOpen(()=>closeModal(true));
  const i=b.querySelector("input:not([type=checkbox])");if(i&&!coarse)setTimeout(()=>i.focus(),60);
}
function _scrimOff(){if(!$("nav").classList.contains("open")&&!$("members").classList.contains("open"))$("scrim").hidden=true}
function openNav(){$("nav").classList.add("open");$("scrim").hidden=false;if(!navUi)navUi=uiOpen(()=>{navUi=0;$("nav").classList.remove("open");_scrimOff()})}
function openMem(){$("members").classList.add("open");$("scrim").hidden=false;if(!memUi)memUi=uiOpen(()=>{memUi=0;$("members").classList.remove("open");_scrimOff()})}
function closeDrawers(){
  const a=navUi,b=memUi;navUi=memUi=0;
  $("nav").classList.remove("open");$("members").classList.remove("open");$("scrim").hidden=true;
  [b,a].forEach(i=>{if(i)uiClosed(i)});
}
{
  const _c=closeLightbox,_o=openLightbox;
  closeLightbox=function(fromPop){_c();const id=lbUi;lbUi=0;if(id&&fromPop!==true)uiClosed(id)};
  openLightbox=function(m,url){_o(m,url);if(!lbUi)lbUi=uiOpen(()=>{lbUi=0;_c()})};
}

/* ---------- アイコンを写す範囲を選ぶ (ドラッグで移動 / スライダーで拡大) ---------- */
function cropImage(file,size=256){
  return new Promise(async res=>{
    let bmp;
    try{bmp=await createImageBitmap(file)}catch(e){res(null);return}
    const V=Math.max(200,Math.min(300,innerWidth-70));
    const ov=el("div","crop"),box=el("div","cropbox"),h=el("b");h.textContent="アイコンに写す範囲を選ぶ";
    const hint=el("small","muted");hint.textContent="ドラッグで動かして、スライダーで大きさを変えられます";
    const stage=el("div","cropstage");stage.style.width=stage.style.height=V+"px";
    const cv=el("canvas");cv.width=cv.height=V;const g=cv.getContext("2d");
    stage.append(cv,el("div","cropmask"));
    let zoom=1,ox=0,oy=0;const base=V/Math.min(bmp.width,bmp.height);
    const clamp=()=>{const mx=(bmp.width*base*zoom-V)/2,my=(bmp.height*base*zoom-V)/2;ox=Math.max(-mx,Math.min(mx,ox));oy=Math.max(-my,Math.min(my,oy))};
    const draw=(ctx,k)=>{const w=bmp.width*base*zoom*k,hh=bmp.height*base*zoom*k;ctx.clearRect(0,0,V*k,V*k);ctx.drawImage(bmp,V*k/2-w/2+ox*k,V*k/2-hh/2+oy*k,w,hh)};
    const sl=el("input");sl.type="range";sl.min=100;sl.max=400;sl.value=100;
    sl.oninput=()=>{zoom=sl.value/100;clamp();draw(g,1)};
    let drag=null;
    stage.onpointerdown=e=>{drag={x:e.clientX,y:e.clientY};stage.setPointerCapture(e.pointerId)};
    stage.onpointermove=e=>{if(!drag)return;ox+=e.clientX-drag.x;oy+=e.clientY-drag.y;drag={x:e.clientX,y:e.clientY};clamp();draw(g,1)};
    stage.onpointerup=stage.onpointercancel=()=>{drag=null};
    stage.onwheel=e=>{e.preventDefault();sl.value=Math.max(100,Math.min(400,Number(sl.value)-e.deltaY/4));sl.oninput()};
    let id=0,done=false;
    const finish=v=>{if(done)return;done=true;ov.remove();const i=id;id=0;if(i)uiClosed(i);res(v)};
    const ok=el("button","btn","これにする"),no=el("button","ghost","やめる");
    no.onclick=()=>finish(undefined);
    ok.onclick=()=>{const o=document.createElement("canvas");o.width=o.height=size;draw(o.getContext("2d"),size/V);o.toBlob(b=>finish(b||null),"image/webp",.92)};
    const row=el("div","row");row.style.justifyContent="center";row.append(no,ok);
    box.append(h,hint,stage,sl,row);ov.append(box);document.body.append(ov);
    clamp();draw(g,1);
    id=uiOpen(()=>{id=0;finish(undefined)});
  });
}

/* ---------- 話している人のアイコンに緑のリング ---------- */
const SPK={ctx:null,an:{},on:{}};
function spkNode(key,stream){
  const tr=stream&&stream.getAudioTracks()[0];if(!tr)return null;
  let a=SPK.an[key];if(a&&a.tid===tr.id)return a;
  if(a){try{a.src.disconnect()}catch(e){}}
  try{
    if(!SPK.ctx)SPK.ctx=new (window.AudioContext||window.webkitAudioContext)();
    const src=SPK.ctx.createMediaStreamSource(new MediaStream([tr])),an=SPK.ctx.createAnalyser();
    an.fftSize=512;src.connect(an);
    return SPK.an[key]={tid:tr.id,src,an,buf:new Uint8Array(an.fftSize),hold:0};
  }catch(e){return null}
}
function spkPaint(){
  document.querySelectorAll("[data-uid]").forEach(n=>n.classList.toggle("speaking",!!SPK.on[n.dataset.uid]));
}
function spkTick(){
  if(!call){if(Object.keys(SPK.on).length){SPK.on={};SPK.an={};spkPaint()}return}
  if(SPK.ctx&&SPK.ctx.state==="suspended")SPK.ctx.resume().catch(()=>{});
  const thr=P("spk",5)/100,now=performance.now();let ch=false;
  for(const u of (S.calls[call.channel]||[])){
    const me=u.id===S.me.id;
    const stream=me?(call.atrack&&call.mic?call.stream:null):(call.peers[u.id]?call.peers[u.id].stream:null);
    const a=u.mic&&stream?spkNode(u.id,stream):null;
    let on=false;
    if(a){
      a.an.getByteTimeDomainData(a.buf);let s=0;
      for(let i=0;i<a.buf.length;i++){const v=(a.buf[i]-128)/128;s+=v*v}
      if(Math.sqrt(s/a.buf.length)>thr)a.hold=now+350;
      on=now<a.hold;
    }
    if(!!SPK.on[u.id]!==on){SPK.on[u.id]=on;ch=true}
  }
  if(ch)spkPaint();
}
setInterval(spkTick,100);

/* ---------- グループ ---------- */
function groupAv(cls){const a=el("div",(cls||"av xs")+" gav");a.innerHTML=ic("users",cls&&cls.includes("sm2")?17:13);return a}
function friendChecks(exclude){
  const wrap=el("div","fpick"),sel=new Set();
  const list=S.friends.filter(f=>!exclude.includes(f.name));
  if(!list.length){const p=el("p","muted");p.textContent="追加できるフレンドがいません。先に「フレンド追加」でフレンドを追加してください。";wrap.append(p)}
  list.forEach(f=>{
    const r=el("label","fprow"),c=el("input");c.type="checkbox";
    c.onchange=()=>{c.checked?sel.add(f.name):sel.delete(f.name)};
    r.append(c,avatar(f.name,"av xs"),document.createTextNode(f.name));wrap.append(r);
  });
  return {node:wrap,sel};
}
function newGroupModal(){
  const w=el("div","fld"),l=el("label"),ni=el("input");
  l.textContent="グループ名";ni.maxLength=30;ni.placeholder="例: なかよしグループ";w.append(l,ni);
  const l2=el("div","muted");l2.textContent="メンバーにするフレンドを選ぶ";
  const fc=friendChecks([]);
  const go=el("button","btn","グループを作る");
  go.onclick=async()=>{
    const name=ni.value.trim();if(!name){toast("グループ名を入れてください");return}
    const d=await T(()=>api("/api/groups","POST",{name,members:[...fc.sel]}));if(!d)return;
    closeModal();await load();S.homeTab="g";setView("dm",d.id);
  };
  modal("グループを作る",[w,l2,fc.node,go]);
}
function newDmModal(){
  if(!S.friends.length){toast("先に「フレンド追加」でフレンドを追加してください");return}
  const rows=S.friends.map(f=>{
    const r=el("div","frow"+(f.online?"":" off")),n=el("span","fn");n.textContent=f.name;
    r.append(statusAvatar(f.name,"av",f.online),n);r.onclick=()=>{closeModal();S.homeTab="d";openDm(f.name)};return r;
  });
  modal("DMを送る相手を選ぶ",rows);
}
function addGroupMembers(c){
  const fc=friendChecks(c.members.map(m=>m.name));
  const go=el("button","btn","追加する");
  go.onclick=async()=>{
    if(!fc.sel.size){toast("追加するフレンドを選んでください");return}
    const d=await T(()=>api(`/api/groups/${c.id}/add`,"POST",{names:[...fc.sel]}));
    if(d){closeModal();toast(d.added+"人を追加しました");await load()}
  };
  modal("メンバーを追加",[fc.node,go]);
}
function renameGroup(c){
  const w=el("div","fld"),l=el("label"),ni=el("input");l.textContent="グループ名";ni.value=c.name;ni.maxLength=30;w.append(l,ni);
  const go=el("button","btn","変更する");
  go.onclick=async()=>{if(await T(()=>api(`/api/groups/${c.id}/rename`,"POST",{name:ni.value.trim()}))){closeModal();await load();if(S.chan===c.id)openChan(c.id,true)}};
  modal("グループ名を変更",[w,go]);
}
async function leaveGroup(c){
  if(!await confirmBox(`「${c.name}」から抜けますか？`,"抜ける"))return;
  if(call&&call.channel===c.id)await leaveCall();
  if(await T(()=>api(`/api/groups/${c.id}/leave`,"POST"))){await load();setView("dm")}
}
function groupMenu(c){
  const lb=el("div","muted");lb.textContent="メンバー "+c.members.length+"人";
  const mem=c.members.map(m=>{
    const e=el("div","frow"),n=el("span","fn");n.textContent=m.name+(m.name===S.me.name?" (あなた)":"");
    e.append(statusAvatar(m.name,"av sm2",m.online),n);e.onclick=()=>{closeModal();profileModal(m.name)};return e;
  });
  modal(c.name,[lb,...mem,mitem("userplus","メンバーを追加",()=>addGroupMembers(c)),mitem("pencil","グループ名を変更",()=>renameGroup(c)),mitem("logout","グループを抜ける",()=>leaveGroup(c))]);
}
function headProfile(){const c=S.dms.find(x=>x.id===S.chan);if(!c)return;if(c.group)groupMenu(c);else profileModal(c.name)}
async function addFriend(){
  const f=await form("フレンドを追加",[{k:"code",label:"友達のフレンドコード"}],"追加する");
  if(!f||!f.code)return;
  const d=await T(()=>api("/api/friends/add","POST",{code:f.code}));
  if(d){toast(d.name+" さんとフレンドになりました");S.homeTab="d";openDm(d.name)}
}

function drawList(){
  const h=$("sidehead"),l=$("list");h.innerHTML="";l.innerHTML="";
  const badge=n=>{const b=el("span","badge");b.textContent=n;return b};
  const hint=t=>{const p=el("p","muted");p.textContent=t;l.append(p)};
  const chanRows=list=>list.forEach(c=>{
    const wrap=el("div");
    const d=el("div","ch"+(c.id===S.chan?" active":"")+(S.unread[c.id]?" unread":""));
    const lead=S.view==="dm"?(c.group?groupAv("av xs"):statusAvatar(c.name,"av xs",c.online)):ico(c.kind==="voice"?"speaker":"hash",18);
    const n=el("span","cn");n.textContent=c.name;d.append(lead,n);
    if(S.view==="dm"&&!c.group){
      const tg=c.blocked?"ブロック中":c.muted?"ミュート中":!c.friend?"フレンドではない":"";
      if(tg){const s=el("span","tg"+(c.blocked?" bad":""));s.textContent=tg;d.append(s)}
    }
    const cu=S.calls[c.id]||[];
    if(S.view==="dm"&&cu.length)d.append(ico("phone",15));
    if(S.ment[c.id])d.append(badge(S.ment[c.id]));
    if(S.view==="dm"&&!c.group)d.append(ibtn("more","",e=>{e.stopPropagation();friendMenu({id:c.uid,name:c.name,friend:c.friend,muted:c.muted,blocked:c.blocked})},"xb","メニュー"));
    if(c.group)d.append(ibtn("more","",e=>{e.stopPropagation();groupMenu(c)},"xb","グループ設定"));
    d.onclick=()=>openChan(c.id);wrap.append(d);
    if(c.kind==="voice")cu.forEach(u=>{
      const v=el("div","vu");v.dataset.uid=u.id;v.append(avatar(u.name,"av xs"));if(!u.mic)v.append(ico("micoff",13));
      const nn=el("span");nn.textContent=u.name;v.append(nn);wrap.append(v);
    });
    l.append(wrap);
  });
  if(S.view==="dm"){
    const t=el("span","t");t.textContent="ホーム";h.append(t);
    if(S.homeTab!=="g")S.homeTab="d";
    const acts=el("div","homeact");
    const ab=el("button","addbar sm",ic("userplus",15)+"<span>フレンド追加</span>");ab.onclick=addFriend;
    const nb=el("button","addbar sm",S.homeTab==="g"?ic("plus",15)+"<span>グループ作成</span>":ic("send",15)+"<span>新しいDM</span>");
    nb.onclick=S.homeTab==="g"?newGroupModal:newDmModal;
    acts.append(ab,nb);l.append(acts);
    const seg=el("div","seg");
    [["d","DM"],["g","グループ"]].forEach(([k,tx])=>{const b=el("button",S.homeTab===k?"on":"");b.textContent=tx;b.onclick=()=>{S.homeTab=k;drawList()};seg.append(b)});
    l.append(seg);
    const dms=S.dms.filter(c=>!c.group),gs=S.dms.filter(c=>c.group);
    if(S.homeTab==="g"){
      if(!gs.length)hint("まだグループがありません。「グループ作成」でフレンドを集めましょう。");
      chanRows(gs);
    }else{
      if(!dms.length)hint("DMはまだありません。「新しいDM」からフレンドにメッセージを送れます。");
      chanRows(dms);
    }
  }else{
    const s=curServer();if(!s)return;
    const t=el("span","t");t.textContent=s.name;h.append(t,ibtn("gear","",serverMenu,"ghost sq","サーバー設定"));
    chanRows(s.channels);
  }
  updateAnnBtn();updateTitle();
}
async function drawMembers(){
  const p=$("members");p.innerHTML="";
  if(S.view==="dm"){
    const c=S.dms.find(x=>x.id===S.chan);
    if(c&&c.group){
      const h=el("h4");h.textContent="メンバー  "+c.members.length;p.append(h);
      c.members.forEach(m=>{
        const e=el("div","mem"+(m.online?"":" off"));e.dataset.uid=m.id;e.append(statusAvatar(m.name,"av sm2",m.online));
        const i=el("div","mi"),n=el("span");n.textContent=m.name+(m.name===S.me.name?" (あなた)":"");i.append(n);e.append(i);
        e.onclick=()=>{closeDrawers();profileModal(m.name)};p.append(e);
      });
      p.append(ibtn("userplus","メンバーを追加",()=>addGroupMembers(c),"ghost"));
      return;
    }
    const h=el("h4");h.textContent="あなたのフレンドコード";p.append(h);
    const cd=el("div","code");cd.textContent=S.code;p.append(cd);
    const row=el("div","row");row.style.justifyContent="flex-start";
    row.append(ibtn("copy","コピー",()=>copyText(S.code),"ghost"),ibtn("gear","再発行",async()=>{if(await confirmBox("古いコードは使えなくなります。再発行しますか？","再発行"))T(()=>api("/api/friends/newcode","POST"))},"ghost"));
    p.append(row);
    const t=el("p","muted");t.textContent="このコードを友達に送ると、友達が「フレンド追加」に入力してフレンドになれます。";p.append(t);
    return;
  }
  const sid=S.view;let d;
  try{d=await api(`/api/servers/${sid}/members`)}catch(e){return}
  if(sid!==S.view)return;
  const h=el("h4");h.textContent="メンバー  "+d.members.length;p.append(h);
  d.members.forEach(m=>{
    const e=el("div","mem"+(m.online?"":" off"));e.append(statusAvatar(m.name,"av sm2",m.online));
    const i=el("div","mi"),n=el("span");n.textContent=m.name;
    if(m.owner)n.append(el("span","tag","オーナー"));
    i.append(n);
    if(m.roles.length){const r=el("small");r.textContent=m.roles.join(", ");i.append(r)}
    e.append(i);
    e.onclick=()=>{closeDrawers();profileModal(m.name)};
    p.append(e);
  });
}
async function openChan(cid,auto){
  const keepPos=!!auto&&S.chan===cid,prevTop=$("msgs").scrollTop,wasNear=msgsNearBottom();
  S.chan=cid;S.unread[cid]=0;S.ment[cid]=0;last=null;clearReply();
  if(!auto)closeDrawers();
  drawList();
  const isA=cid===S.announce;
  const c=isA?{name:"公式アナウンス",kind:"announce"}:chanList().find(x=>x.id===cid);
  const ht=$("headt");ht.innerHTML="";
  if(c){
    const lead=(isA||c.kind==="voice")?ico("speaker",19):(S.view==="dm"?(c.group?groupAv("av xs"):statusAvatar(c.name,"av xs",c.online)):ico("hash",19));
    const nm=el("span");nm.textContent=c.name;ht.append(lead,nm);
    if(c.group){const k=el("small","muted");k.textContent=c.members.length+"人";ht.append(k)}
  }
  $("profbtn").hidden=!(c&&S.view==="dm"&&!isA);
  $("profbtn").innerHTML=ic(c&&c.group?"gear":"user",20);$("profbtn").title=c&&c.group?"グループ設定":"プロフィール";
  $("sq").placeholder=(c?c.name:"")+"を検索";
  const ro=isA&&!S.owner;$("bar").hidden=ro;$("ronote").hidden=!ro;$("stampbox").hidden=true;
  updateCallBtn();
  if(S.view==="dm")drawMembers();
  if(curAu){curAu.pause();curAu=null}
  $("msgs").innerHTML="";
  const d=await T(()=>api(`/api/channels/${cid}/messages`));
  if(!d||S.chan!==cid)return;
  d.messages.forEach(addMsg);
  /* 読み返し中の自動更新 (再接続など) では今の位置を保つ。チャットを開いたときだけ一番下へ */
  if(keepPos&&!wasNear){$("msgs").scrollTo({top:prevTop,behavior:"instant"});S.stick=false;updDown()}
  else{S.stick=true;toBottom(false)}
}

/* ---------- 設定画面 (ディスコード風) ---------- */
const sRow=(title,desc,ctl)=>{
  const r=el("div","srow"),t=el("div","st"),b=el("b");b.textContent=title;t.append(b);
  if(desc){const s=el("small");s.textContent=desc;t.append(s)}
  r.append(t);if(ctl)r.append(ctl);return r;
};
const sHead=t=>{const h=el("h3","sh3");h.textContent=t;return h};
const sNote=(t,cls)=>{const n=el("div","snote "+(cls||""));n.textContent=t;return n};
function sSwitch(val,fn){const l=el("label","sw"),i=el("input");i.type="checkbox";i.checked=!!val;i.onchange=()=>fn(i.checked);l.append(i,el("span","knob"));return l}
function sSelect(opts,val,fn){const s=el("select");opts.forEach(([v,t])=>{const o=el("option");o.value=v;o.textContent=t;s.append(o)});s.value=val;s.onchange=()=>fn(s.value);return s}
function sRange(min,max,step,val,fn,fmt){
  const w=el("div","srange"),i=el("input"),o=el("span","muted");
  i.type="range";i.min=min;i.max=max;i.step=step;i.value=val;o.textContent=fmt(val);
  i.oninput=()=>{o.textContent=fmt(i.value);fn(Number(i.value))};w.append(i,o);return w;
}
const allCats=()=>S.owner?[...SET_CATS.slice(0,-1),["update","download","アップデート (管理者)"],SET_CATS[SET_CATS.length-1]]:SET_CATS;
const SET_CATS=[["acct","user","マイアカウント"],["prof","pencil","プロフィール"],["appear","globe","外観"],["chat","send","チャット"],
  ["notif","bell","通知"],["block","shield","ブロック・ミュート"],["voice","mic","音声・ビデオ"],["access","zoomin","アクセシビリティ"],["data","shield","データ・保存"],["about","gear","アプリについて"]];
function closeSettings(fromPop){
  const o=$("settings");if(o)o.remove();
  const id=setUi;setUi=0;if(id&&fromPop!==true)uiClosed(id);
}
function settingsModal(page){
  if($("settings"))closeSettings(true);
  const ov=el("div","set");ov.id="settings";ov.dataset.view="list";
  const nav=el("div","setnav"),body=el("div","setbody"),top=el("div","settop"),inner=el("div","setin");
  const ttl=el("div","setttl");ttl.textContent="設定";
  nav.append(ttl,ibtn("x","",closeSettings,"ghost sq mo setx","閉じる"));
  const back=ibtn("chev","",()=>{ov.dataset.view="list"},"ghost sq mo setback","戻る");
  const ct=el("b","setct");
  top.append(back,ct,ibtn("x","",closeSettings,"ghost sq","閉じる"));
  body.append(top,inner);
  const items={};
  const PAGES={
    acct(c){
      const card=el("div","pcard"),i=el("div","pci"),n=el("b");n.textContent=S.me.name;
      const s=el("small");s.textContent="フレンドコード: "+S.code;i.append(n,s);card.append(avatar(S.me.name,"av lg"),i);c.append(card);
      c.append(sRow("プロフィールを編集","名前・自己紹介・アイコン・バナーを変更",ibtn("pencil","編集",()=>{closeSettings();editProfileModal()},"btn")));
      c.append(sRow("フレンドコード",S.code,ibtn("copy","コピー",()=>copyText(S.code),"ghost")));
      c.append(sRow("フレンドコードを再発行","古いコードは使えなくなります",ibtn("gear","再発行",async()=>{
        if(await confirmBox("古いコードは使えなくなります。再発行しますか？","再発行"))T(async()=>{await api("/api/friends/newcode","POST");await load();show("acct")});
      },"ghost")));
      c.append(sRow("ログアウト","この端末からログアウトします",ibtn("logout","ログアウト",()=>{closeSettings();logout()},"danger")));
    },
    prof(c){
      c.append(sNote("アイコンは画像を選んだあと、写したい場所を動かして決められます。バナーとプロフィール背景は画像も動画も容量無制限です。"));
      c.append(sRow("プロフィールを編集","アイコン / 名前 / 自己紹介 / バナー / 背景",ibtn("pencil","編集する",()=>{closeSettings();editProfileModal()},"btn")));
      c.append(sRow("自分のプロフィールを見る","みんなに見える表示を確認",ibtn("user","見る",()=>{closeSettings();profileModal(S.me.name)},"ghost")));
    },
    appear(c){
      c.append(sHead("テーマ"));
      c.append(sRow("テーマ","明るさを選びます",sSelect([["system","システムに合わせる"],["dark","ダーク"],["light","ライト"]],localStorage.theme||"system",v=>{localStorage.theme=v;applyTheme()})));
      const sw=el("div","swatches");
      [["標準",""],["スカイ","#38b6ff"],["インディゴ","#6b7cff"],["パープル","#a855f7"],["ピンク","#ec4899"],["レッド","#ef4444"],["オレンジ","#f59e0b"],["グリーン","#22c55e"]].forEach(([nm,col])=>{
        const b=el("button","swt");b.title=nm;b.style.background=col||"var(--grad)";
        b.onclick=()=>{if(col)localStorage.accent=col;else localStorage.removeItem("accent");applyTheme()};sw.append(b);
      });
      const ci=el("input");ci.type="color";ci.value=localStorage.accent||"#6b7cff";ci.title="好きな色を選ぶ";
      ci.oninput=()=>{localStorage.accent=ci.value;applyTheme()};sw.append(ci);
      c.append(sRow("テーマカラー","好きな色を選べます",null));c.append(sw);
      c.append(sRow("文字をポップな太字にする","名前などを丸ゴシックで目立たせます",sSwitch(localStorage.pop!=="0",v=>{localStorage.pop=v?"1":"0";applyTheme()})));
    },
    chat(c){
      c.append(sHead("メッセージ"));
      c.append(sRow("Enterキーで送信","オフにすると Ctrl + Enter で送信します",sSwitch(P("enter",true),v=>setP("enter",v))));
      c.append(sRow("メッセージの文字サイズ","チャットの文字の大きさ",sRange(80,150,5,P("fs",100),v=>setP("fs",v),v=>v+"%")));
      c.append(sRow("コンパクト表示","メッセージの間隔をつめます",sSwitch(P("compact",false),v=>setP("compact",v))));
    },
    notif(c){
      c.append(sRow("通知の対象","どのメッセージで通知するか",sSelect([["mention","メンション・返信・DMのみ"],["all","すべてのメッセージ"],["off","通知しない"]],S.notify,v=>{S.notify=v;T(()=>api("/api/settings","POST",{notify_mode:v}))})));
      const st=()=>"Notification" in window?({granted:"許可済み",denied:"ブロック中 (ブラウザの設定で許可してください)",default:"未設定"})[Notification.permission]:"このブラウザは非対応です";
      const desc=sRow("ブラウザ通知","状態: "+st(),ibtn("bell","オンにする",async()=>{await enableNotifications();desc.querySelector("small").textContent="状態: "+st()},"btn"));
      c.append(desc);
      c.append(sRow("通知音を鳴らす","新しいメッセージが届いたとき",sSwitch(S.sound,v=>{S.sound=v;localStorage.sound=v?"1":"0"})));
      c.append(sNote(S.push?"Push通知が有効なので、アプリを閉じていても通知が届きます (iPhoneはホーム画面に追加してください)。":"サーバー側でPush通知が未設定のため、通知はこのアプリを開いている間だけ届きます。"));
    },
    block(c){
      const list=(title,arr,path,none)=>{
        c.append(sHead(title));
        if(!arr.length)c.append(sNote(none));
        arr.forEach(x=>c.append(sRow(x.name,"",ibtn("x","解除",async()=>{
          if(await T(()=>api(path,"POST",{id:x.id}))){await load();show("block")}
        },"ghost rm"))));
      };
      list("ブロック中のユーザー",S.blocked||[],"/api/blocks/remove","ブロックしているユーザーはいません。");
      list("通知をミュート中のユーザー",S.mutedList||[],"/api/mutes/remove","ミュートしているユーザーはいません。");
      c.append(sNote("ブロック: フレンドから外れ、お互いにDMやDM通話ができなくなります。ミュート: その人からの通知だけを止めます (メッセージは今までどおり届きます)。ユーザーは、ホームのDM一覧の「…」メニューから追加できます。"));
    },
    voice(c){
      c.append(sHead("入力"));
      const sel=el("select"),o0=el("option");o0.value="";o0.textContent="既定のマイク";sel.append(o0);
      if(navigator.mediaDevices&&navigator.mediaDevices.enumerateDevices)navigator.mediaDevices.enumerateDevices().then(ds=>{
        ds.filter(d=>d.kind==="audioinput"&&d.deviceId).forEach((d,i)=>{const o=el("option");o.value=d.deviceId;o.textContent=d.label||"マイク "+(i+1);sel.append(o)});
        sel.value=P("micId","");
      }).catch(()=>{});
      sel.onchange=()=>setP("micId",sel.value);
      c.append(sRow("マイク","次に通話へ入るときから反映されます",sel));
      c.append(sRow("発言の検出感度","小さいほど小さな声でも緑のリングが出ます",sRange(1,25,1,P("spk",5),v=>setP("spk",v),v=>String(v))));
      c.append(sHead("音声処理 (次の通話から)"));
      c.append(sRow("エコーを消す","スピーカーの音がマイクに入るのを防ぎます",sSwitch(P("ec",true),v=>setP("ec",v))));
      c.append(sRow("ノイズを抑える","キーボード音などを小さくします",sSwitch(P("ns",false),v=>setP("ns",v))));
      c.append(sRow("音量を自動で調整","声の大きさを自動でそろえます",sSwitch(P("agc",false),v=>setP("agc",v))));
      c.append(sNote("カメラ・画面共有・インカメラとアウトカメラの切り替えは、通話中のボタンから行えます。"));
    },
    access(c){
      c.append(sRow("アニメーションを減らす","動きや光る演出を止めます",sSwitch(P("reduce",false),v=>setP("reduce",v))));
      c.append(sRow("コントラストを強める","薄い文字を見やすくします",sSwitch(P("contrast",false),v=>setP("contrast",v))));
      c.append(sRow("ボタンを大きくする","タップしやすくします",sSwitch(P("big",false),v=>setP("big",v))));
    },
    data(c){
      c.append(sNote(S.persistent?"外部データベースに保存中です。アプリを更新(再デプロイ)しても、アカウント・メッセージ・アイコン・バナーは消えません。":"データがこのサーバーのディスクだけに保存されています。Renderなどで更新すると消えてしまうので、環境変数 DATABASE_URL に外部データベース(Neonなど)の接続URLを設定してください。",S.persistent?"ok":"warn"));
      c.append(sRow("この端末の設定をリセット","テーマ・文字サイズ・音声設定などを初期化します (アカウントは消えません)",ibtn("trash","リセット",async()=>{
        if(!await confirmBox("この端末の表示・音声設定を初期化しますか？","リセット"))return;
        ["prefs","theme","accent","pop","recent"].forEach(k=>localStorage.removeItem(k));applyTheme();applyPrefs();show("data");toast("設定をリセットしました");
      },"ghost")));
      c.append(sRow("アプリを再読み込み","表示がおかしいときに",ibtn("compress","再読み込み",()=>location.reload(),"ghost")));
    },
    update(c){
      const nextV=k=>{const m=/(\d+)\.(\d+)\.(\d+)/.exec(S.appver||"");let a=+m[1],b=+m[2],p=+m[3];if(k==="major"){a++;b=0;p=0}else if(k==="minor"){b++;p=0}else p++;return `v${a}.${String(b).padStart(2,"0")}.${p}`};
      c.append(sNote("管理者だけが使えます。新しい main.py を選んで「アップデートする」を押すと、サーバーが自動で入れ替わって再起動し、公式アナウンスにお知らせが届きます。入れ替える前に起動テストをして、動かないファイルは受け付けません。"));
      c.append(sRow("いまのバージョン",S.appver,null));
      const fi=el("input");fi.type="file";fi.accept=".py";fi.hidden=true;
      const fl=el("div","muted");fl.textContent="ファイルはまだ選ばれていません";
      fi.onchange=()=>{const f=fi.files[0];fl.textContent=f?f.name+" ("+(f.size/1024).toFixed(0)+" KB)":"ファイルはまだ選ばれていません"};
      const pick=el("button","ghost","ファイルを選ぶ");pick.onclick=()=>fi.click();
      c.append(sRow("main.py をアップロード","更新したいファイルを選びます",pick),fi,fl);
      const sel=sSelect([["patch","小さな修正 ("+S.appver+" → "+nextV("patch")+")"],["minor","機能の追加 ("+S.appver+" → "+nextV("minor")+")"],["major","大きな更新 ("+S.appver+" → "+nextV("major")+")"]],"patch",()=>{});
      c.append(sRow("バージョンの上げ方","",sel));
      const notes=el("textarea");notes.rows=4;notes.maxLength=1500;notes.placeholder="アップデートの内容 (公式アナウンスに載ります)\n例: ・グループ機能を追加 ・不具合を修正";
      notes.style.width="100%";notes.style.margin="12px 0";c.append(notes);
      const go=el("button","btn","アップデートする");
      go.onclick=async()=>{
        const f=fi.files[0];if(!f){toast("main.py を選んでください");return}
        if(!await confirmBox(`${nextV(sel.value)} に更新します。サーバーが数秒間再起動します。よろしいですか？`,"更新する"))return;
        go.disabled=true;go.textContent="起動テスト中… (数十秒かかることがあります)";
        const code=await f.text();
        const d=await T(()=>api("/api/admin/update","POST",{code,bump:sel.value,notes:notes.value.trim()}));
        if(!d){go.disabled=false;go.textContent="アップデートする";return}
        go.textContent="再起動中…";toast(d.version+" に更新します。自動で再読み込みします");
        let n=0;const iv=setInterval(async()=>{
          if(++n>60){clearInterval(iv);location.reload();return}
          try{const r=await fetch("/api/version",{cache:"no-store"});const j=await r.json();if(j.version===d.version&&j.v!==APP_V){clearInterval(iv);location.reload()}}catch(e){}
        },1500);
      };
      c.append(go);
      const hist=el("div");c.append(hist);
      api("/api/admin/update").then(d=>{
        if(!d.writable)c.prepend(sNote("サーバー上のファイルを書き換えられないため、この方法ではアップデートできません。","warn"));
        c.append(sNote(d.persistent?"アップロードしたコードは外部データベースにも保存されるので、再起動しても引き継がれます。GitHubなどに新しいコードをデプロイしたときは、そちらが優先されます。":"外部データベース(DATABASE_URL)が未設定のため、サーバーが作り直されると元のコードに戻ります。GitHubのmain.pyも一緒に更新してください。",d.persistent?"ok":"warn"));
        if(d.can_rollback)c.append(sRow("ひとつ前に戻す","更新後に不具合が出たとき",ibtn("compress","戻す",async()=>{
          if(!await confirmBox("ひとつ前のバージョンに戻しますか？ サーバーが再起動します。","戻す"))return;
          const r=await T(()=>api("/api/admin/rollback","POST"));if(r){toast(r.version+" に戻します");setTimeout(()=>location.reload(),6000)}
        },"danger")));
        if(d.log.length){const h=sHead("更新の履歴");c.append(h);d.log.forEach(x=>{
          const r=el("div","srow"),t=el("div","st"),b=el("b"),sm=el("small");
          b.textContent=x.v+"  ·  "+new Date(x.ts*1000).toLocaleString("ja-JP");sm.textContent=(x.notes||"")+(x.by?"  (by "+x.by+")":"");sm.style.whiteSpace="pre-wrap";
          t.append(b,sm);r.append(t);c.append(r);
        })}
      }).catch(()=>{});
    },
    about(c){
      c.append(sRow("Motto",(S.appver||"")+"  (build "+APP_V+")",null));
      c.append(sRow("アップデート","新しいバージョンが公開されているか確認します。あるときは画面の上に通知が出ます。",ibtn("refresh","確認",()=>checkUpdate(true),"ghost rm")));
      c.append(sRow("ホーム画面に追加","スマホではブラウザのメニューから「ホーム画面に追加」するとアプリのように使えます",null));
      c.append(sRow("キーボード","Enter: 送信 / Ctrl+V: 画像を貼り付け / Esc: 閉じる",null));
    }
  };
  function show(key){
    Object.entries(items).forEach(([k,b])=>b.classList.toggle("on",k===key));
    ov.dataset.view="detail";ov.dataset.page=key;
    ct.textContent=allCats().find(x=>x[0]===key)[2];
    inner.innerHTML="";PAGES[key](inner);body.scrollTop=0;
  }
  allCats().forEach(([k,icn,t])=>{const b=el("button","sitem",ic(icn,18)+`<span>${t}</span>`);b.onclick=()=>show(k);items[k]=b;nav.append(b)});
  const vv=el("div","setver");vv.textContent="Motto "+(S.appver||"");
  nav.append(el("div","ssp"),ibtn("logout","ログアウト",()=>{closeSettings();logout()},"sitem dng"),vv);
  ov.append(nav,body);document.body.append(ov);
  setUi=uiOpen(()=>{setUi=0;closeSettings(true)});
  if(!MOB()||page)show(page||"acct");
}
document.addEventListener("keydown",e=>{if(e.key==="Escape"&&$("settings")&&$("modal").hidden&&!$("lb"))closeSettings()});

/* ---------- 入力中の表示 ---------- */
const TY={};let tyLast=0;
function tyAdd(cid,name){(TY[cid]=TY[cid]||{})[name]=Date.now()+4500;tyDraw()}
function tyDraw(){
  const box=$("typing");if(!box)return;
  const now=Date.now(),o=TY[S.chan]||{},names=Object.keys(o).filter(n=>o[n]>now&&n!==(S.me&&S.me.name));
  box.innerHTML="";
  if(!names.length)return;
  const b=el("b"),t=el("span","tyt");
  b.textContent=names.length>2?"数人":names.join("、");t.textContent=" が入力中";
  box.append(b,t);
}
setInterval(tyDraw,500);
$("txt").addEventListener("input",()=>{
  if(!S.chan||!$("txt").value.trim())return;
  const now=Date.now();if(now-tyLast<2500)return;
  tyLast=now;wsend({type:"typing",channel:S.chan});
});

/* ---------- ボイスメッセージ (録音して送る) ---------- */
let rec=null;
const bvoice=ibtn("mic","",()=>toggleVoiceRec(),"ghost","ボイスメッセージを録音");bvoice.id="bvoice";
const bvc=ibtn("x","",()=>stopRec(false),"ghost","録音をやめる");bvc.id="bvc";bvc.hidden=true;
$("bclip").after(bvoice,bvc);
const mmss=ms=>{const s=Math.floor(ms/1000);return Math.floor(s/60)+":"+String(s%60).padStart(2,"0")};
function recUI(on){
  bvoice.classList.toggle("rec",on);bvoice.innerHTML=ic(on?"send":"mic",21);
  bvoice.title=on?"録音を送信":"ボイスメッセージを録音";bvc.hidden=!on;
  $("prog").textContent=on&&rec?`● 録音中 ${mmss(Date.now()-rec.t0)}  (マイクボタンで送信 / ✕で破棄)`:"";
}
async function toggleVoiceRec(){
  if(rec){stopRec(true);return}
  if(!S.chan||S.chan===S.announce)return;
  if(!window.MediaRecorder){toast("このブラウザは録音に対応していません");return}
  let st;
  try{st=await navigator.mediaDevices.getUserMedia({audio:AUDIO})}catch(e){toast("マイクを使えません。ブラウザで許可してください");return}
  const chunks=[],mr=new MediaRecorder(st);
  rec={mr,st,chunks,cid:S.chan,t0:Date.now(),cancel:false,tm:0};
  mr.ondataavailable=e=>{if(e.data&&e.data.size)chunks.push(e.data)};
  mr.onstop=async()=>{
    const r=rec;if(!r)return;
    clearInterval(r.tm);st.getTracks().forEach(t=>t.stop());
    const dur=Date.now()-r.t0;rec=null;recUI(false);
    if(r.cancel||!chunks.length)return;
    if(dur<600){toast("短すぎます。もう少し長く録音してください");return}
    const type=(mr.mimeType||"audio/webm").split(";")[0];
    const ext=type.includes("mp4")?"m4a":type.includes("ogg")?"ogg":"webm";
    try{
      await upload(`/api/channels/${r.cid}/upload?name=${encodeURIComponent("ボイスメッセージ."+ext)}&type=${encodeURIComponent(type)}`,new Blob(chunks,{type}),
        e=>{$("prog").textContent=`送信中 ${(e.loaded/e.total*100).toFixed(0)}%`});
    }catch(e){toast(e.message)}
    $("prog").textContent="";
  };
  mr.start();
  rec.tm=setInterval(()=>{if(!rec)return;recUI(true);if(Date.now()-rec.t0>10*60*1000)stopRec(true)},250);
  recUI(true);
}
function stopRec(send){if(!rec)return;rec.cancel=!send;try{rec.mr.stop()}catch(e){rec.st.getTracks().forEach(t=>t.stop());rec=null;recUI(false)}}

/* ---------- クリップボードから貼り付け (Ctrl+V / スマホはボタン) ---------- */
const pad2=n=>String(n).padStart(2,"0");
function clipName(f){
  if(f.name&&!/^image\.[a-z0-9]+$/i.test(f.name))return f.name;
  const d=new Date(),ext=((f.type||"image/png").split("/")[1]||"png").split("+")[0];
  return `スクリーンショット-${d.getFullYear()}${pad2(d.getMonth()+1)}${pad2(d.getDate())}-${pad2(d.getHours())}${pad2(d.getMinutes())}${pad2(d.getSeconds())}.${ext}`;
}
async function sendFiles(files,cid){
  for(const f of files){
    const name=clipName(f);
    try{
      await upload(`/api/channels/${cid}/upload?name=${encodeURIComponent(name)}&type=${encodeURIComponent(f.type||"application/octet-stream")}`,f,
        e=>{$("prog").textContent=`アップロード中 ${(e.loaded/e.total*100).toFixed(1)}%`});
    }catch(e){toast(e.message)}
  }
  $("prog").textContent="";
}
function pasteSend(files){
  if(!S.chan){return}
  if(S.chan===S.announce&&!S.owner){toast("ここには送信できません");return}
  const cid=S.chan,box=el("div","pastebox");
  files.forEach(f=>{
    if((f.type||"").startsWith("image/")){const i=el("img","pv");i.src=URL.createObjectURL(f);i.style.maxWidth="100%";i.style.maxHeight="260px";box.append(i)}
    else{const t=el("div","muted");t.textContent=clipName(f);box.append(t)}
  });
  const go=el("button","btn","送信する"),no=el("button","ghost","やめる");
  no.onclick=()=>closeModal();
  go.onclick=()=>{closeModal();sendFiles(files,cid)};
  const row=el("div","row");row.append(no,go);
  modal(files.length>1?"貼り付けた "+files.length+" 個を送る":"貼り付けたものを送る",[box,row]);
  setTimeout(()=>go.focus(),80);
}
document.addEventListener("paste",e=>{
  if(!S.me||!S.chan||$("settings")||!$("modal").hidden||document.querySelector(".crop"))return;
  const cd=e.clipboardData;if(!cd)return;
  let files=[...(cd.files||[])];
  if(!files.length)files=[...(cd.items||[])].filter(i=>i.kind==="file").map(i=>i.getAsFile()).filter(Boolean);
  if(!files.length)return;
  e.preventDefault();pasteSend(files);
});
async function pasteFromClipboard(){
  if(!navigator.clipboard||!navigator.clipboard.read){toast("このブラウザはクリップボードの読み取りに未対応です。クリップのボタンから画像を選んでください");return}
  try{
    const items=await navigator.clipboard.read(),files=[];
    for(const it of items){
      const ty=it.types.find(t=>t.startsWith("image/"));
      if(ty){const b=await it.getType(ty);files.push(new File([b],"image."+(ty.split("/")[1]||"png").split("+")[0],{type:ty}))}
    }
    if(files.length){pasteSend(files);return}
    const t=await navigator.clipboard.readText().catch(()=>"");
    if(t){$("txt").value+=t;$("txt").focus()}else toast("クリップボードに画像がありません");
  }catch(e){toast("クリップボードを読み取れませんでした。貼り付けを許可してください")}
}
const bpaste=ibtn("copy","",pasteFromClipboard,"ghost","クリップボードから貼り付け");bpaste.id="bpaste";
$("bclip").after(bpaste);

/* ---------- 一番下へボタン / スクロール ---------- */
S.stick=true;S.below=0;
function msgsNearBottom(){const m=$("msgs");return m.scrollHeight-m.scrollTop-m.clientHeight<80}
function updDown(){
  const b=$("bdown");if(!b)return;
  const far=!msgsNearBottom(),n=S.below||0,bd=b.querySelector(".bdn");if(!bd)return;
  b.hidden=!far;b.classList.toggle("new",n>0);
  bd.hidden=!n;bd.textContent=n>99?"99+":String(n);
}
function toBottom(smooth){
  const m=$("msgs");S.below=0;S.stick=true;
  m.scrollTo({top:m.scrollHeight,behavior:smooth?"smooth":"instant"});
  updDown();
}
$("msgs").addEventListener("scroll",()=>{S.stick=msgsNearBottom();if(S.stick)S.below=0;updDown()},{passive:true});
/* 画像・動画・埋め込みが後から読み込まれても、一番下を見ていたなら位置がずれないようにする */
["load","loadedmetadata"].forEach(t=>$("msgs").addEventListener(t,()=>{if(S.stick)toBottom(false)},true));

/* ---------- アップデート通知 ---------- */
const APP_V="__APP_VERSION__";
let updSnooze=0;
async function checkUpdate(manual){
  if(!manual&&Date.now()<updSnooze)return false;
  try{
    const r=await fetch("/api/version",{cache:"no-store"});
    if(!r.ok)throw new Error("x");
    const d=await r.json();
    if(d.v&&d.v!==APP_V){showUpdate();return true}
    if(manual)toast("最新のバージョンです");
  }catch(e){if(manual)toast("更新を確認できませんでした")}
  return false;
}
function showUpdate(){
  if($("upd"))return;
  const b=el("div");b.id="upd";
  const t=el("span");t.textContent="新しいバージョンがあります";
  const go=el("button","go");go.textContent="更新";go.onclick=()=>location.reload();
  const x=el("button","x",ic("x",16));x.title="あとで";x.onclick=()=>{updSnooze=Date.now()+30*60e3;b.remove()};
  b.append(t,go,x);document.body.append(b);
}
setInterval(()=>checkUpdate(),5*60e3);
document.addEventListener("visibilitychange",()=>{if(!document.hidden)checkUpdate()});

applyPrefs();
applyTheme();
initIcons();
if(S.token)boot();
</script>"""


# コードが変わる (= 新しくデプロイされる) と変わる。再起動しただけでは変わらない
APP_VERSION = hashlib.sha1(PAGE.encode("utf-8")).hexdigest()[:10]
_PAGE_OUT = PAGE.replace("__APP_VERSION__", APP_VERSION)


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(_PAGE_OUT, headers={"Cache-Control": "no-cache"})


@app.get("/api/version")
async def app_version():
    return JSONResponse({"v": APP_VERSION, "version": ver_str()}, headers={"Cache-Control": "no-store"})


if __name__ == "__main__":
    import uvicorn

    ssl = {}
    if Path("cert.pem").exists() and Path("key.pem").exists():
        ssl = {"ssl_certfile": "cert.pem", "ssl_keyfile": "key.pem"}
        print("cert.pem / key.pem を検出: https で起動します")
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8000")), **ssl)
