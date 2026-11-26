"""
MiniCord v3 -- Discord風チャット
FastAPI + WebSocket + WebRTC + SQLite

起動:
    pip install fastapi "uvicorn[standard]"
    python main.py

ブラウザ:
    http://localhost:8000

ロゴ画像:
    C:\\Users\\renge\\Downloads\\M.png
    起動時に読み込み、HTML内にBase64で埋め込みます。
"""

import base64
import hashlib
import hmac
import json
import re
import secrets
import sqlite3
import time
import traceback
import uuid
from pathlib import Path
from fastapi.responses import FileResponse

from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse

app = FastAPI()
@app.get("/M.png")
async def get_logo():
    return FileResponse(Path(__file__).parent / "M.png")

# ------------------------------------------------------------------
# ロゴ画像
# ------------------------------------------------------------------

LOGO_PATH = Path(__file__).parent / "M.png"

if not LOGO_PATH.is_file():
    raise FileNotFoundError(
        f"ロゴ画像が見つかりません: {LOGO_PATH}"
    )

LOGO_BASE64 = base64.b64encode(
    LOGO_PATH.read_bytes()
).decode("ascii")
# ------------------------------------------------------------------
# 基本設定
# ------------------------------------------------------------------

UPLOADS = Path("uploads")
UPLOADS.mkdir(exist_ok=True)

MAX_STAMP = 5 * 1024 * 1024
MAX_CALL = 8

# ------------------------------------------------------------------
# DB
# ------------------------------------------------------------------

db = sqlite3.connect("minicord.db", check_same_thread=False)

db.executescript("""
create table if not exists users(
    id integer primary key autoincrement,
    name text unique,
    salt blob,
    pw blob,
    created real
);

create table if not exists sessions(
    token text primary key,
    user_id integer,
    created real
);

create table if not exists servers(
    id integer primary key autoincrement,
    name text,
    owner_id integer,
    invite text unique
);

create table if not exists members(
    server_id integer,
    user_id integer,
    primary key(server_id, user_id)
);

create table if not exists roles(
    id integer primary key autoincrement,
    server_id integer,
    name text,
    perms integer,
    is_default integer default 0
);

create table if not exists member_roles(
    server_id integer,
    user_id integer,
    role_id integer,
    primary key(user_id, role_id)
);

create table if not exists channels(
    id integer primary key autoincrement,
    server_id integer,
    name text
);

create table if not exists dm_members(
    channel_id integer,
    user_id integer,
    primary key(channel_id, user_id)
);

create table if not exists messages(
    id integer primary key autoincrement,
    channel_id integer,
    user_id integer,
    text text,
    file_id text,
    file_name text,
    file_type text,
    ts real,
    mentions text default '[]'
);

create table if not exists reactions(
    message_id integer,
    user_id integer,
    emoji text,
    primary key(message_id, user_id, emoji)
);

create table if not exists friends(
    user_id integer,
    friend_id integer,
    primary key(user_id, friend_id)
);

create table if not exists stamps(
    id integer primary key autoincrement,
    name text,
    file_id text,
    mime text,
    owner_id integer,
    hidden integer default 0
);

create index if not exists idx_msg on messages(channel_id, id);
""")


def _has_col(t, c):
    return any(r[1] == c for r in db.execute(f"pragma table_info({t})"))


for _t, _c, _d in [
    ("messages", "stamp_id", "integer"),
    ("channels", "kind", "text default 'text'"),
    ("users", "friend_code", "text"),
]:
    if not _has_col(_t, _c):
        db.execute(f"alter table {_t} add column {_c} {_d}")

db.commit()

# ------------------------------------------------------------------
# 権限
# ------------------------------------------------------------------

P_SEND, P_FILES, P_REACT, P_CHANNELS, P_ROLES, P_ADMIN = 1, 2, 4, 8, 16, 32
ALL = 63
DEFAULT = P_SEND | P_FILES | P_REACT

PERM_NAMES = {
    "send": P_SEND,
    "files": P_FILES,
    "react": P_REACT,
    "channels": P_CHANNELS,
    "roles": P_ROLES,
    "admin": P_ADMIN,
}


def server_users(sid):
    return [
        r[0] for r in db.execute(
            "select user_id from members where server_id=?", (sid,)
        )
    ]


def server_perms(uid, sid):
    s = db.execute(
        "select owner_id from servers where id=?", (sid,)
    ).fetchone()

    if not s or not db.execute(
        "select 1 from members where server_id=? and user_id=?",
        (sid, uid)
    ).fetchone():
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
    ch = db.execute(
        "select server_id from channels where id=?", (cid,)
    ).fetchone()

    if not ch:
        return None

    if ch[0] is None:
        ok = db.execute(
            "select 1 from dm_members where channel_id=? and user_id=?",
            (cid, uid)
        ).fetchone()
        return DEFAULT if ok else None

    if not db.execute(
        "select 1 from members where server_id=? and user_id=?",
        (ch[0], uid)
    ).fetchone():
        return None

    return server_perms(uid, ch[0])


def require(uid, cid, bit=0):
    p = chan_info(uid, cid)

    if p is None:
        raise HTTPException(403, "このチャンネルにアクセスできません")

    if bit and not p & bit:
        raise HTTPException(403, "その操作の権限がありません")


def channel_users(cid):
    ch = db.execute(
        "select server_id from channels where id=?", (cid,)
    ).fetchone()

    if not ch:
        return []

    if ch[0] is None:
        return [
            r[0] for r in db.execute(
                "select user_id from dm_members where channel_id=?", (cid,)
            )
        ]

    return server_users(ch[0])


# ------------------------------------------------------------------
# リアルタイム配信
# ------------------------------------------------------------------

class Hub:
    def __init__(self):
        self.users: dict[int, set[WebSocket]] = {}
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
    return [
        r[0] for r in db.execute(
            "select friend_id from friends where user_id=?", (uid,)
        )
    ]


async def presence(uid, online):
    await push(
        friend_ids(uid),
        {"type": "presence", "id": uid, "online": online}
    )


# ------------------------------------------------------------------
# 通話ルーム
# ------------------------------------------------------------------

def room_users(cid):
    return [
        {
            "id": i,
            "name": v["name"],
            "mic": v["mic"],
            "cam": v["cam"]
        }
        for i, v in hub.rooms.get(cid, {}).items()
    ]


async def broadcast_call(cid):
    await push(
        channel_users(cid),
        {
            "type": "call_state",
            "channel": cid,
            "users": room_users(cid)
        }
    )


async def call_leave(uid, cid):
    room = hub.rooms.get(cid)

    if room and room.pop(uid, None) is not None:
        if not room:
            del hub.rooms[cid]

        await broadcast_call(cid)


async def call_join(ws, uid, name, cid):
    require(uid, cid)

    ch = db.execute(
        "select server_id,kind from channels where id=?", (cid,)
    ).fetchone()

    if ch[0] is not None and ch[1] != "voice":
        raise HTTPException(400, "このチャンネルでは通話できません")

    for ocid in [c for c, r in hub.rooms.items() if uid in r]:
        await call_leave(uid, ocid)

    room = hub.rooms.setdefault(cid, {})

    if len(room) >= MAX_CALL:
        raise HTTPException(
            400,
            f"通話は最大{MAX_CALL}人までです"
        )

    peers = [
        {"id": i, "name": v["name"]}
        for i, v in room.items()
    ]

    room[uid] = {
        "ws": ws,
        "name": name,
        "mic": True,
        "cam": False
    }

    await send_ws(
        ws,
        {
            "type": "call_peers",
            "channel": cid,
            "peers": peers
        }
    )

    await broadcast_call(cid)


# ------------------------------------------------------------------
# 認証
# ------------------------------------------------------------------

def hash_pw(pw: str, salt: bytes) -> bytes:
    return hashlib.scrypt(
        pw.encode(),
        salt=salt,
        n=2**14,
        r=8,
        p=1,
        dklen=32
    )


def new_session(uid):
    t = secrets.token_urlsafe(32)

    db.execute(
        "insert into sessions values(?,?,?)",
        (t, uid, time.time())
    )

    db.commit()
    return t


def user_from_token(t):
    r = db.execute(
        "select u.id,u.name from sessions s "
        "join users u on u.id=s.user_id where s.token=?",
        (t,)
    ).fetchone()

    return {"id": r[0], "name": r[1]} if r else None


def current_user(request: Request):
    h = request.headers.get("authorization", "")

    t = (
        h[7:] if h.startswith("Bearer ")
        else request.query_params.get("token", "")
    )

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
        raise HTTPException(
            400,
            "名前は2〜20文字で、空白と@は使えません"
        )

    if len(pw) < 6:
        raise HTTPException(
            400,
            "パスワードは6文字以上にしてください"
        )

    if db.execute(
        "select 1 from users where name=?", (name,)
    ).fetchone():
        raise HTTPException(400, "その名前は使われています")

    salt = secrets.token_bytes(16)

    cur = db.execute(
        "insert into users(name,salt,pw,created,friend_code) "
        "values(?,?,?,?,?)",
        (
            name,
            salt,
            hash_pw(pw, salt),
            time.time(),
            new_friend_code()
        ),
    )

    db.commit()

    return {"token": new_session(cur.lastrowid)}


@app.post("/api/login")
async def login(b: dict):
    name = str(b.get("name", "")).strip()

    r = db.execute(
        "select id,salt,pw from users where name=?", (name,)
    ).fetchone()

    if not r or not hmac.compare_digest(
        hash_pw(str(b.get("password", "")), r[1]),
        r[2]
    ):
        raise HTTPException(
            401,
            "名前またはパスワードが違います"
        )

    return {"token": new_session(r[0])}


@app.post("/api/logout")
async def logout(request: Request, u=Depends(current_user)):
    h = request.headers.get("authorization", "")

    db.execute(
        "delete from sessions where token=?", (h[7:],)
    )

    db.commit()
    return {"ok": True}


@app.get("/api/me")
async def me(u=Depends(current_user)):
    uid = u["id"]
    servers = []

    for sid, name, owner, invite in db.execute(
        "select s.id,s.name,s.owner_id,s.invite "
        "from servers s join members m on m.server_id=s.id "
        "where m.user_id=? order by s.id",
        (uid,)
    ).fetchall():

        chans = [
            {"id": i, "name": n, "kind": k or "text"}
            for i, n, k in db.execute(
                "select id,name,kind from channels "
                "where server_id=? order by id",
                (sid,)
            )
        ]

        servers.append({
            "id": sid,
            "name": name,
            "owner": owner == uid,
            "invite": invite,
            "channels": chans
        })

    dms = [
        {"id": cid, "name": n}
        for cid, n in db.execute(
            "select c.id,(select u2.name from dm_members d2 "
            "join users u2 on u2.id=d2.user_id "
            "where d2.channel_id=c.id and d2.user_id!=?) "
            "from channels c join dm_members d on d.channel_id=c.id "
            "where c.server_id is null and d.user_id=? order by c.id",
            (uid, uid)
        )
    ]

    code = db.execute(
        "select friend_code from users where id=?", (uid,)
    ).fetchone()[0]

    if not code:
        code = new_friend_code()

        db.execute(
            "update users set friend_code=? where id=?",
            (code, uid)
        )

        db.commit()

    friends = [
        {
            "id": i,
            "name": n,
            "online": bool(hub.users.get(i))
        }
        for i, n in db.execute(
            "select u.id,u.name from friends f "
            "join users u on u.id=f.friend_id "
            "where f.user_id=? order by u.name",
            (uid,)
        )
    ]

    calls = {}

    for cid in list(hub.rooms):
        if hub.rooms.get(cid) and chan_info(uid, cid) is not None:
            calls[str(cid)] = room_users(cid)

    return {
        "user": u,
        "servers": servers,
        "dms": dms,
        "friend_code": code,
        "friends": friends,
        "calls": calls
    }


# ------------------------------------------------------------------
# フレンド
# ------------------------------------------------------------------

@app.post("/api/friends/add")
async def add_friend(b: dict, u=Depends(current_user)):
    code = str(b.get("code", "")).strip()

    if not code:
        raise HTTPException(
            400,
            "フレンドコードを入力してください"
        )

    r = db.execute(
        "select id,name from users where friend_code=?",
        (code,)
    ).fetchone()

    if not r:
        raise HTTPException(
            404,
            "フレンドコードが見つかりません"
        )

    if r[0] == u["id"]:
        raise HTTPException(
            400,
            "自分自身は追加できません"
        )

    db.execute(
        "insert or ignore into friends values(?,?)",
        (u["id"], r[0])
    )

    db.execute(
        "insert or ignore into friends values(?,?)",
        (r[0], u["id"])
    )

    db.commit()

    await push(
        [u["id"], r[0]],
        {"type": "refresh"}
    )

    return {"name": r[1]}


@app.post("/api/friends/remove")
async def remove_friend(b: dict, u=Depends(current_user)):
    fid = int(b.get("id", 0))

    db.execute(
        "delete from friends where "
        "(user_id=? and friend_id=?) or "
        "(user_id=? and friend_id=?)",
        (u["id"], fid, fid, u["id"])
    )

    db.commit()

    await push(
        [u["id"], fid],
        {"type": "refresh"}
    )

    return {"ok": True}


@app.post("/api/friends/newcode")
async def new_code(u=Depends(current_user)):
    code = new_friend_code()

    db.execute(
        "update users set friend_code=? where id=?",
        (code, u["id"])
    )

    db.commit()

    await push(
        [u["id"]],
        {"type": "refresh"}
    )

    return {"code": code}


# ------------------------------------------------------------------
# サーバー / チャンネル / ロール
# ------------------------------------------------------------------

@app.post("/api/servers")
async def create_server(b: dict, u=Depends(current_user)):
    name = str(b.get("name", "")).strip()[:30]

    if not name:
        raise HTTPException(
            400,
            "サーバー名を入力してください"
        )

    sid = db.execute(
        "insert into servers(name,owner_id,invite) values(?,?,?)",
        (name, u["id"], secrets.token_urlsafe(6))
    ).lastrowid

    db.execute(
        "insert into roles(server_id,name,perms,is_default) "
        "values(?,?,?,1)",
        (sid, "@everyone", DEFAULT)
    )

    db.execute(
        "insert into roles(server_id,name,perms) values(?,?,?)",
        (sid, "Admin", ALL)
    )

    db.execute(
        "insert into members values(?,?)",
        (sid, u["id"])
    )

    db.execute(
        "insert into channels(server_id,name,kind) "
        "values(?,?,'text')",
        (sid, "general")
    )

    db.execute(
        "insert into channels(server_id,name,kind) "
        "values(?,?,'voice')",
        (sid, "ボイス")
    )

    db.commit()

    return {"id": sid}


@app.post("/api/servers/join")
async def join_server(b: dict, u=Depends(current_user)):
    r = db.execute(
        "select id from servers where invite=?",
        (str(b.get("invite", "")).strip(),)
    ).fetchone()

    if not r:
        raise HTTPException(
            404,
            "招待コードが見つかりません"
        )

    db.execute(
        "insert or ignore into members values(?,?)",
        (r[0], u["id"])
    )

    db.commit()

    await push(
        server_users(r[0]),
        {"type": "refresh"}
    )

    return {"id": r[0]}


@app.get("/api/servers/{sid}/members")
async def list_members(sid: int, u=Depends(current_user)):
    s = db.execute(
        "select owner_id from servers where id=?", (sid,)
    ).fetchone()

    if not s or not db.execute(
        "select 1 from members where server_id=? and user_id=?",
        (sid, u["id"])
    ).fetchone():
        raise HTTPException(
            403,
            "このサーバーのメンバーではありません"
        )

    members = []

    for uid, name in db.execute(
        "select u.id,u.name from members m "
        "join users u on u.id=m.user_id "
        "where m.server_id=? order by u.name",
        (sid,)
    ).fetchall():

        roles = [
            r[0] for r in db.execute(
                "select r.name from member_roles mr "
                "join roles r on r.id=mr.role_id "
                "where mr.server_id=? and mr.user_id=?",
                (sid, uid)
            )
        ]

        members.append({
            "id": uid,
            "name": name,
            "owner": uid == s[0],
            "roles": roles,
            "online": bool(hub.users.get(uid))
        })

    return {"members": members}


@app.post("/api/servers/{sid}/channels")
async def create_channel(sid: int, b: dict, u=Depends(current_user)):
    if not server_perms(u["id"], sid) & P_CHANNELS:
        raise HTTPException(
            403,
            "チャンネル管理の権限がありません"
        )

    name = re.sub(
        r"\s+",
        "-",
        str(b.get("name", "")).strip()
    )[:30]

    if not name:
        raise HTTPException(
            400,
            "チャンネル名を入力してください"
        )

    kind = "voice" if b.get("kind") == "voice" else "text"

    cid = db.execute(
        "insert into channels(server_id,name,kind) values(?,?,?)",
        (sid, name, kind)
    ).lastrowid

    db.commit()

    await push(
        server_users(sid),
        {"type": "refresh"}
    )

    return {"id": cid}


@app.post("/api/servers/{sid}/roles")
async def create_role(sid: int, b: dict, u=Depends(current_user)):
    mine = server_perms(u["id"], sid)

    if not mine & P_ROLES:
        raise HTTPException(
            403,
            "ロール管理の権限がありません"
        )

    name = str(b.get("name", "")).strip()[:30]

    if not name:
        raise HTTPException(
            400,
            "ロール名を入力してください"
        )

    perms = 0

    for p in b.get("perms", []):
        perms |= PERM_NAMES.get(str(p), 0)

    if perms & ~mine:
        raise HTTPException(
            403,
            "自分が持っていない権限は付与できません"
        )

    if db.execute(
        "select 1 from roles where server_id=? and name=?",
        (sid, name)
    ).fetchone():
        raise HTTPException(
            400,
            "同じ名前のロールがあります"
        )

    db.execute(
        "insert into roles(server_id,name,perms) values(?,?,?)",
        (sid, name, perms)
    )

    db.commit()

    await push(
        server_users(sid),
        {"type": "refresh"}
    )

    return {"ok": True}


@app.post("/api/servers/{sid}/assign")
async def assign_role(sid: int, b: dict, u=Depends(current_user)):
    mine = server_perms(u["id"], sid)

    if not mine & P_ROLES:
        raise HTTPException(
            403,
            "ロール管理の権限がありません"
        )

    t = db.execute(
        "select u.id from users u "
        "join members m on m.user_id=u.id "
        "where u.name=? and m.server_id=?",
        (str(b.get("user", "")), sid)
    ).fetchone()

    role = db.execute(
        "select id,perms,is_default from roles "
        "where server_id=? and name=?",
        (sid, str(b.get("role", "")))
    ).fetchone()

    if not t:
        raise HTTPException(
            404,
            "そのユーザーはこのサーバーにいません"
        )

    if not role or role[2]:
        raise HTTPException(
            404,
            "ロールが見つかりません"
        )

    if role[1] & ~mine:
        raise HTTPException(
            403,
            "自分が持っていない権限のロールは付与できません"
        )

    if b.get("remove"):
        db.execute(
            "delete from member_roles where user_id=? and role_id=?",
            (t[0], role[0])
        )
    else:
        db.execute(
            "insert or ignore into member_roles values(?,?,?)",
            (sid, t[0], role[0])
        )

    db.commit()

    await push(
        server_users(sid),
        {"type": "refresh"}
    )

    return {"ok": True}


# ------------------------------------------------------------------
# DM
# ------------------------------------------------------------------

@app.post("/api/dm")
async def open_dm(b: dict, u=Depends(current_user)):
    t = db.execute(
        "select id from users where name=?",
        (str(b.get("name", "")).strip(),)
    ).fetchone()

    if not t:
        raise HTTPException(
            404,
            "そのユーザーは存在しません"
        )

    if t[0] == u["id"]:
        raise HTTPException(
            400,
            "自分にはDMを送れません"
        )

    r = db.execute(
        "select c.id from channels c where c.server_id is null "
        "and exists(select 1 from dm_members "
        "where channel_id=c.id and user_id=?) "
        "and exists(select 1 from dm_members "
        "where channel_id=c.id and user_id=?)",
        (u["id"], t[0])
    ).fetchone()

    if r:
        return {"id": r[0]}

    cid = db.execute(
        "insert into channels(server_id,name,kind) "
        "values(NULL,'dm','dm')"
    ).lastrowid

    db.executemany(
        "insert into dm_members values(?,?)",
        [(cid, u["id"]), (cid, t[0])]
    )

    db.commit()

    await push(
        [u["id"], t[0]],
        {"type": "refresh"}
    )

    return {"id": cid}


# ------------------------------------------------------------------
# スタンプ
# ------------------------------------------------------------------

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


@app.get("/api/stamps")
async def list_stamps(u=Depends(current_user)):
    rows = db.execute(
        "select id,name,owner_id from stamps "
        "where hidden=0 order by id"
    ).fetchall()

    return {
        "stamps": [
            {
                "id": r[0],
                "name": r[1],
                "mine": r[2] == u["id"]
            }
            for r in rows
        ]
    }


@app.post("/api/stamps")
async def add_stamp(
    request: Request,
    name: str = "stamp",
    u=Depends(current_user)
):
    fid = uuid.uuid4().hex
    path = UPLOADS / fid
    size, head = 0, b""

    try:
        with open(path, "wb") as f:
            async for chunk in request.stream():
                size += len(chunk)

                if size > MAX_STAMP:
                    raise HTTPException(
                        413,
                        "スタンプ画像は5MBまでです"
                    )

                if len(head) < 16:
                    head += chunk[:16 - len(head)]

                f.write(chunk)

        mime = sniff_image(head)

        if not mime:
            raise HTTPException(
                400,
                "png / jpg / gif / webp の画像を選んでください"
            )

    except BaseException:
        path.unlink(missing_ok=True)
        raise

    db.execute(
        "insert into stamps(name,file_id,mime,owner_id) "
        "values(?,?,?,?)",
        (
            name.strip()[:20] or "stamp",
            fid,
            mime,
            u["id"]
        )
    )

    db.commit()

    await push(
        list(hub.users.keys()),
        {"type": "stamps"}
    )

    return {"ok": True}


@app.delete("/api/stamps/{sid}")
async def delete_stamp(sid: int, u=Depends(current_user)):
    cur = db.execute(
        "update stamps set hidden=1 where id=? and owner_id=?",
        (sid, u["id"])
    )

    db.commit()

    if cur.rowcount == 0:
        raise HTTPException(
            403,
            "自分のスタンプだけ削除できます"
        )

    await push(
        list(hub.users.keys()),
        {"type": "stamps"}
    )

    return {"ok": True}


@app.get("/api/stamps/{sid}/img")
async def stamp_img(sid: int, u=Depends(current_user)):
    r = db.execute(
        "select file_id,mime from stamps where id=?",
        (sid,)
    ).fetchone()

    if not r:
        raise HTTPException(404)

    return FileResponse(
        UPLOADS / r[0],
        media_type=r[1],
        headers={"X-Content-Type-Options": "nosniff"}
    )


# ------------------------------------------------------------------
# メッセージ
# ------------------------------------------------------------------

MSG_SQL = (
    "select m.id,m.channel_id,u.name,m.text,m.file_id,m.file_name,"
    "m.file_type,m.ts,m.mentions,m.stamp_id "
    "from messages m join users u on u.id=m.user_id "
)


def reactions_of(mid):
    d: dict[str, list[str]] = {}

    for emoji, name in db.execute(
        "select r.emoji,u.name from reactions r "
        "join users u on u.id=r.user_id "
        "where r.message_id=? order by r.rowid",
        (mid,)
    ):
        d.setdefault(emoji, []).append(name)

    return d


def row_to_msg(r):
    return {
        "id": r[0],
        "channel": r[1],
        "user": r[2],
        "text": r[3],
        "file_id": r[4],
        "file_name": r[5],
        "file_type": r[6],
        "ts": r[7],
        "mentions": json.loads(r[8]),
        "stamp": r[9],
        "reactions": reactions_of(r[0])
    }


def find_mentions(cid, text):
    names = set(re.findall(r"@([^\s@]+)", text))
    ids = channel_users(cid)

    if not names or not ids:
        return []

    rows = db.execute(
        f"select name from users where id in "
        f"({','.join('?' * len(ids))})",
        ids
    ).fetchall()

    valid = {r[0] for r in rows}

    return sorted(n for n in names if n in valid)


async def post_message(uid, cid, text="", file=None, stamp=None):
    ments = find_mentions(cid, text)

    cur = db.execute(
        "insert into messages(channel_id,user_id,text,file_id,file_name,"
        "file_type,ts,mentions,stamp_id) values(?,?,?,?,?,?,?,?,?)",
        (
            cid,
            uid,
            text,
            *(file or (None, None, None)),
            time.time(),
            json.dumps(ments),
            stamp
        )
    )

    db.commit()

    msg = row_to_msg(
        db.execute(
            MSG_SQL + "where m.id=?",
            (cur.lastrowid,)
        ).fetchone()
    )

    await push(
        channel_users(cid),
        {"type": "message", "message": msg}
    )


async def toggle_reaction(uid, mid, emoji):
    r = db.execute(
        "select channel_id from messages where id=?",
        (mid,)
    ).fetchone()

    if not r:
        raise HTTPException(
            404,
            "メッセージが見つかりません"
        )

    require(uid, r[0], P_REACT)

    if not emoji.strip():
        return

    cur = db.execute(
        "delete from reactions where message_id=? "
        "and user_id=? and emoji=?",
        (mid, uid, emoji)
    )

    if cur.rowcount == 0:
        db.execute(
            "insert into reactions values(?,?,?)",
            (mid, uid, emoji)
        )

    db.commit()

    await push(
        channel_users(r[0]),
        {
            "type": "reactions",
            "channel": r[0],
            "message_id": mid,
            "reactions": reactions_of(mid)
        }
    )


@app.get("/api/channels/{cid}/messages")
async def get_messages(cid: int, u=Depends(current_user)):
    require(u["id"], cid)

    rows = db.execute(
        MSG_SQL + "where m.channel_id=? order by m.id desc limit 100",
        (cid,)
    ).fetchall()

    return {
        "messages": [
            row_to_msg(r) for r in reversed(rows)
        ]
    }


@app.post("/api/channels/{cid}/upload")
async def upload(
    cid: int,
    request: Request,
    name: str = "file",
    type: str = "application/octet-stream",
    u=Depends(current_user)
):
    require(u["id"], cid, P_FILES)

    fid = uuid.uuid4().hex
    path = UPLOADS / fid

    try:
        with open(path, "wb") as f:
            async for chunk in request.stream():
                f.write(chunk)
    except BaseException:
        path.unlink(missing_ok=True)
        raise

    await post_message(
        u["id"],
        cid,
        "",
        (fid, name[:200], type[:100])
    )

    return {"ok": True}


@app.get("/files/{fid}")
async def get_file(fid: str, u=Depends(current_user)):
    r = db.execute(
        "select channel_id,file_name,file_type "
        "from messages where file_id=?",
        (fid,)
    ).fetchone()

    if not r or chan_info(u["id"], r[0]) is None:
        raise HTTPException(404)

    ftype = r[2] or ""

    safe = (
        ftype.startswith(("video/", "audio/"))
        or ftype in (
            "image/png",
            "image/jpeg",
            "image/gif",
            "image/webp"
        )
    )

    return FileResponse(
        UPLOADS / fid,
        media_type=ftype if safe else "application/octet-stream",
        filename=r[1],
        content_disposition_type="inline" if safe else "attachment",
        headers={"X-Content-Type-Options": "nosniff"}
    )


# ------------------------------------------------------------------
# WebSocket
# ------------------------------------------------------------------

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
                        await post_message(uid, cid, text)

                elif t == "stamp":
                    cid = int(d["channel"])
                    sid = int(d["stamp_id"])

                    require(uid, cid, P_SEND)

                    if not db.execute(
                        "select 1 from stamps where id=?",
                        (sid,)
                    ).fetchone():
                        raise HTTPException(
                            404,
                            "スタンプが見つかりません"
                        )

                    await post_message(
                        uid,
                        cid,
                        "",
                        None,
                        sid
                    )

                elif t == "react":
                    await toggle_reaction(
                        uid,
                        int(d["message_id"]),
                        str(d["emoji"])[:16]
                    )

                elif t == "call_join":
                    await call_join(
                        ws,
                        uid,
                        u["name"],
                        int(d["channel"])
                    )

                elif t == "call_leave":
                    await call_leave(
                        uid,
                        int(d["channel"])
                    )

                elif t == "call_media":
                    cid = next(
                        (
                            c for c, r in hub.rooms.items()
                            if r.get(uid, {}).get("ws") is ws
                        ),
                        None
                    )

                    if cid is not None:
                        hub.rooms[cid][uid]["mic"] = bool(
                            d.get("mic", True)
                        )

                        hub.rooms[cid][uid]["cam"] = bool(
                            d.get("cam", False)
                        )

                        await broadcast_call(cid)

                elif t == "rtc":
                    cid = int(d["channel"])
                    to = int(d["to"])

                    room = hub.rooms.get(cid, {})

                    if uid in room and to in room:
                        await send_ws(
                            room[to]["ws"],
                            {
                                "type": "rtc",
                                "from": uid,
                                "name": u["name"],
                                "channel": cid,
                                "data": d["data"]
                            }
                        )

            except HTTPException as e:
                await send_ws(
                    ws,
                    {
                        "type": "error",
                        "message": e.detail
                    }
                )

            except (ValueError, KeyError, TypeError):
                pass

            except WebSocketDisconnect:
                raise

            except Exception:
                traceback.print_exc()

    except WebSocketDisconnect:
        pass

    finally:
        for cid in [
            c for c, r in hub.rooms.items()
            if r.get(uid, {}).get("ws") is ws
        ]:
            await call_leave(uid, cid)

        conns = hub.users.get(uid)

        if conns is not None:
            conns.discard(ws)

            if not conns:
                del hub.users[uid]
                await presence(uid, False)


# ------------------------------------------------------------------
# 画面
# ------------------------------------------------------------------

PAGE = r"""<!doctype html>
<meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>MiniCord</title>

<style>
*{box-sizing:border-box}
[hidden]{display:none!important}
body{margin:0;height:100vh;font-family:sans-serif;background:#313338;color:#dbdee1}
button{border:0;border-radius:4px;padding:8px 12px;background:#5865f2;color:#fff;cursor:pointer}
input{padding:8px;border:0;border-radius:4px;background:#1e1f22;color:#fff}
#auth{height:100vh;display:flex;align-items:center;justify-content:center;background:#1e1f22}
#authbox{width:320px;display:flex;flex-direction:column;gap:10px;background:#313338;padding:24px;border-radius:8px}
#logo{width:120px;height:120px;object-fit:contain;margin:0 auto}
#aerr{color:#f23f43;min-height:1.2em}
#app{display:flex;height:100vh}
#rail{width:64px;background:#1e1f22;display:flex;flex-direction:column;gap:8px;padding:8px;overflow-y:auto}
.rb{width:48px;height:48px;border-radius:50%;background:#313338;padding:0;font-size:16px}
.rb.active{border-radius:14px;background:#5865f2}
#side{width:230px;background:#2b2d31;display:flex;flex-direction:column}
#sidehead{padding:10px;font-weight:bold;display:flex;flex-wrap:wrap;gap:4px;align-items:center;border-bottom:1px solid #1e1f22}
.sm{padding:3px 6px;font-size:11px;background:#404249}
#list{flex:1;overflow-y:auto;padding:6px}
.lbl{font-size:11px;color:#949ba4;margin:8px 4px 2px;text-transform:uppercase}
.ch{padding:6px 8px;border-radius:4px;cursor:pointer;color:#949ba4;display:flex;justify-content:space-between;align-items:center}
.ch:hover,.ch.active{background:#404249;color:#fff}
.ch.unread{color:#fff;font-weight:bold}
.vu{font-size:12px;color:#949ba4;padding:1px 0 1px 26px}
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:#80848e;margin-right:6px}
.dot.on{background:#23a559}
.x{background:none;padding:0 4px;color:#949ba4;font-size:12px}
.badge{background:#f23f43;color:#fff;border-radius:10px;padding:0 6px;font-size:12px}
#meb{padding:8px;background:#232428;display:flex;justify-content:space-between;align-items:center}
#main{flex:1;display:flex;flex-direction:column;min-width:0}
#ring{background:#23a559;color:#fff;padding:8px;display:flex;flex-direction:column;gap:6px}
#ring button{background:#1a7a42;margin-left:8px}
#head{padding:10px 12px;font-weight:bold;border-bottom:1px solid #1e1f22;display:flex;justify-content:space-between;align-items:center}
#call{background:#1e1f22;padding:8px 12px;border-bottom:1px solid #111}
#tiles{display:flex;flex-wrap:wrap;gap:8px;margin:6px 0}
.tile{position:relative;width:200px;height:130px;background:#2b2d31;border-radius:8px;overflow:hidden;display:flex;align-items:center;justify-content:center}
.tile video{position:absolute;top:0;left:0;width:100%;height:100%;object-fit:cover;display:none}
.tile.cam video{display:block}
.tile.cam .av{display:none}
.av{width:56px;height:56px;border-radius:50%;background:#5865f2;display:flex;align-items:center;justify-content:center;font-size:24px;color:#fff}
.nm{position:absolute;left:6px;bottom:4px;font-size:12px;background:#000a;padding:1px 6px;border-radius:4px}
.tile.off .nm{color:#f23f43}
#ctl{display:flex;gap:8px}
#ctl button{background:#404249}
#ctl button.offb{background:#f23f43}
#ctl button.danger{background:#da373c}
#msgs{flex:1;overflow-y:auto;padding:12px}
.m{position:relative;padding:4px 8px;border-radius:4px;margin-bottom:6px}
.m:hover{background:#2e3035}
.mme{background:#444037;border-left:3px solid #f0b232}
.u{font-weight:bold;color:#fff}
.t{font-size:11px;color:#949ba4;margin-left:6px}
.m video,.m img{max-width:420px;max-height:300px;display:block;margin-top:4px}
.m img.stamp{max-width:160px;max-height:160px}
.mention{background:#3c4270;color:#c9cdfb;border-radius:3px;padding:0 2px}
.mention.me{background:#5865f2;color:#fff}
.rx{display:flex;gap:4px;flex-wrap:wrap;margin-top:4px}
.chip{background:#2b2d31;border:1px solid #404249;padding:2px 8px;font-size:13px}
.chip.mine{border-color:#5865f2;background:#3c4270}
.addrx{position:absolute;top:2px;right:8px;padding:2px 8px;background:#404249;opacity:.4}
.m:hover .addrx{opacity:1}
.picker{position:absolute;top:28px;right:8px;background:#1e1f22;padding:4px;border-radius:6px;z-index:5}
.picker button{background:none;font-size:18px;padding:2px 6px}
#prog{font-size:12px;padding:0 12px;height:16px}
#stampbox{display:flex;flex-wrap:wrap;gap:6px;padding:8px;background:#2b2d31;max-height:200px;overflow-y:auto;border-top:1px solid #1e1f22}
.st{position:relative}
.st img{width:64px;height:64px;object-fit:contain;cursor:pointer;border-radius:4px}
.st img:hover{background:#404249}
.st button{position:absolute;top:0;right:0;padding:0 4px;font-size:11px;background:#da373c}
.stadd{background:#404249;width:64px;height:64px;font-size:12px}
#bar{display:flex;gap:6px;padding:10px;background:#383a40}
#txt{flex:1;background:#404249}
#members{width:210px;background:#2b2d31;padding:10px;overflow-y:auto}
.mem{padding:4px;border-radius:4px;cursor:pointer}
.mem:hover{background:#404249}
.mem small{display:block;color:#949ba4;font-size:11px}
.code{background:#1e1f22;padding:8px;border-radius:4px;word-break:break-all;margin:6px 0;user-select:all;font-family:monospace}
@media(max-width:800px){#members{display:none}#side{width:160px}}
</style>

<div id=auth>
 <div id=authbox>
  <img id=logo src="data:image/png;base64,LOGO_PLACEHOLDER" alt="MiniCord">
  <h2 style="margin:0;text-align:center">MiniCord</h2>
  <input id=an placeholder="ユーザー名">
  <input id=ap type=password placeholder="パスワード(6文字以上)">
  <div id=aerr></div>
  <button onclick="doAuth('login')">ログイン</button>
  <button onclick="doAuth('register')" style="background:#4e5058">新規登録</button>
 </div>
</div>

<div id=app hidden>
 <div id=rail></div>
 <div id=side>
  <div id=sidehead></div>
  <div id=list></div>
  <div id=meb>
   <span id=me></span>
   <button class=sm onclick="logout()">ログアウト</button>
  </div>
 </div>

 <div id=main>
  <div id=ring hidden></div>
  <div id=head>
   <span id=headt></span>
   <button id=callbtn hidden onclick="callBtn()"></button>
  </div>

  <div id=call hidden>
   <div id=calltitle></div>
   <div id=tiles></div>
   <div id=ctl>
    <button id=micb onclick="toggleMic()"></button>
    <button id=camb onclick="toggleCam()"></button>
    <button class=danger onclick="leaveCall()">退出</button>
   </div>
  </div>

  <div id=msgs></div>
  <div id=prog></div>
  <div id=stampbox hidden></div>

  <div id=bar>
   <button onclick="f.click()" title="ファイル">📁</button>
   <input type=file id=f hidden>
   <button onclick="toggleStamps()" title="スタンプ" style="background:#4e5058">🏷</button>
   <input type=file id=sf accept="image/png,image/jpeg,image/gif,image/webp" hidden>
   <input id=txt placeholder="メッセージを送信 (@名前 でメンション)">
   <button onclick="sendMsg()">送信</button>
  </div>
 </div>

 <div id=members></div>
</div>

<script>
const $=id=>document.getElementById(id);
const EMOJI=["👍","❤️","😂","🎉","😮","😢","🖕"];

const AUDIO={
 echoCancellation:true,
 noiseSuppression:false,
 autoGainControl:false
};

const ICE={
 iceServers:[
  {urls:"stun:stun.l.google.com:19302"}
 ]
};

const S={
 token:localStorage.token||"",
 me:null,
 servers:[],
 dms:[],
 friends:[],
 code:"",
 calls:{},
 stamps:[],
 view:"dm",
 chan:null,
 unread:{},
 ment:{},
 ws:null
};

let call=null;

async function api(path,method="GET",body){
 const r=await fetch(path,{
  method,
  headers:{
   "Content-Type":"application/json",
   ...(S.token?{Authorization:"Bearer "+S.token}:{})
  },
  body:body?JSON.stringify(body):undefined
 });

 const d=await r.json().catch(()=>({}));

 if(!r.ok){
  throw new Error(
   typeof d.detail==="string"?d.detail:r.statusText
  );
 }

 return d;
}

const T=async f=>{
 try{return await f()}
 catch(e){alert(e.message)}
};

const wsend=o=>{
 if(S.ws&&S.ws.readyState===1){
  S.ws.send(JSON.stringify(o));
  return true;
 }
 return false;
};

/* ログイン */

async function doAuth(kind){
 $("aerr").textContent="";

 try{
  const d=await api(
   "/api/"+kind,
   "POST",
   {
    name:$("an").value.trim(),
    password:$("ap").value
   }
  );

  S.token=d.token;
  localStorage.token=d.token;

  await boot();

 }catch(e){
  $("aerr").textContent=e.message;
 }
}

async function logout(){
 try{await leaveCall()}
 catch(e){}

 try{await api("/api/logout","POST")}
 catch(e){}

 localStorage.removeItem("token");
 S.token="";

 if(S.ws)S.ws.close();

 location.reload();
}

async function load(){
 const d=await api("/api/me");

 S.me=d.user;
 S.servers=d.servers;
 S.dms=d.dms;
 S.friends=d.friends;
 S.code=d.friend_code;
 S.calls=d.calls||{};

 if(
  S.view!=="dm"&&
  !S.servers.find(s=>s.id===S.view)
 ){
  S.view="dm";
 }

 $("me").textContent=S.me.name;

 drawRail();
 drawList();
 drawMembers();
 drawRing();
 updateCallBtn();
}

async function boot(){
 try{
  await load();
 }catch(e){
  localStorage.removeItem("token");
  S.token="";
  $("auth").hidden=false;
  $("app").hidden=true;
  return;
 }

 $("auth").hidden=true;
 $("app").hidden=false;

 connect();
 loadStamps();
 setView("dm");
}

/* WebSocket */

function connect(){
 const old=S.ws;

 const w=new WebSocket(
  `${location.protocol==="https:"?"wss":"ws"}://${location.host}/ws?token=${S.token}`
 );

 S.ws=w;

 if(old)old.close();

 w.onopen=()=>{
  if(S.chan)openChan(S.chan);
 };

 w.onclose=()=>{
  if(S.ws===w&&S.token){
   if(call)leaveCall();
   setTimeout(connect,2000);
  }
 };

 w.onmessage=onEvent;
}

function onEvent(e){
 const d=JSON.parse(e.data);

 if(d.type==="message"){
  const m=d.message;

  if(m.channel===S.chan){
   addMsg(m);
   $("msgs").scrollTop=1e9;
  }else{
   S.unread[m.channel]=1;

   const inServer=S.servers.some(
    s=>s.channels.some(c=>c.id===m.channel)
   );

   if(!inServer||m.mentions.includes(S.me.name)){
    S.ment[m.channel]=(S.ment[m.channel]||0)+1;
   }

   drawList();
  }

 }else if(d.type==="reactions"){
  const el=document.querySelector(
   `[data-mid="${d.message_id}"]`
  );

  if(el)renderReactions(el,d.reactions);

 }else if(d.type==="refresh"){
  load().catch(()=>{});

 }else if(d.type==="presence"){
  const f=S.friends.find(x=>x.id===d.id);

  if(f){
   f.online=d.online;
   if(S.view==="dm")drawList();
  }

  if(S.view!=="dm")drawMembers();

 }else if(d.type==="stamps"){
  loadStamps();

 }else if(d.type==="call_peers"){
  onPeers(d);

 }else if(d.type==="call_state"){
  onCallState(d);

 }else if(d.type==="rtc"){
  onRtc(d).catch(console.error);

 }else if(d.type==="error"){
  if(call&&!call.joined)leaveCall(true);
  alert(d.message);
 }
}

/* 画面 */

const curServer=()=>S.servers.find(s=>s.id===S.view);

const chanList=()=>(
 S.view==="dm"
 ?S.dms
 :(curServer()?curServer().channels:[])
);

const chanName=cid=>{
 const d=S.dms.find(c=>c.id===cid);

 if(d)return d.name;

 for(const s of S.servers){
  const c=s.channels.find(c=>c.id===cid);
  if(c)return c.name;
 }

 return "";
};

function callable(cid){
 if(!cid)return false;

 if(S.dms.some(c=>c.id===cid))return true;

 return S.servers.some(
  s=>s.channels.some(
   c=>c.id===cid&&c.kind==="voice"
  )
 );
}

function setView(v){
 S.view=v;

 drawRail();
 drawList();
 drawMembers();

 const l=chanList();

 if(l.length){
  openChan(l[0].id);
 }else{
  S.chan=null;
  $("msgs").innerHTML="";
  $("headt").textContent=
   v==="dm"
   ?"ホーム — フレンドを選ぶとDMが開きます"
   :"";

  updateCallBtn();
 }
}

function drawRail(){
 const r=$("rail");
 r.innerHTML="";

 const mk=(label,title,fn,active)=>{
  const b=document.createElement("button");
  b.textContent=label;
  b.title=title;
  b.className="rb"+(active?" active":"");
  b.onclick=fn;
  r.append(b);
 };

 mk(
  "🏠",
  "ホーム (フレンド / DM)",
  ()=>setView("dm"),
  S.view==="dm"
 );

 S.servers.forEach(s=>{
  mk(
  "",
  "ホーム (フレンド / DM)",
  ()=>setView("dm"),
  S.view==="dm"
 );

 const homeBtn = r.lastElementChild;
 const logo = document.createElement("img");
 logo.src = "/M.png";
 logo.style.width = "24px";
 logo.style.height = "24px";
 logo.style.objectFit = "contain";
 homeBtn.appendChild(logo);
 });

 mk("＋","サーバーを作成",newServer);
 mk("🔗","招待コードで参加",joinServer);
}

function drawList(){
 const h=$("sidehead");
 const l=$("list");

 h.innerHTML="";
 l.innerHTML="";

 const btn=(t,fn)=>{
  const b=document.createElement("button");
  b.textContent=t;
  b.className="sm";
  b.onclick=fn;
  return b;
 };

 const lbl=t=>{
  const x=document.createElement("div");
  x.className="lbl";
  x.textContent=t;
  l.append(x);
 };

 if(S.view==="dm"){
  h.append(
   "ホーム ",
   btn("＋フレンド追加",addFriend)
  );

  lbl("フレンド — "+S.friends.length);

  S.friends.forEach(f=>{
   const d=document.createElement("div");
   d.className="ch";

   const n=document.createElement("span");
   const dot=document.createElement("span");

   dot.className="dot"+(f.online?" on":"");

   n.append(dot,f.name);
   d.append(n);

   const x=document.createElement("button");

   x.className="x";
   x.textContent="✕";
   x.title="フレンドを削除";

   x.onclick=ev=>{
    ev.stopPropagation();

    if(confirm(f.name+" をフレンドから削除しますか?")){
     T(()=>api(
      "/api/friends/remove",
      "POST",
      {id:f.id}
     ));
    }
   };

   d.append(x);
   d.onclick=()=>openDm(f.name);

   l.append(d);
  });

  lbl("ダイレクトメッセージ");

 }else{
  const s=curServer();

  if(!s)return;

  h.append(
   s.name+" ",
   btn("招待",()=>prompt(
    "招待コード (友達に送ってください)",
    s.invite
   )),
   btn("＋ch",()=>newChannel("text")),
   btn("＋🔊",()=>newChannel("voice")),
   btn("ロール作成",newRole),
   btn("ロール付与",assignRole)
  );
 }

 chanList().forEach(c=>{
  const wrap=document.createElement("div");
  const d=document.createElement("div");

  d.className=
   "ch"+
   (c.id===S.chan?" active":"")+
   (S.unread[c.id]?" unread":"");

  const inCall=S.calls[c.id]&&S.calls[c.id].length;

  const n=document.createElement("span");

  n.textContent=
   (S.view==="dm"?"@ ":(c.kind==="voice"?"🔊 ":"# "))+
   c.name+
   (S.view==="dm"&&inCall?" 📞":"");

  d.append(n);

  if(S.ment[c.id]){
   const b=document.createElement("span");
   b.className="badge";
   b.textContent=S.ment[c.id];
   d.append(b);
  }

  d.onclick=()=>openChan(c.id);
  wrap.append(d);

  if(c.kind==="voice"){
   (S.calls[c.id]||[]).forEach(u=>{
    const v=document.createElement("div");
    v.className="vu";
    v.textContent="· "+(u.mic?"":"🔇 ")+u.name;
    wrap.append(v);
   });
  }

  l.append(wrap);
 });
}

async function drawMembers(){
 const p=$("members");
 p.innerHTML="";

 if(S.view==="dm"){
  p.append(
   Object.assign(
    document.createElement("b"),
    {textContent:"あなたのフレンドコード"}
   )
  );

  const c=document.createElement("div");
  c.className="code";
  c.textContent=S.code;
  p.append(c);

  const cp=document.createElement("button");
  cp.className="sm";
  cp.textContent="コピー";

  cp.onclick=()=>{
   (
    navigator.clipboard
    ?navigator.clipboard.writeText(S.code)
    :Promise.reject()
   )
   .then(()=>alert("コピーしました"))
   .catch(()=>prompt("コードをコピーしてください",S.code));
  };

  const nw=document.createElement("button");
  nw.className="sm";
  nw.textContent="再発行";
  nw.style.marginLeft="4px";

  nw.onclick=()=>{
   if(confirm("古いコードは使えなくなります。再発行しますか?")){
    T(()=>api("/api/friends/newcode","POST"));
   }
  };

  p.append(cp,nw);

  const t=document.createElement("p");

  t.style.cssText="font-size:12px;color:#949ba4";

  t.textContent=
   "このコードを友達に送ると、友達が「＋フレンド追加」に入力してフレンドになれます。";

  p.append(t);
  return;
 }

 const sid=S.view;
 let d;

 try{
  d=await api(`/api/servers/${sid}/members`);
 }catch(e){
  return;
 }

 if(sid!==S.view)return;

 p.append(
  Object.assign(
   document.createElement("b"),
   {textContent:"メンバー "+d.members.length}
  )
 );

 d.members.forEach(m=>{
  const e=document.createElement("div");
  e.className="mem";

  const dot=document.createElement("span");
  dot.className="dot"+(m.online?" on":"");

  e.append(
   dot,
   (m.owner?"管理者 ":"")+m.name
  );

  if(m.roles.length){
   const r=document.createElement("small");
   r.textContent=m.roles.join(", ");
   e.append(r);
  }

  if(m.id!==S.me.id){
   e.onclick=()=>openDm(m.name);
  }

  p.append(e);
 });
}

async function openChan(cid){
 S.chan=cid;
 S.unread[cid]=0;
 S.ment[cid]=0;

 drawList();

 const c=chanList().find(x=>x.id===cid);

 $("headt").textContent=
  (S.view==="dm"?"@ ":(c&&c.kind==="voice"?"🔊 ":"# "))+
  (c?c.name:"");

 updateCallBtn();

 $("msgs").innerHTML="";

 const d=await T(
  ()=>api(`/api/channels/${cid}/messages`)
 );

 if(!d||S.chan!==cid)return;

 d.messages.forEach(addMsg);

 $("msgs").scrollTop=1e9;
}

/* メッセージ */

function renderText(el,text){
 text.split(/(@[^\s@]+)/).forEach(p=>{
  if(p.startsWith("@")&&p.length>1){
   const s=document.createElement("span");

   s.className=
    "mention"+
    (p.slice(1)===S.me.name?" me":"");

   s.textContent=p;
   el.append(s);
  }else{
   el.append(p);
  }
 });
}

function addMsg(m){
 const d=document.createElement("div");

 d.className=
  "m"+
  (m.mentions.includes(S.me.name)?" mme":"");

 d.dataset.mid=m.id;

 const hd=document.createElement("div");

 const u=document.createElement("span");
 u.className="u";
 u.textContent=m.user;

 const t=document.createElement("span");
 t.className="t";
 t.textContent=new Date(m.ts*1000).toLocaleString();

 hd.append(u,t);
 d.append(hd);

 if(m.text){
  const p=document.createElement("div");
  renderText(p,m.text);
  d.append(p);
 }

 if(m.stamp){
  const i=document.createElement("img");

  i.className="stamp";
  i.alt="スタンプ";
  i.src=`/api/stamps/${m.stamp}/img?token=${S.token}`;

  d.append(i);
 }

 if(m.file_id){
  const url=`/files/${m.file_id}?token=${S.token}`;
  const ty=m.file_type||"";
  let e;

  if(ty.startsWith("video/")){
   e=document.createElement("video");
   e.controls=true;
   e.preload="metadata";
   e.src=url;
  }else if(ty.startsWith("image/")){
   e=document.createElement("img");
   e.src=url;
  }else{
   e=document.createElement("a");
   e.href=url;
   e.textContent="📄 "+m.file_name;
   e.style.color="#00a8fc";
  }

  d.append(e);
 }

 const rx=document.createElement("div");
 rx.className="rx";
 d.append(rx);

 const add=document.createElement("button");

 add.className="addrx";
 add.textContent="☺";

 add.onclick=()=>{
  const ex=d.querySelector(".picker");

  if(ex){
   ex.remove();
   return;
  }

  const pk=document.createElement("span");
  pk.className="picker";

  EMOJI.forEach(e=>{
   const b=document.createElement("button");
   b.textContent=e;

   b.onclick=()=>{
    react(m.id,e);
    pk.remove();
   };

   pk.append(b);
  });

  add.after(pk);
 };

 d.append(add);

 $("msgs").append(d);

 renderReactions(d,m.reactions);
}

function renderReactions(d,r){
 const rx=d.querySelector(".rx");
 rx.innerHTML="";

 for(const [e,names] of Object.entries(r)){
  const b=document.createElement("button");

  b.className=
   "chip"+
   (names.includes(S.me.name)?" mine":"");

  b.textContent=`${e} ${names.length}`;
  b.title=names.join(", ");

  b.onclick=()=>react(Number(d.dataset.mid),e);

  rx.append(b);
 }
}

const react=(mid,emoji)=>wsend({
 type:"react",
 message_id:mid,
 emoji
});

function sendMsg(){
 const v=$("txt").value.trim();

 if(!v||!S.chan)return;

 if(wsend({
  type:"send",
  channel:S.chan,
  text:v
 })){
  $("txt").value="";
 }
}

$("txt").addEventListener(
 "keydown",
 e=>{
  if(e.key==="Enter"&&!e.isComposing){
   sendMsg();
  }
 }
);

$("ap").addEventListener(
 "keydown",
 e=>{
  if(e.key==="Enter")doAuth("login");
 }
);

$("f").onchange=()=>{
 const file=$("f").files[0];

 if(!file||!S.chan)return;

 const x=new XMLHttpRequest();

 x.open(
  "POST",
  `/api/channels/${S.chan}/upload?name=${encodeURIComponent(file.name)}&type=${encodeURIComponent(file.type||"application/octet-stream")}`
 );

 x.setRequestHeader(
  "Authorization",
  "Bearer "+S.token
 );

 x.upload.onprogress=e=>{
  $("prog").textContent=
   `アップロード中 ${(e.loaded/e.total*100).toFixed(1)}%`;
 };

 x.onload=()=>{
  $("prog").textContent="";
  $("f").value="";

  if(x.status>=300){
   try{
    alert(JSON.parse(x.responseText).detail);
   }catch(e){
    alert("アップロードに失敗しました");
   }
  }
 };

 x.onerror=()=>{
  $("prog").textContent="";
  alert("アップロードに失敗しました");
 };

 x.send(file);
};

/* スタンプ */

async function loadStamps(){
 const d=await api("/api/stamps").catch(()=>null);

 if(d){
  S.stamps=d.stamps;
  drawStamps();
 }
}

function toggleStamps(){
 $("stampbox").hidden=!$("stampbox").hidden;
}

function drawStamps(){
 const b=$("stampbox");
 b.innerHTML="";

 S.stamps.forEach(s=>{
  const w=document.createElement("div");
  w.className="st";

  const i=document.createElement("img");

  i.src=`/api/stamps/${s.id}/img?token=${S.token}`;
  i.title=s.name;

  i.onclick=()=>{
   if(S.chan){
    wsend({
     type:"stamp",
     channel:S.chan,
     stamp_id:s.id
    });
   }

   b.hidden=true;
  };

  w.append(i);

  if(s.mine){
   const x=document.createElement("button");

   x.textContent="×";
   x.title="削除";

   x.onclick=()=>{
    if(confirm("このスタンプを削除しますか?")){
     T(()=>api("/api/stamps/"+s.id,"DELETE"));
    }
   };

   w.append(x);
  }

  b.append(w);
 });

 const add=document.createElement("button");

 add.className="stadd";
 add.textContent="＋ 追加";
 add.onclick=()=>$("sf").click();

 b.append(add);
}

$("sf").onchange=()=>{
 const f=$("sf").files[0];

 if(!f)return;

 const name=
  prompt(
   "スタンプの名前",
   f.name.replace(/\.[^.]+$/,"")
  )||"stamp";

 const x=new XMLHttpRequest();

 x.open(
  "POST",
  `/api/stamps?name=${encodeURIComponent(name)}`
 );

 x.setRequestHeader(
  "Authorization",
  "Bearer "+S.token
 );

 x.onload=()=>{
  $("sf").value="";

  if(x.status>=300){
   try{
    alert(JSON.parse(x.responseText).detail);
   }catch(e){
    alert("追加に失敗しました");
   }
  }
 };

 x.onerror=()=>alert("追加に失敗しました");

 x.send(f);
};

/* 通話 */

const rtc=(to,data)=>wsend({
 type:"rtc",
 channel:call.channel,
 to,
 data
});

async function joinCall(cid){
 if(call)await leaveCall();

 let stream;

 try{
  stream=await navigator.mediaDevices.getUserMedia({
   audio:AUDIO
  });
 }catch(e){
  alert(
   "マイクを使えません: "+e.message+
   "\n(httpsまたはlocalhostで開く必要があります)"
  );
  return;
 }

 call={
  channel:cid,
  stream,
  atrack:stream.getAudioTracks()[0],
  vtrack:null,
  preview:null,
  mic:true,
  cam:false,
  peers:{},
  joined:false
 };

 if(!wsend({
  type:"call_join",
  channel:cid
 })){
  leaveCall(true);
  return;
 }

 drawCall();
}

async function leaveCall(local){
 if(!call)return;

 if(!local){
  wsend({
   type:"call_leave",
   channel:call.channel
  });
 }

 Object.values(call.peers).forEach(
  p=>p.pc.close()
 );

 call.stream.getTracks().forEach(
  t=>t.stop()
 );

 if(call.vtrack)call.vtrack.stop();

 call=null;

 drawCall();
 drawList();
 drawRing();
}

function newPeer(id,name){
 const pc=new RTCPeerConnection(ICE);

 const p={
  id,
  name,
  pc,
  stream:new MediaStream(),
  pending:[],
  vsender:null
 };

 pc.onicecandidate=e=>{
  if(e.candidate&&call){
   rtc(id,{candidate:e.candidate});
  }
 };

 pc.ontrack=e=>{
  p.stream.addTrack(e.track);
  drawCall();
 };

 call.peers[id]=p;

 return p;
}

async function flush(p){
 for(const c of p.pending.splice(0)){
  await p.pc.addIceCandidate(c).catch(()=>{});
 }
}

async function callPeer(id,name){
 const p=newPeer(id,name);
 const pc=p.pc;

 pc.addTrack(
  call.atrack,
  call.stream
 );

 const vt=pc.addTransceiver(
  "video",
  {direction:"sendrecv"}
 );

 p.vsender=vt.sender;

 if(call.vtrack){
  await vt.sender.replaceTrack(call.vtrack);
 }

 await pc.setLocalDescription(
  await pc.createOffer()
 );

 rtc(id,{
  description:pc.localDescription
 });
}

async function onPeers(d){
 if(!call||call.channel!==d.channel)return;

 call.joined=true;

 for(const p of d.peers){
  await callPeer(p.id,p.name).catch(console.error);
 }
}

async function onRtc(d){
 if(!call||d.channel!==call.channel)return;

 const x=d.data;
 let p=call.peers[d.from];

 if(x.description){
  if(x.description.type==="offer"){
   if(p)p.pc.close();

   p=newPeer(d.from,d.name);

   await p.pc.setRemoteDescription(x.description);

   for(const t of p.pc.getTransceivers()){
    const kind=t.receiver.track.kind;

    t.direction="sendrecv";

    await t.sender.replaceTrack(
     kind==="audio"?call.atrack:call.vtrack
    );

    if(kind==="video")p.vsender=t.sender;
   }

   await flush(p);

   await p.pc.setLocalDescription(
    await p.pc.createAnswer()
   );

   rtc(d.from,{
    description:p.pc.localDescription
   });

  }else if(p){
   await p.pc.setRemoteDescription(x.description);
   await flush(p);
  }

 }else if(x.candidate&&p){
  if(p.pc.remoteDescription){
   await p.pc.addIceCandidate(x.candidate).catch(()=>{});
  }else{
   p.pending.push(x.candidate);
  }
 }
}

function onCallState(d){
 if(d.users.length){
  S.calls[d.channel]=d.users;
 }else{
  delete S.calls[d.channel];
 }

 if(call&&call.channel===d.channel){
  const ids=new Set(d.users.map(u=>u.id));

  for(const [id,p] of Object.entries(call.peers)){
   if(!ids.has(Number(id))){
    p.pc.close();
    delete call.peers[id];
   }
  }
 }

 drawCall();
 drawList();
 drawRing();
}

function sendMedia(){
 wsend({
  type:"call_media",
  mic:call.mic,
  cam:call.cam
 });
}

function toggleMic(){
 if(!call)return;

 call.mic=!call.mic;
 call.atrack.enabled=call.mic;

 sendMedia();
 drawCall();
}

async function toggleCam(){
 if(!call)return;

 if(!call.cam){
  let s;

  try{
   s=await navigator.mediaDevices.getUserMedia({
    video:{width:640,height:360}
   });
  }catch(e){
   alert("カメラを使えません: "+e.message);
   return;
  }

  call.vtrack=s.getVideoTracks()[0];
  call.preview=new MediaStream([call.vtrack]);

 }else{
  call.vtrack.stop();
  call.vtrack=null;
  call.preview=null;
 }

 call.cam=!!call.vtrack;

 for(const p of Object.values(call.peers)){
  if(p.vsender){
   await p.vsender.replaceTrack(call.vtrack);
  }
 }

 sendMedia();
 drawCall();
}

function drawCall(){
 const box=$("call");
 const tiles=$("tiles");

 if(!call){
  box.hidden=true;
  tiles.innerHTML="";
  updateCallBtn();
  return;
 }

 box.hidden=false;

 $("calltitle").textContent=
  "通話中: "+chanName(call.channel);

 $("micb").textContent=
  call.mic?"マイクON":"ミュート中";

 $("micb").className=
  call.mic?"":"offb";

 $("camb").textContent=
  call.cam?"カメラON":"カメラOFF";

 $("camb").className=
  call.cam?"":"offb";

 const users=S.calls[call.channel]||[];
 const keep=new Set();

 users.forEach(u=>{
  const id="t"+u.id;
  keep.add(id);

  let t=document.getElementById(id);

  if(!t){
   t=document.createElement("div");
   t.id=id;
   t.className="tile";

   const v=document.createElement("video");

   v.autoplay=true;
   v.playsInline=true;

   if(u.id===S.me.id)v.muted=true;

   const av=document.createElement("div");

   av.className="av";
   av.textContent=u.name.slice(0,1);

   const nm=document.createElement("div");

   nm.className="nm";

   t.append(v,av,nm);
   tiles.append(t);
  }

  t.classList.toggle("cam",!!u.cam);
  t.classList.toggle("off",!u.mic);

  t.querySelector(".nm").textContent=
   (u.mic?"":"ミュート中 ")+u.name;

  const v=t.querySelector("video");

  let src=null;

  if(u.id===S.me.id){
   src=call.cam?call.preview:null;
  }else if(call.peers[u.id]){
   src=call.peers[u.id].stream;
  }

  if(v.srcObject!==src){
   v.srcObject=src;

   if(src)v.play().catch(()=>{});
  }
 });

 [...tiles.children].forEach(t=>{
  if(!keep.has(t.id))t.remove();
 });

 updateCallBtn();
}

function updateCallBtn(){
 const b=$("callbtn");

 b.hidden=!callable(S.chan);

 b.textContent=
  call&&call.channel===S.chan
  ?"通話から退出"
  :"通話に参加";

 b.style.background=
  call&&call.channel===S.chan
  ?"#da373c"
  :"#23a559";
}

const callBtn=()=>{
 if(call&&call.channel===S.chan){
  leaveCall();
 }else{
  joinCall(S.chan);
 }
};

function drawRing(){
 const r=$("ring");
 r.innerHTML="";

 S.dms.forEach(c=>{
  const u=S.calls[c.id];

  if(
   u&&
   u.length&&
   !(call&&call.channel===c.id)
  ){
   const d=document.createElement("div");

   d.textContent=` ${c.name} と通話中です`;

   const b=document.createElement("button");

   b.textContent="参加する";

   b.onclick=async()=>{
    setView("dm");
    await openChan(c.id);
    joinCall(c.id);
   };

   d.append(b);
   r.append(d);
  }
 });

 r.hidden=!r.children.length;
}

/* フレンド / サーバー / ロール / DM */

async function addFriend(){
 const c=prompt("友達のフレンドコードを入力");

 if(!c)return;

 const d=await T(
  ()=>api(
   "/api/friends/add",
   "POST",
   {code:c}
  )
 );

 if(d)alert(d.name+" さんとフレンドになりました");
}

async function newServer(){
 const n=prompt("サーバー名");

 if(!n)return;

 const d=await T(
  ()=>api(
   "/api/servers",
   "POST",
   {name:n}
  )
 );

 if(d){
  await load();
  setView(d.id);
 }
}

async function joinServer(){
 const c=prompt("招待コードを入力");

 if(!c)return;

 const d=await T(
  ()=>api(
   "/api/servers/join",
   "POST",
   {invite:c}
  )
 );

 if(d){
  await load();
  setView(d.id);
 }
}

async function newChannel(kind){
 const n=prompt(
  kind==="voice"
  ?"ボイスチャンネル名"
  :"チャンネル名"
 );

 if(!n)return;

 await T(
  ()=>api(
   `/api/servers/${S.view}/channels`,
   "POST",
   {name:n,kind}
  )
 );
}

async function newRole(){
 const n=prompt("ロール名");

 if(!n)return;

 const p=prompt(
  "権限をカンマ区切りで入力\n"+
  "send=発言  files=ファイル送信  react=リアクション\n"+
  "channels=チャンネル管理  roles=ロール管理  admin=管理者",
  "send,files,react"
 );

 if(p===null)return;

 const d=await T(
  ()=>api(
   `/api/servers/${S.view}/roles`,
   "POST",
   {
    name:n,
    perms:p.split(",").map(x=>x.trim())
   }
  )
 );

 if(d)alert("ロールを作成しました");
}

async function assignRole(){
 const u=prompt("ユーザー名");

 if(!u)return;

 const r=prompt("ロール名");

 if(!r)return;

 const add=confirm(
  "OK＝ロールを付与 / キャンセル＝ロールを外す"
 );

 const d=await T(
  ()=>api(
   `/api/servers/${S.view}/assign`,
   "POST",
   {
    user:u,
    role:r,
    remove:!add
   }
  )
 );

 if(d)alert("完了しました");
}

async function openDm(name){
 const d=await T(
  ()=>api(
   "/api/dm",
   "POST",
   {name}
  )
 );

 if(d){
  await load();
  setView("dm");
  openChan(d.id);
 }
}

if(S.token)boot();
</script>
"""


# ------------------------------------------------------------------
# トップページ
# ------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index():
    page = PAGE.replace(
        "LOGO_PLACEHOLDER",
        LOGO_BASE64
    )

    return HTMLResponse(page)


# ------------------------------------------------------------------
# 起動
# ------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    ssl = {}

    if Path("cert.pem").exists() and Path("key.pem").exists():
        ssl = {
            "ssl_certfile": "cert.pem",
            "ssl_keyfile": "key.pem"
        }

        print(
            "cert.pem / key.pem を検出: "
            "https://localhost:8000 で起動します"
        )

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        **ssl
    )