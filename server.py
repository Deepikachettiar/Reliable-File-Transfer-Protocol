import socket
import os
import json
import struct
import hashlib
import zlib
from pathlib import Path


SERVER_IP = "0.0.0.0"
SERVER_PORT = 5000
BUFFER_SIZE = 2048
UPLOAD_DIR = Path("uploads")
STATE_DIR = Path("state")

UPLOAD_DIR.mkdir(exist_ok=True)
STATE_DIR.mkdir(exist_ok=True)

# --------------- PACKET TYPES ---------------
TYPE_HELLO = 1   # client -> server : file metadata
TYPE_RESUME = 2  # server -> client : start/resume from seq
TYPE_DATA = 3    # client -> server : actual chunk
TYPE_ACK = 4     # server -> client : ack for seq
TYPE_FIN = 5     # client -> server : all data sent, verify file
TYPE_DONE = 6    # server -> client : transfer complete
TYPE_ERROR = 7   # server -> client : error message

# Binary formats
ACK_STRUCT = struct.Struct("!BI")         # type(1), seq(4)
DATA_HEADER = struct.Struct("!BIII")      # type(1), seq(4), payload_len(4), crc32(4)


sessions = {}


# ---------------- UTILS ----------------
def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            data = f.read(8192)
            if not data:
                break
            h.update(data)
    return h.hexdigest()


def make_json_packet(pkt_type, obj):
    return bytes([pkt_type]) + json.dumps(obj).encode("utf-8")


def parse_json_packet(data):
    pkt_type = data[0]
    payload = json.loads(data[1:].decode("utf-8"))
    return pkt_type, payload


def make_ack(seq):
    return ACK_STRUCT.pack(TYPE_ACK, seq)


def parse_ack(data):
    pkt_type, seq = ACK_STRUCT.unpack(data[:ACK_STRUCT.size])
    return pkt_type, seq


def make_data_packet(seq, payload):
    crc = zlib.crc32(payload) & 0xFFFFFFFF
    header = DATA_HEADER.pack(TYPE_DATA, seq, len(payload), crc)
    return header + payload


def parse_data_packet(data):
    pkt_type, seq, payload_len, crc = DATA_HEADER.unpack(data[:DATA_HEADER.size])
    payload = data[DATA_HEADER.size:DATA_HEADER.size + payload_len]
    return pkt_type, seq, payload_len, crc, payload


def state_file_for(filename):
    safe = filename.replace("/", "_").replace("\\", "_")
    return STATE_DIR / f"{safe}.json"


def part_file_for(filename):
    safe = filename.replace("/", "_").replace("\\", "_")
    return UPLOAD_DIR / f"{safe}.part"


def final_file_for(filename):
    safe = filename.replace("/", "_").replace("\\", "_")
    return UPLOAD_DIR / safe


def init_or_load_session(meta):
    filename = meta["filename"]
    filesize = meta["filesize"]
    filehash = meta["filehash"]
    chunk_size = meta["chunk_size"]
    total_chunks = meta["total_chunks"]

    state_path = state_file_for(filename)
    part_path = part_file_for(filename)
    final_path = final_file_for(filename)

    # If already fully uploaded and hash matches, mark done
    if final_path.exists():
        try:
            if sha256_file(final_path) == filehash:
                return {
                    "status": "done",
                    "filename": filename,
                    "next_seq": total_chunks
                }
        except Exception:
            pass

    session = {
        "filename": filename,
        "filesize": filesize,
        "filehash": filehash,
        "chunk_size": chunk_size,
        "total_chunks": total_chunks,
        "part_path": str(part_path),
        "final_path": str(final_path),
        "state_path": str(state_path),
        "received": set(),
        "next_seq": 0
    }

    # Load existing compatible state
    if state_path.exists():
        try:
            with open(state_path, "r", encoding="utf-8") as f:
                saved = json.load(f)

            if (
                saved.get("filename") == filename and
                saved.get("filesize") == filesize and
                saved.get("filehash") == filehash and
                saved.get("chunk_size") == chunk_size and
                saved.get("total_chunks") == total_chunks
            ):
                session["received"] = set(saved.get("received", []))
        except Exception:
            pass

    # Ensure part file exists and has correct size
    with open(part_path, "ab") as f:
        pass
    with open(part_path, "r+b") as f:
        f.truncate(filesize)

    # Compute first missing contiguous sequence for resume
    next_seq = 0
    while next_seq in session["received"]:
        next_seq += 1
    session["next_seq"] = next_seq

    save_session_state(session)
    return session


def save_session_state(session):
    state = {
        "filename": session["filename"],
        "filesize": session["filesize"],
        "filehash": session["filehash"],
        "chunk_size": session["chunk_size"],
        "total_chunks": session["total_chunks"],
        "received": sorted(session["received"])
    }
    with open(session["state_path"], "w", encoding="utf-8") as f:
        json.dump(state, f)


def update_next_seq(session):
    while session["next_seq"] in session["received"]:
        session["next_seq"] += 1


def write_chunk(session, seq, payload):
    offset = seq * session["chunk_size"]
    with open(session["part_path"], "r+b") as f:
        f.seek(offset)
        f.write(payload)


def finalize_session(session):
    if len(session["received"]) != session["total_chunks"]:
        return False, f"Missing chunks. Resume from {session['next_seq']}"

    computed_hash = sha256_file(session["part_path"])
    if computed_hash != session["filehash"]:
        return False, "Integrity check failed: SHA-256 mismatch"

    # Rename .part to final file
    if os.path.exists(session["final_path"]):
        os.remove(session["final_path"])
    os.replace(session["part_path"], session["final_path"])

    # Remove state file
    if os.path.exists(session["state_path"]):
        os.remove(session["state_path"])

    return True, "Transfer complete"


# ---------------- SERVER ----------------
def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_socket.bind((SERVER_IP, SERVER_PORT))
    print(f"UDP server listening on {SERVER_IP}:{SERVER_PORT}")

    while True:
        data, client_addr = server_socket.recvfrom(BUFFER_SIZE)

        if not data:
            continue

        pkt_type = data[0]

        # ---- HELLO ----
        if pkt_type == TYPE_HELLO:
            _, meta = parse_json_packet(data)
            session = init_or_load_session(meta)

            if session.get("status") == "done":
                reply = make_json_packet(TYPE_DONE, {
                    "message": "File already exists on server",
                    "next_seq": meta["total_chunks"]
                })
                server_socket.sendto(reply, client_addr)
                print(f"[HELLO] {meta['filename']} already complete for {client_addr}")
                continue

            sessions[client_addr] = session

            reply = make_json_packet(TYPE_RESUME, {
                "next_seq": session["next_seq"],
                "message": f"Resume from chunk {session['next_seq']}"
            })
            server_socket.sendto(reply, client_addr)
            print(f"[HELLO] {meta['filename']} from {client_addr}, resume at {session['next_seq']}")

        # ---- DATA ----
        elif pkt_type == TYPE_DATA:
            if client_addr not in sessions:
                err = make_json_packet(TYPE_ERROR, {"message": "No active session. Send HELLO first."})
                server_socket.sendto(err, client_addr)
                continue

            session = sessions[client_addr]

            try:
                _, seq, payload_len, crc, payload = parse_data_packet(data)
            except Exception:
                err = make_json_packet(TYPE_ERROR, {"message": "Malformed DATA packet"})
                server_socket.sendto(err, client_addr)
                continue

            if payload_len != len(payload):
                err = make_json_packet(TYPE_ERROR, {"message": f"Payload length mismatch on chunk {seq}"})
                server_socket.sendto(err, client_addr)
                continue

            computed_crc = zlib.crc32(payload) & 0xFFFFFFFF
            if computed_crc != crc:
                # Corrupted packet: do not ACK, sender will timeout and resend
                print(f"[DROP] CRC mismatch for chunk {seq} from {client_addr}")
                continue

            if seq >= session["total_chunks"]:
                err = make_json_packet(TYPE_ERROR, {"message": f"Invalid chunk number {seq}"})
                server_socket.sendto(err, client_addr)
                continue

            if seq not in session["received"]:
                write_chunk(session, seq, payload)
                session["received"].add(seq)
                update_next_seq(session)
                save_session_state(session)
                print(f"[RECV] chunk {seq} ({len(payload)} bytes) from {client_addr}")
            else:
                print(f"[DUP ] chunk {seq} from {client_addr}")

            server_socket.sendto(make_ack(seq), client_addr)

        # ---- FIN ----
        elif pkt_type == TYPE_FIN:
            if client_addr not in sessions:
                err = make_json_packet(TYPE_ERROR, {"message": "No active session for FIN"})
                server_socket.sendto(err, client_addr)
                continue

            session = sessions[client_addr]
            _, info = parse_json_packet(data)

            if info.get("filehash") != session["filehash"]:
                err = make_json_packet(TYPE_ERROR, {"message": "Final hash metadata mismatch"})
                server_socket.sendto(err, client_addr)
                continue

            ok, msg = finalize_session(session)
            if ok:
                reply = make_json_packet(TYPE_DONE, {"message": msg})
                server_socket.sendto(reply, client_addr)
                print(f"[DONE] {session['filename']} complete from {client_addr}")
                del sessions[client_addr]
            else:
                reply = make_json_packet(TYPE_RESUME, {
                    "next_seq": session["next_seq"],
                    "message": msg
                })
                server_socket.sendto(reply, client_addr)
                print(f"[RESM] {session['filename']} needs resume from {session['next_seq']}")

        else:
            err = make_json_packet(TYPE_ERROR, {"message": "Unknown packet type"})
            server_socket.sendto(err, client_addr)


if __name__ == "__main__":
    main()
