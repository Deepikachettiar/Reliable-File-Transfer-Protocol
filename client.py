import socket
import os
import json
import struct
import hashlib
import zlib
import time
import argparse

# ---------------- CONFIG ----------------
BUFFER_SIZE = 2048
CHUNK_SIZE = 1024
WINDOW_SIZE = 5
ACK_TIMEOUT = 0.6
MAX_RETRIES = 20

# --------------- PACKET TYPES ---------------
TYPE_HELLO = 1
TYPE_RESUME = 2
TYPE_DATA = 3
TYPE_ACK = 4
TYPE_FIN = 5
TYPE_DONE = 6
TYPE_ERROR = 7

ACK_STRUCT = struct.Struct("!BI")
DATA_HEADER = struct.Struct("!BIII")


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


def read_chunk(file_path, seq, chunk_size):
    with open(file_path, "rb") as f:
        f.seek(seq * chunk_size)
        return f.read(chunk_size)


def print_progress(done_chunks, total_chunks):
    percent = (done_chunks / total_chunks) * 100 if total_chunks else 100
    print(f"\rProgress: {done_chunks}/{total_chunks} chunks ({percent:.2f}%)", end="")


# ---------------- CLIENT LOGIC ----------------
def main():
    parser = argparse.ArgumentParser(description="Reliable UDP File Transfer Client")
    parser.add_argument("server_ip", help="Server IP address")
    parser.add_argument("server_port", type=int, help="Server port")
    parser.add_argument("file_path", help="Path of file to send")
    args = parser.parse_args()

    file_path = args.file_path
    server_addr = (args.server_ip, args.server_port)

    if not os.path.exists(file_path):
        print("Error: file not found")
        return

    filename = os.path.basename(file_path)
    filesize = os.path.getsize(file_path)
    filehash = sha256_file(file_path)
    total_chunks = (filesize + CHUNK_SIZE - 1) // CHUNK_SIZE

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(ACK_TIMEOUT)

    # ---- HELLO handshake ----
    hello = make_json_packet(TYPE_HELLO, {
        "filename": filename,
        "filesize": filesize,
        "filehash": filehash,
        "chunk_size": CHUNK_SIZE,
        "total_chunks": total_chunks
    })

    print(f"Starting transfer of '{filename}' ({filesize} bytes)")
    print(f"SHA-256: {filehash}")
    print("Sending HELLO...")

    retries = 0
    start_seq = 0

    while True:
        sock.sendto(hello, server_addr)
        try:
            data, _ = sock.recvfrom(BUFFER_SIZE)
            pkt_type = data[0]

            if pkt_type == TYPE_RESUME:
                _, payload = parse_json_packet(data)
                start_seq = payload["next_seq"]
                print(f"Server says: {payload['message']}")
                break

            elif pkt_type == TYPE_DONE:
                _, payload = parse_json_packet(data)
                print(payload["message"])
                sock.close()
                return

            elif pkt_type == TYPE_ERROR:
                _, payload = parse_json_packet(data)
                print("Server error:", payload["message"])
                sock.close()
                return

        except socket.timeout:
            retries += 1
            if retries > MAX_RETRIES:
                print("HELLO failed: server not responding")
                sock.close()
                return
            print("Retrying HELLO...")

    # ---- Sliding window transfer ----
    base = start_seq
    next_seq = start_seq
    unacked = {}   # seq -> {"packet": ..., "time": ..., "retries": ...}

    sent_count = 0
    retransmissions = 0
    start_time = time.time()

    while base < total_chunks:
        # Fill the window
        while next_seq < base + WINDOW_SIZE and next_seq < total_chunks:
            if next_seq not in unacked:
                chunk = read_chunk(file_path, next_seq, CHUNK_SIZE)
                packet = make_data_packet(next_seq, chunk)
                sock.sendto(packet, server_addr)
                unacked[next_seq] = {
                    "packet": packet,
                    "time": time.time(),
                    "retries": 0
                }
                print(f"\nSent chunk {next_seq}")
                sent_count += 1
            next_seq += 1

        # Try receiving ACKs
        try:
            data, _ = sock.recvfrom(BUFFER_SIZE)
            pkt_type = data[0]

            if pkt_type == TYPE_ACK:
                _, ack_seq = parse_ack(data)
                if ack_seq in unacked:
                    del unacked[ack_seq]
                    print(f"ACK received for chunk {ack_seq}")

                    # Slide base forward
                    while base not in unacked and base < next_seq:
                        base += 1

                    print_progress(base, total_chunks)

            elif pkt_type == TYPE_ERROR:
                _, payload = parse_json_packet(data)
                print("\nServer error:", payload["message"])

            elif pkt_type == TYPE_RESUME:
                _, payload = parse_json_packet(data)
                print("\nServer requested resume:", payload["message"])
                # Force resend from given point
                base = payload["next_seq"]
                next_seq = base
                unacked.clear()

        except socket.timeout:
            pass

        # Retransmit timed out packets
        now = time.time()
        timed_out = [seq for seq, info in unacked.items() if now - info["time"] > ACK_TIMEOUT]

        for seq in timed_out:
            if unacked[seq]["retries"] >= MAX_RETRIES:
                print(f"\nChunk {seq} failed too many times. Aborting.")
                sock.close()
                return

            sock.sendto(unacked[seq]["packet"], server_addr)
            unacked[seq]["time"] = time.time()
            unacked[seq]["retries"] += 1
            retransmissions += 1
            print(f"\nRetransmitted chunk {seq}")

    print("\nAll chunks sent and ACKed.")

    # ---- FIN ----
    fin = make_json_packet(TYPE_FIN, {
        "filename": filename,
        "filehash": filehash
    })

    retries = 0
    while True:
        sock.sendto(fin, server_addr)
        try:
            data, _ = sock.recvfrom(BUFFER_SIZE)
            pkt_type = data[0]

            if pkt_type == TYPE_DONE:
                _, payload = parse_json_packet(data)
                elapsed = time.time() - start_time
                print("\n" + payload["message"])
                print(f"Total chunks: {total_chunks}")
                print(f"Packets sent: {sent_count}")
                print(f"Retransmissions: {retransmissions}")
                print(f"Elapsed time: {elapsed:.2f} sec")
                if elapsed > 0:
                    throughput = filesize / elapsed / 1024
                    print(f"Approx throughput: {throughput:.2f} KB/s")
                break

            elif pkt_type == TYPE_RESUME:
                _, payload = parse_json_packet(data)
                print("\nServer says transfer incomplete:", payload["message"])
                # Resume again from missing chunk
                base = payload["next_seq"]
                next_seq = base
                unacked.clear()

                while base < total_chunks:
                    while next_seq < base + WINDOW_SIZE and next_seq < total_chunks:
                        if next_seq not in unacked:
                            chunk = read_chunk(file_path, next_seq, CHUNK_SIZE)
                            packet = make_data_packet(next_seq, chunk)
                            sock.sendto(packet, server_addr)
                            unacked[next_seq] = {
                                "packet": packet,
                                "time": time.time(),
                                "retries": 0
                            }
                            print(f"\nResent chunk {next_seq}")
                        next_seq += 1

                    try:
                        d, _ = sock.recvfrom(BUFFER_SIZE)
                        t = d[0]
                        if t == TYPE_ACK:
                            _, ack_seq = parse_ack(d)
                            if ack_seq in unacked:
                                del unacked[ack_seq]
                                while base not in unacked and base < next_seq:
                                    base += 1
                                print_progress(base, total_chunks)
                    except socket.timeout:
                        pass

                    now = time.time()
                    timed_out = [seq for seq, info in unacked.items() if now - info["time"] > ACK_TIMEOUT]
                    for seq in timed_out:
                        sock.sendto(unacked[seq]["packet"], server_addr)
                        unacked[seq]["time"] = time.time()
                        unacked[seq]["retries"] += 1
                        retransmissions += 1
                        print(f"\nRetransmitted chunk {seq}")

                print("\nResume sending completed. Sending FIN again...")

            elif pkt_type == TYPE_ERROR:
                _, payload = parse_json_packet(data)
                print("\nServer error:", payload["message"])
                break

        except socket.timeout:
            retries += 1
            if retries > MAX_RETRIES:
                print("\nFIN failed: server not responding")
                break
            print("Retrying FIN...")

    sock.close()


if __name__ == "__main__":
    main()
