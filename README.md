#  Reliable File Transfer Protocol over UDP
A custom implementation of a **reliable and resumable file transfer system built on top of UDP** using Python.  
This project demonstrates how reliability mechanisms similar to TCP can be implemented at the **application layer** while using the faster but unreliable **UDP transport protocol**.

---

# 📌 Overview
UDP is a connectionless protocol that provides fast communication but does **not guarantee**:
- Packet delivery
- Packet ordering
- Packet retransmission
- Duplicate detection

To overcome these limitations, this project implements a **custom reliable protocol over UDP**.

The system ensures:
✔ Reliable file delivery  
✔ Correct packet ordering  
✔ Retransmission of lost packets  
✔ Resume interrupted transfers  
✔ File integrity verification  

---

# ⚙️ Key Features

##  Chunk-Based Transfer
Large files are divided into **fixed-size chunks** before transmission.  
Each chunk is sent individually as a UDP packet.

##  Sequence Numbers
Each packet carries a **sequence number** to ensure correct ordering of data.

##  Acknowledgement System
The server sends **ACK packets** for successfully received chunks.

##  Retransmission Mechanism
If an ACK is not received within a timeout period, the client **retransmits the packet**.

##  Resume Interrupted Transfers
If a transfer stops midway, the server remembers received chunks and the client resumes from the **last successfully received chunk**.

##  File Integrity Verification
The client generates a **SHA-256 hash** of the original file.  
After reconstruction, the server verifies the hash to ensure the file was not corrupted.

##  Throughput Optimization
A **sliding window protocol** allows multiple packets to be sent before waiting for acknowledgements, improving transfer speed.

---

#  System Architecture
          +----------------------+
          |        CLIENT        |
          |----------------------|
          | File Reader          |
          | Chunk Generator      |
          | Sequence Numbers     |
          | Sliding Window       |
          | Retransmission Logic |
          | SHA-256 Hash         |
          +----------+-----------+
                     |
                     | UDP Packets
                     v
            ====================
                NETWORK (UDP)
            ====================
                     |
                     v
          +----------+-----------+
          |        SERVER        |
          |----------------------|
          | Packet Receiver      |
          | ACK Generator        |
          | Chunk Reassembly     |
          | Resume Handler       |
          | SHA-256 Verification |
          | File Storage         |
          +----------------------+


---

# 📂 Project Structure
reliable-udp-file-transfer/
│
├── client.py # Client application (file sender)
├── server.py # Server application (file receiver)
│
├── uploads/ # Files received by the server
├── state/ # Resume-transfer metadata
│
├── sample_files/
│ └── test.txt
│
├── README.md
└── docs/
└── architecture.png

---

# ▶ Running the Project

## 1 Start the Server
python3 server.py

The server will start listening for incoming UDP packets.
---
## 2️ Run the Client
python3 client.py <SERVER_IP> <PORT> <FILE_PATH>

Example:
python3 client.py 127.0.0.1 5000 sample_files/test.txt
---

#  Demo Instructions
1. Start the server.
2. Run the client and send a file.
3. Observe chunk transmission and ACK responses.
4. Stop the client during transfer.
5. Restart the client — the transfer will **resume automatically**.
6. 
---

#  Packet Structure
Each packet transmitted over UDP follows this structure:

| Field | Description |
|------|-------------|
| Packet Type | Identifies packet purpose |
| Sequence Number | Chunk order |
| Payload Length | Size of the data chunk |
| CRC32 | Error detection checksum |
| Payload | Actual file data |

---

#  Technologies Used

- Python 3
- UDP Socket Programming
- SHA-256 Hashing (Integrity Verification)
- CRC32 Error Detection
- Sliding Window Protocol

---

#  Learning Outcomes
This project demonstrates:

- Reliable communication over unreliable protocols
- Application-layer protocol design
- Packet structuring and parsing
- Retransmission and timeout mechanisms
- File integrity verification techniques

---

#  Future Improvements
Possible enhancements include:

- Multi-client support
- Encryption for secure file transfer
- Graphical web interface
- Congestion control algorithms

---

#  Author

**Deepika K**  
Computer Science Engineering Student
