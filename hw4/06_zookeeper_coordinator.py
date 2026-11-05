import kazoo.client
import kazoo.exceptions
import json
import time
import os
import sys
import signal
import boto3
import collections
import itertools
import re
from datetime import datetime

ZK_HOSTS = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"
SESSION_TIMEOUT = 10
LEADER_PATH = "/coordinator/leader_lock"
CHECKPOINT_PATH = "/coordinator/checkpoint"
RESULT_PATH = "/coordinator/result"
EPOCH_PATH = "/coordinator/epoch"

S3_ENDPOINT = "http://127.0.0.1:9000"
ACCESS_KEY = "test"
SECRET_KEY = "test"
BUCKET = "hh-lake"
KEY = "vacancies.jsonl"
CHUNK_SIZE = 2 * 1024 * 1024

COORDINATOR_ID = f"coord_{os.getpid()}"


class Coordinator:
    def __init__(self, enable_fencing=False):
        self.zk = None
        self.is_leader = False
        self.my_epoch = 0
        self.checkpoint = {"processed_bytes": 0, "results": {}}
        self.enable_fencing = enable_fencing
        self.logs = []
        self.file_size = 0

    def log(self, msg):
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_msg = f"[{timestamp}] [{COORDINATOR_ID}] {msg}"
        self.logs.append(log_msg)
        print(log_msg)

    def connect_zk(self):
        self.log(f"Connecting to ZooKeeper (session timeout: {SESSION_TIMEOUT}s)...")
        self.zk = kazoo.client.KazooClient(hosts=ZK_HOSTS, timeout=SESSION_TIMEOUT)
        self.zk.start()
        self.log("Connected to ZooKeeper")

        for path in ["/coordinator", CHECKPOINT_PATH, RESULT_PATH, EPOCH_PATH]:
            if not self.zk.exists(path):
                self.zk.create(path, makepath=True)

    def participate_in_election(self):
        self.log("Participating in leader election...")
        while True:
            try:
                self.zk.create(LEADER_PATH, value=COORDINATOR_ID.encode(), ephemeral=True)
                self.is_leader = True
                current_epoch_data, _ = self.zk.get(EPOCH_PATH)
                current_epoch = int(current_epoch_data.decode()) if current_epoch_data else 0
                self.my_epoch = current_epoch + 1
                self.zk.set(EPOCH_PATH, str(self.my_epoch).encode())
                self.log(f"BECAME LEADER. My epoch: {self.my_epoch}")
                self.run_computation()
                return
            except kazoo.exceptions.NodeExistsError:
                self.is_leader = False
                self.log("Not leader, waiting...")
                while self.zk.exists(LEADER_PATH):
                    time.sleep(1)
                self.log("Leader node disappeared. Retrying election...")

    def load_checkpoint(self):
        try:
            data, _ = self.zk.get(CHECKPOINT_PATH)
            if data:
                self.checkpoint = json.loads(data.decode())
                self.log(f"Loaded checkpoint: processed {self.checkpoint['processed_bytes']} bytes")
        except Exception:
            self.log("Checkpoint not found, starting from scratch")

    def save_checkpoint(self):
        data = json.dumps(self.checkpoint).encode()
        self.zk.set(CHECKPOINT_PATH, data)
        self.log(f"Checkpoint saved: {self.checkpoint['processed_bytes']} bytes")

    def get_file_size(self):
        s3 = boto3.client('s3', endpoint_url=S3_ENDPOINT, aws_access_key_id=ACCESS_KEY,
                          aws_secret_access_key=SECRET_KEY)
        return s3.head_object(Bucket=BUCKET, Key=KEY)['ContentLength']

    def read_chunk(self, start, length):
        s3 = boto3.client('s3', endpoint_url=S3_ENDPOINT, aws_access_key_id=ACCESS_KEY,
                          aws_secret_access_key=SECRET_KEY)
        range_header = f"bytes={start}-{start + length - 1}"
        response = s3.get_object(Bucket=BUCKET, Key=KEY, Range=range_header)
        raw_bytes = response['Body'].read()

        if start + length < self.file_size:
            last_newline = raw_bytes.rfind(b'\n')
            if last_newline != -1:
                raw_bytes = raw_bytes[:last_newline]
                length = last_newline + 1
        return raw_bytes.decode('utf-8'), length

    def mapper(self, chunk_data):
        local_counts = collections.defaultdict(int)
        for line in chunk_data.split('\n'):
            line = line.strip()
            if not line:
                continue
            try:
                line = re.sub(r',\s*}', '}', line)
                line = re.sub(r',\s*]', ']', line)
                vac = json.loads(line)

                name_lower = vac.get('name', '').lower()
                skills_raw = vac.get('key_skills', [])

                if isinstance(skills_raw, list):
                    skills_list = [str(s) for s in skills_raw if s]
                elif isinstance(skills_raw, str):
                    skills_list = [skills_raw]
                else:
                    skills_list = []

                has_go = 'golang' in name_lower or 'goland' in name_lower
                if not has_go:
                    for s in skills_list:
                        if s.lower().strip() in ['go', 'golang', 'goland']:
                            has_go = True
                            break
                if not has_go:
                    continue

                skills = sorted(list(set([s.lower().strip() for s in skills_list if str(s).strip() != ''])))
                if not skills:
                    continue

                for s in skills:
                    local_counts[('S', s)] += 1
                for pair in itertools.combinations(skills, 2):
                    local_counts[('P', pair)] += 1
                for triple in itertools.combinations(skills, 3):
                    local_counts[('T', triple)] += 1
            except:
                continue
        return local_counts

    def run_computation(self):
        self.log("Starting computation...")
        self.load_checkpoint()
        self.file_size = self.get_file_size()
        start = self.checkpoint["processed_bytes"]
        self.log(f"Starting from position: {start} / {self.file_size} bytes")

        while start < self.file_size:
            length = min(CHUNK_SIZE, self.file_size - start)
            chunk_text, actual_length = self.read_chunk(start, length)
            chunk_results = self.mapper(chunk_text)

            for k, v in chunk_results.items():
                key_str = f"{k[0]}|{','.join(k[1]) if isinstance(k[1], tuple) else k[1]}"
                self.checkpoint["results"][key_str] = self.checkpoint["results"].get(key_str, 0) + v

            start += actual_length
            self.checkpoint["processed_bytes"] = start

            if start % (5 * 1024 * 1024) < actual_length:
                self.save_checkpoint()

            time.sleep(0.2)

        self.save_checkpoint()
        self.log("Computation finished!")
        self.write_result()

    def write_result(self):
        result_data = json.dumps(self.checkpoint["results"], ensure_ascii=False, indent=2)

        if self.enable_fencing:
            self.log(f"Attempting to write result with epoch protection (epoch {self.my_epoch})...")

            if self.zk.exists(RESULT_PATH):
                existing_data, _ = self.zk.get(RESULT_PATH)
                if existing_data:
                    self.log(f"REJECTED: Result already exists in ZooKeeper. Write cancelled to prevent overwrite.")
                    return False

            current_epoch_data, _ = self.zk.get(EPOCH_PATH)
            current_epoch = int(current_epoch_data.decode()) if current_epoch_data else 0

            if current_epoch != self.my_epoch:
                self.log(
                    f"REJECTED: Current epoch in ZK ({current_epoch}) does not match my epoch ({self.my_epoch}). Write cancelled.")
                return False

            self.zk.set(RESULT_PATH, result_data.encode())
            self.log(f"Result successfully and safely written (Epoch {self.my_epoch})")
            return True
        else:
            self.log("Writing result WITHOUT epoch protection...")
            self.zk.set(RESULT_PATH, result_data.encode())
            self.log("Result written (may have overwritten new leader's data!)")
            return True

    def stop(self):
        if self.zk:
            self.zk.stop()
            self.zk.close()
        self.log("Coordinator stopped")


if __name__ == "__main__":
    enable_fencing = len(sys.argv) > 1 and sys.argv[1] == "--fencing"
    coord = Coordinator(enable_fencing=enable_fencing)


    def signal_handler(sig, frame):
        coord.log("Stop signal received")
        coord.stop()
        sys.exit(0)


    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        coord.connect_zk()
        coord.participate_in_election()
    except Exception as e:
        coord.log(f"Error: {e}")
        coord.stop()