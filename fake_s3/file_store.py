import json
import os
from datetime import datetime
from shutil import rmtree
from typing import TypedDict, Optional, AsyncIterator

import aiofiles
from magic import from_buffer

from .s3_responses import Bucket, Owner, Object

PART_FILE = '.fakes3_content_part'
CONTENT_FILE = '.fakes3_content'
METADATA_FILE = '.fakes3_metadata'

class PartDict(TypedDict):
    partNum: int
    partMd5: str

class MetadataDict(TypedDict):
    size: int
    md5: str
    content_type: str
    creation_date: str
    parts: Optional[list[PartDict]]

class FileStore:
    OWNER = Owner(1, "FakeS3")

    def __init__(self, root: str):
        self._root = root
        if not os.path.exists(root):
            os.makedirs(root, exist_ok=True)

    def get_buckets(self) -> list[Bucket]:
        buckets = []
        for bucket_name in os.listdir(self._root):
            mtime = os.stat(self.bucket_path(bucket_name)).st_mtime
            buckets.append(Bucket(bucket_name, datetime.fromtimestamp(mtime).strftime('%Y-%m-%dT%H:%M:%S.000Z')))
        buckets.sort(key=lambda b: b.Name)
        return buckets

    def get_objects(self, bucket_name: str, max_keys: int=1000, prefix: str="") -> list[Object]:
        objects = []
        for root, dirs, files in os.walk(self.bucket_path(bucket_name)):
            if not root.startswith(os.path.join(self.bucket_path(bucket_name), prefix)) \
                    or METADATA_FILE not in files or CONTENT_FILE not in files:
                continue
            with open(os.path.join(root, METADATA_FILE), "r") as f:
                metadata: MetadataDict = json.load(f)
            key = root.replace(self.bucket_path(bucket_name), "", 1)
            objects.append(Object(key, metadata["md5"], self.OWNER, metadata["size"], metadata["creation_date"], metadata["content_type"]))
            if len(objects) >= max_keys:
                break
        return objects

    def bucket_path(self, bucket_name: str) -> str:
        return os.path.join(self._root, bucket_name)

    def key_path(self, bucket_name: str, key: str) -> str:
        return os.path.join(self.bucket_path(bucket_name), key)

    def create_bucket(self, bucket_name: str) -> None:
        os.makedirs(self.bucket_path(bucket_name), exist_ok=True)

    def get_object(self, bucket_name: str, key: str) -> Optional[Object]:
        object_root = self.key_path(bucket_name, key)
        if os.path.exists(object_root) and os.path.exists(os.path.join(object_root, METADATA_FILE)) \
                and os.path.exists(os.path.join(object_root, CONTENT_FILE)):
            with open(os.path.join(object_root, METADATA_FILE), "r") as f:
                metadata: MetadataDict = json.load(f)
            return Object(key, metadata["md5"], self.OWNER, metadata["size"], metadata["creation_date"], metadata["content_type"])

    def delete_object(self, bucket_name: str, key: str) -> None:
        rmtree(self.key_path(bucket_name, key), ignore_errors=True)

    async def store_object_singlepart(self, bucket_name: str, key: str, data: bytes, md5_checksum: str, content_type: str=None) -> None:
        if not content_type:
            content_type = from_buffer(data[:4096], mime=True)
        obj_root = self.key_path(bucket_name, key)
        os.makedirs(obj_root, exist_ok=True)
        async with aiofiles.open(os.path.join(obj_root, CONTENT_FILE), "wb") as f:
            await f.write(data)
        async with aiofiles.open(os.path.join(obj_root, METADATA_FILE), "w") as f:
            metadata = {
                "size": len(data),
                "md5": md5_checksum,
                "content_type": content_type,
                "creation_date": datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z'),
            }
            await f.write(json.dumps(metadata))

    async def store_object_multipart(self, bucket_name: str, key: str, data: bytes, partNumber: int) -> None:
        obj_root = self.key_path(bucket_name, key)
        os.makedirs(obj_root, exist_ok=True)
        async with aiofiles.open(os.path.join(obj_root, f"{partNumber}{PART_FILE}"), "wb") as f:
            await f.write(data)

    async def complete_multipart_upload(self, bucket_name: str, key: str, md5_checksum: str) -> None:
        obj_root = self.key_path(bucket_name, key)
        parts = [int(part.replace(PART_FILE, "")) for part in os.listdir(obj_root)]
        parts.sort()
        first_part = b""
        size = 0
        async with aiofiles.open(os.path.join(obj_root, CONTENT_FILE), "wb") as f:
            for part in parts:
                async with aiofiles.open(os.path.join(obj_root, f"{part}{PART_FILE}"), "rb") as p:
                    while data := await p.read(32*1024*1024):
                        if not first_part: first_part = data[:4096]
                        size += len(data)
                        await f.write(data)
                os.remove(os.path.join(obj_root, f"{part}{PART_FILE}"))
        async with aiofiles.open(os.path.join(obj_root, METADATA_FILE), "w") as f:
            metadata = {
                "size": size,
                "md5": md5_checksum,
                "content_type": from_buffer(first_part[:4096], mime=True),
                "creation_date": datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z'),
            }
            await f.write(json.dumps(metadata))

    async def stream_object(self, bucket_name: str, key: str, start: int, end: int) -> AsyncIterator[bytes]:
        obj_root = self.key_path(bucket_name, key)
        total_read = end-start
        if total_read <= 0: total_read = 1024**4

        async with aiofiles.open(os.path.join(obj_root, CONTENT_FILE), "rb") as f:
            await f.seek(start)
            to_read = 1024*1024*2 if total_read > 1024*1024*2 else total_read
            total_read -= to_read
            while data := await f.read(to_read):
                yield data
