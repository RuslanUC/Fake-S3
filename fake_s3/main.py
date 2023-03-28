import os.path
from base64 import b64decode
from hashlib import md5
from pathlib import Path
from re import compile

import click as click
from quart import Quart, request

from .file_store import FileStore
from .s3_responses import CompleteMultipartUploadResult, NoSuchBucket, InitiateMultipartUploadResult, \
    InvalidBucketName, BucketAlreadyExists, ListBucketResult, VersioningConfiguration, \
    LocationConstraint, ListAllBucketsResult, BucketNotEmpty

bucket_name_pattern = compile('^[a-z0-9_-]{1,255}$')
etag_pattern = compile(r'(?:<ETag>")([a-z\d]{32})(?:"<\/ETag>)')
partnum_pattern = compile(r'(?:<PartNumber>)(\d{1,})(?:<\/PartNumber>)')
access_pattern = compile(r'(?:<BlockPublicAcls>)(true|false)(?:<\/BlockPublicAcls>)')

app = Quart("Fake-S3")
app.url_map.strict_slashes = False
app.config['MAX_CONTENT_LENGTH'] = 512 * 1024 * 1024
app.config["RESPONSE_TIMEOUT"] = 9000
app.config["BODY_TIMEOUT"] = 600

@app.get("/")
async def list_buckets():
    store = app.config["store"]
    return ListAllBucketsResult(store.OWNER, store.get_buckets()).gen()

@app.delete("/<string:bucket>")
async def delete_bucket(bucket: str):
    store = app.config["store"]
    if not os.path.exists(store.bucket_path(bucket)):
        return NoSuchBucket
    try:
        os.rmdir(store.bucket_path(bucket))
    except OSError as exc:
        if not exc.errno == 39:  # 39 = Directory not empty
            return InvalidBucketName
        return BucketNotEmpty
    return "", 204

@app.get("/<string:bucket>")
async def get_bucket_contents(bucket: str):
    store = app.config["store"]
    if not os.path.exists(store.bucket_path(bucket)):
        return NoSuchBucket
    if "location" in request.args:
        return LocationConstraint().gen()
    elif "versioning" in request.args:
        return VersioningConfiguration().gen()
    objects = store.get_objects(bucket, int(request.args.get("max_keys", 1000)), request.args.get("prefix", ""))
    print(objects)
    return ListBucketResult(objects, bucket).gen()

@app.put("/<string:bucket>")
async def create_bucket(bucket: str):
    store = app.config["store"]
    if "publicAccessBlock" not in request.args:
        if not bucket_name_pattern.match(bucket):
            return InvalidBucketName
        if os.path.exists(store.bucket_path(bucket)):
            return BucketAlreadyExists
        store.create_bucket(bucket)
    return ""

@app.delete("/<string:bucket>/<path:key>")
async def delete_object(bucket: str, key: str):
    store = app.config["store"]
    if not store.get_object(bucket, key):
        return "", 204
    store.delete_object(bucket, key)
    return "", 204

async def putObjectSinglepart(bucket: str, key: str):
    store = app.config["store"]
    data = await request.body
    if md5_checksum := request.content_md5:
        md5_checksum = b64decode(bytes(md5_checksum, "utf8")).hex()
    else:
        m = md5()
        m.update(data)
        md5_checksum = m.hexdigest()
    await store.store_object_singlepart(bucket, key, data, md5_checksum, request.headers.get("Content-Type"))
    return "", 200, {"etag": f"\"{md5_checksum}\""}

async def putObjectMultipart(bucket: str, key: str, partNumber: int):
    store = app.config["store"]
    data = await request.body
    if md5_checksum := request.content_md5:
        md5_checksum = b64decode(bytes(md5_checksum, "utf8")).hex()
    else:
        m = md5()
        m.update(data)
        md5_checksum = m.hexdigest()
    await store.store_object_multipart(bucket, key, data, partNumber)
    return "", 200, {"etag": f"\"{md5_checksum}\""}

@app.put("/<string:bucket>/<path:key>")
async def put_object(bucket: str, key: str):
    store = app.config["store"]
    if not os.path.exists(store.bucket_path(bucket)):
        return NoSuchBucket
    if uploadId := request.args.get("uploadId"):
        m = md5()
        m.update(f"{bucket}/{key}".encode("utf8"))
        assert uploadId == m.hexdigest()
        partNumber = int(request.args.get("partNumber", 0))
        return await putObjectMultipart(bucket, key, int(partNumber))
    return await putObjectSinglepart(bucket, key)

@app.post("/<string:bucket>/<path:key>")
async def create_complete_multipart_upload(bucket: str, key: str):
    if "uploads" in request.args: # Create multipart upload
        m = md5()
        m.update(f"{bucket}/{key}".encode("utf8"))
        uploadId = m.hexdigest()
        return InitiateMultipartUploadResult(bucket, key, uploadId).gen()
    elif uploadId := request.args.get("uploadId"): # Complete multipart upload
        m = md5()
        m.update(f"{bucket}/{key}".encode("utf8"))
        assert uploadId == m.hexdigest()
        data = await request.body
        data = data.decode("utf8")
        parts = list(zip(etag_pattern.findall(data), partnum_pattern.findall(data)))
        parts.sort(key=lambda x: int(x[1]))
        parts = [m[0] for m in parts]
        b = b""
        for part in parts:
            b += bytes.fromhex(part)
        m = md5()
        m.update(b)
        md5_checksum = f"{m.hexdigest()}-{len(parts)}"
        await app.config["store"].complete_multipart_upload(bucket, key, md5_checksum)
        return CompleteMultipartUploadResult(bucket, key, md5_checksum).gen()

@app.get("/<string:bucket>/<path:key>")
async def getObject(bucket: str, key: str):
    store = app.config["store"]
    if not os.path.exists(store.bucket_path(bucket)):
        return NoSuchBucket
    if not (obj := store.get_object(bucket, key)):
        return "", 404
    mime = obj.ContentType
    headers = {"Content-Type": mime, "Content-Length": str(obj.Size)}
    if not (mime.startswith("image/") or mime.startswith("text/") or mime.startswith("video/")):
        name = obj.Key.split("/")[-1]
        headers["Content-Disposition"] = f"attachment; filename={name}"
    start = 0
    end = obj.Size-1
    if "Range" in request.headers:
        range_ = request.headers['range'].split('=')[1]
        start = int(range_.split('-')[0])
        end = int(range_.split('-')[1])
        if end == 0:
            end = obj.Size - 1
        bytes_to_read = end - start + 1
        headers["Content-Length"] = str(bytes_to_read)
        headers["Content-Range"] = f"bytes {start}-{end}/{obj.Size}"
    return store.stream_object(bucket, key, start, end)

@click.command()
@click.option("--host", default="0.0.0.0", help="Hostname to listen on.")
@click.option("--port", default=10001, help="Port to run server on.")
@click.option("--root", default=f"{Path.home()}/s3store", help="Defaults to $HOME/s3store.")
def main(host: str, port: int, root: str):
    app.config["store"] = FileStore(root)
    from uvicorn import run
    run(app, host=host, port=port, use_colors=False)

if __name__ == "__main__":
    main()