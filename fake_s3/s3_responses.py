class s3Type:
    def toXML(self, removeNone=False):
        xml = f"<{self.__class__.__name__}>"
        for k,v in self.__dict__.items():
            if v is None:
                if removeNone:
                    continue
                v = ""
            if isinstance(v, s3Type):
                xml += v.toXML()
                continue
            xml += f"<{k}>{v}</{k}>"
        xml += f"</{self.__class__.__name__}>"
        return xml

class Owner(s3Type):
    def __init__(self, id, name):
        self.ID = id
        self.DisplayName = name

class Bucket(s3Type):
    def __init__(self, name, creation_date):
        self.Name = name
        self.CreationDate = creation_date

class Object(s3Type):
    def __init__(self, Key, ETag, Owner, Size, LastModified, ContentType, **kwargs):
        self.Key = Key
        self.ETag = ETag
        self.LastModified = LastModified
        self.Owner = Owner
        self.StorageClass = "STANDART"
        self.Size = Size
        self.ContentType = ContentType

class Contents(Object):
    pass

class Error(s3Type):
    def __init__(self, code, message, resource=None):
        self.Code = code
        self.Message = message
        self.Resource = resource

    def gen(self):
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>{body}".format(body=self.toXML(removeNone=True))

RESULT_PAYLOAD = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><{resultName} xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{body}</{resultName}>"

class ListAllBucketsResult:
    BODY = "{owner}<Buckets>{buckets}</Buckets>"

    def __init__(self, owner, buckets):
        self.owner = owner
        self.buckets = buckets

    def gen(self):
        owner = self.owner.toXML()
        buckets = "".join([b.toXML() for b in self.buckets])
        return RESULT_PAYLOAD.format(resultName=self.__class__.__name__, body=self.BODY.format(owner=owner, buckets=buckets))

class LocationConstraint:
    def gen(self):
        return RESULT_PAYLOAD.format(resultName=self.__class__.__name__, body="local")

class VersioningConfiguration:
    def gen(self):
        return RESULT_PAYLOAD.format(resultName=self.__class__.__name__, body="<Status>Disabled</Status>")

class ListBucketResult:
    BODY = "{items}<EncodingType>{etype}</EncodingType><IsTruncated>{is_trunc}</IsTruncated><MaxKeys>{max_keys}</MaxKeys><Name>{name}</Name><Prefix>{prefix}</Prefix><Marker>{marker}</Marker>"

    def __init__(self, items, name, etype="url", is_trunc=False, max_keys=1000, prefix=None, marker=None):
        self.items = items
        self.etype = etype
        self.is_trunc = is_trunc
        self.name = name
        self.max_keys = max_keys
        self.prefix = prefix
        self.marker = marker

    def gen(self):
        items = [Contents(**item.__dict__) for item in self.items]
        d = {
            "items": "".join([i.toXML() for i in items]),
            "etype": self.etype,
            "is_trunc": "true" if self.is_trunc else "false",
            "max_keys": str(self.max_keys), "name": self.name,
            "prefix": self.prefix if self.prefix is not None else "",
            "marker": self.marker if self.marker is not None else ""
        }

        return RESULT_PAYLOAD.format(resultName=self.__class__.__name__, body=self.BODY.format(**d))

class CompleteMultipartUploadResult:
    BODY = "<Bucket>{bucket}</Bucket><ETag>\"{hash}\"</ETag><Key>{name}</Key><Location>/{bucket}/{name}</Location>"

    def __init__(self, bucket, name, hash):
        self.bucket = bucket
        self.name = name
        self.hash = hash

    def gen(self):
        d = {}
        d["bucket"] = self.bucket
        d["name"] = self.name
        d["hash"] = self.hash

        return RESULT_PAYLOAD.format(resultName=self.__class__.__name__, body=self.BODY.format(**d))

class InitiateMultipartUploadResult:
    BODY = "<Bucket>{bucket}</Bucket><Key>{name}</Key><UploadId>{uploadId}</UploadId>"

    def __init__(self, bucket, name, uploadId):
        self.bucket = bucket
        self.name = name
        self.uploadId = uploadId

    def gen(self):
        d = {}
        d["bucket"] = self.bucket
        d["name"] = self.name
        d["uploadId"] = self.uploadId

        return RESULT_PAYLOAD.format(resultName=self.__class__.__name__, body=self.BODY.format(**d))

NoSuchBucket = (Error("NoSuchBucket", f"The specified bucket does not exist").gen(), 404)
InvalidBucketName = (Error("InvalidBucketName", "Invalid characters in bucketName or bucketName has invalid length (must be 1-255)").gen(), 400)
BucketAlreadyExists = (Error("BucketAlreadyExists", "Bucket name is already in use!").gen(), 409)
BucketNotEmpty = (Error("BucketNotEmpty", "The bucket is not empty").gen(), 409)