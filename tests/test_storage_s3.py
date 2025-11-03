import boto3
from tap_grib.storage import Storage
import os
import pytest

import os
import pytest
from moto.server import ThreadedMotoServer


@pytest.fixture(scope="session")
def moto_http_endpoint():
    server = ThreadedMotoServer()  # picks a free port
    server.start()
    try:
        srv, port = server.get_host_and_port()
        yield f"http://{srv}:{port}"
    finally:
        server.stop()


@pytest.fixture(scope="session")
def aws_test_env(moto_http_endpoint):
    # Minimal AWS env for s3fs/boto3
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    # Point Storage to Moto HTTP
    os.environ["S3_ENDPOINT_URL"] = moto_http_endpoint
    return moto_http_endpoint


@pytest.mark.usefixtures("aws_test_env")
def test_storage_s3_roundtrip(moto_http_endpoint):
    s3 = boto3.client(
        "s3",
        endpoint_url=moto_http_endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )
    bucket = "local-data"
    s3.create_bucket(Bucket=bucket)

    key = "test/test.grib"
    content = b"FAKEGRIB"
    s3.put_object(Bucket=bucket, Key=key, Body=content)

    storage = Storage(f"s3://{bucket}/**/*.grib")
    files = storage.glob()
    assert any(p.endswith(key) for p in files), files

    file_path = next(p for p in files if p.endswith(key))
    info = storage.describe(file_path)
    assert info.size == len(content)
    assert info.mtime is not None

    with storage.open(file_path, "rb") as fh:
        assert fh.read() == content
