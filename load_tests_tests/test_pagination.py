import botocore.session
from botocore.stub import Stubber

from cwm_minio_api.load_tests.campaign.config import Manifest
from cwm_minio_api.load_tests.campaign.state import State
from cwm_minio_api.load_tests.campaign.s3 import ObjectStore


def test_multipart_wire_pagination_uses_both_markers(manifest_data):
    m = Manifest.model_validate(manifest_data)
    s3 = botocore.session.get_session().create_client("s3", aws_access_key_id="test", aws_secret_access_key="test")
    store = ObjectStore(m, State(m), s3)
    with Stubber(s3) as stub:
        stub.add_response("list_multipart_uploads", {
            "IsTruncated": True, "NextKeyMarker": "same", "NextUploadIdMarker": "one",
            "Uploads": [{"Key": "same", "UploadId": "one"}],
        }, {"Bucket": "bucket", "MaxUploads": 1000})
        stub.add_response("list_multipart_uploads", {
            "IsTruncated": False, "Uploads": [{"Key": "same", "UploadId": "two"}],
        }, {"Bucket": "bucket", "MaxUploads": 1000, "KeyMarker": "same", "UploadIdMarker": "one"})
        assert [r["UploadId"] for r in store.uploads("bucket")] == ["one", "two"]
