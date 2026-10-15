import json
from cwm_minio_api.load_tests.campaign import __main__ as campaign


def test_unexpected_diagnostic_does_not_copy_secret_messages_or_locals():
    secret = 'credential-must-not-be-serialized'
    try:
        raise ValueError(secret)
    except ValueError as exc:
        diagnostic = campaign.failure_diagnostic(exc)
    assert diagnostic['exception'] == 'ValueError'
    assert diagnostic['frames']
    assert secret not in json.dumps(diagnostic)
    assert all(set(frame) == {'file', 'function', 'line'} for frame in diagnostic['frames'])
