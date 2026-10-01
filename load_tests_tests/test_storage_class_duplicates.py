import pytest

from cwm_minio_api.load_tests.campaign import tier as module
from test_review_tiers import setup, restored_cycle


@pytest.mark.parametrize('raw', ['LOW', 'LOW, LOW', ' LOW , LOW '])
def test_duplicate_class_expiry_preserves_raw_evidence(manifest_data, raw):
    tier, s3, clock = setup(manifest_data)
    restored_cycle(tier, s3, clock)
    clock.now = 102 * 86400 + 1
    s3.modes['expiry'] = ('cold', None)
    original = s3.head_object
    def head(**kwargs):
        value = original(**kwargs)
        value['StorageClass'] = raw
        return value
    s3.head_object = head
    tier.expiry()
    rec = tier.records('expiry')[0]
    rows = list(tier.state.observations())
    row = next(row for row in reversed(rows) if row['cohort'] == 'expiry' and row['kind'] == 'head')
    assert row['value']['state'] == 'cold'
    assert row['value']['StorageClass'] == raw
    assert tier.prerequisite('expiry', rec)['value']['status'] == 'passed'


@pytest.mark.parametrize('raw', ['LOW, STANDARD', 'STANDARD, LOW', 'LOW,', ',LOW', '', None, 42])
def test_conflicting_or_malformed_classes_are_not_cold(raw):
    assert not module.is_storage_class({'StorageClass': raw}, 'LOW')
