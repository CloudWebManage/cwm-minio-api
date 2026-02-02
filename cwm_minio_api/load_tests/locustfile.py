from cwm_minio_api.load_tests import config

if config.CWM_GETGETTER_ENABLED:
    from cwm_minio_api.load_tests.users.getgetter import GetGetter

if config.CWM_UPDOWNDEL_ENABLED:
    from cwm_minio_api.load_tests.users.updowndel import UpDownDel
