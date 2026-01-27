from cwm_minio_api.load_tests import config

if config.CWM_GETGETTER_ENABLED:
    from cwm_minio_api.load_tests.users.getgetter import GetGetter

if config.CWM_UPLOADER_DOWNLOADER_ENABLED:
    from cwm_minio_api.load_tests.users.uploader_downloader import UploaderDownloader
