from utils.sftp_wrapper import SFTPWrapper

def test_sftp_connect():
    sftp = SFTPWrapper()
    connect = sftp.connect()
    assert connect == True

def test_list_files_current_dir():
    sftp = SFTPWrapper()
    connect = sftp.connect()
    files = sftp.list_files()
    print(files)
    assert connect == True

def test_list_files_by_directory():
    sftp = SFTPWrapper()
    sftp.connect()
    sftp_root_path = "/Inbox/Sample/speedeon/premover_list/"
    files_by_dir = sftp.list_files_by_directory(sftp_root_path)
    for directory, files in files_by_dir.items():
        print(f"\n📁 Directory: {directory}")
        for file in files:
            print(f"  └── {file}")

def test_file_exists():
    sftp = SFTPWrapper()
    sftp.connect()
    file_exists = sftp.file_exists('fmcg_premoveratlist_20250417.txt')
    assert file_exists == True

