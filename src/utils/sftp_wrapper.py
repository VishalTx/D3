import paramiko
import stat
from utils.config_wrapper import ConfigWrapper


class SFTPWrapper(ConfigWrapper):
    client = None
    transport = None

    def __init__(self):
        super().__init__()

    def validate_params(self, value, env_key):
        if value is not None:
            return value
        else:
            return self.getenv(env_key)

    def cleanup(self):
        # Cleanup
        if self.client:
            self.client.close()
        if self.transport:
            self.transport.close()

    def change_directory(self, sftp_working_directory):
        self.client.chdir(sftp_working_directory)

    def list_files(self):
        if self.client is None:
            print("Please connect to SFTP to access the services")
            return False
        return self.client.listdir()

    def list_files_by_directory(self, path):
        if self.client is None:
            print("Please connect to SFTP to access the services")
            return False

        file_structure = {}
        def walk_sftp_dir(sftp, in_path):
            items = sftp.listdir_attr(in_path)
            file_structure[path] = []

            for item in items:
                full_path = f"{in_path}/{item.filename}"
                if stat.S_ISDIR(item.st_mode):
                    # It's a directory; recurse into it
                    walk_sftp_dir(sftp, full_path)
                else:
                    # It's a file; add to the list
                    file_structure[in_path].append(item.filename)

        walk_sftp_dir(self.client, path)
        return file_structure

    def file_exists(self, filename):
        return filename in self.client.listdir()

    def connect(self, host=None, port=None, username=None, password=None, sftp_working_dir=None):
        host = self.validate_params(host, 'sftp_host')
        port = int(self.validate_params(port, 'sftp_port'))
        username = self.validate_params(username, 'sftp_username')
        password = self.validate_params(password, 'sftp_password')
        sftp_working_dir = self.validate_params(sftp_working_dir, 'sftp_working_dir')

        try:
            # Setup the transport (TCP connection)
            self.transport = paramiko.Transport((host, port))
            self.transport.connect(username=username, password=password)

            # Setup the SFTP client
            self.client = paramiko.SFTPClient.from_transport(self.transport)
            print("Connected to SFTP server successfully!")

            # Change directory
            self.change_directory(sftp_working_dir)
            return True
        except Exception as e:
            print(f"Error connecting to SFTP server: {e}")
            return False
