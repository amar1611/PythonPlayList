import os
import subprocess
from smb.SMBConnection import SMBConnection
import logging
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

class SmbClient:
    """Class for interaticng with remote Samba/smb server
    """    

    def __init__(self, server_ip, username, password, share_name):
        """Initialize the smb class

        Parameters
        ----------
        server_ip : str
            IP address of the smb server
        username : str
            Username for the smb server
        password : str
            Password for the smb server
        share_name : str
            Root shared folder name on the smb server
        """

        # smb attributes
        self.server_ip = server_ip
        self.username = username
        self.password = password
        self.share_name = share_name
        self.port = 139

        # Hostname of the local machine 
        self.client_name = subprocess.Popen(['hostname'],stdout=subprocess.PIPE).communicate()[0].strip().decode('UTF-8')
        logging.info("SMBClient initialized successfully")


    def connect(self):
        """Function to create a connection with smb server
        """
        self.server = SMBConnection(
            username = self.username,
            password = self.password,
            my_name = self.client_name,
            remote_name = self.share_name
        )
        self.server.connect(self.server_ip, self.port)
        logging.info("Connection to smb created")


    def upload(self, file_name, file_location_local, file_location_smb):
        """Function to upload file on smb server from local

        Parameters
        ----------
        file_name : str
            Name of the file that needs to be copied
        file_location_local : str
            Folder location where file exists on local server
        file_location_smb : str
            Folder location relative to share name where file exists on smb server

        """
        file_path_local = os.path.join(file_location_local, file_name)
        file_path_smb = os.path.join(file_location_smb, file_name)

        data = open(file_path_local, 'rb')
        self.server.storeFile(self.share_name, file_path_smb, data)
        logging.info(f"File [{file_name}] has been uploaded")


    def download(self, file_name, file_location_local, file_location_smb):
        """Function to download file from smb server to local

        Parameters
        ----------
        file_name : str
            Name of the file that needs to be downloaded
        file_location_local : str
            Folder location where file exists on local server
        file_location_smb : str
            Folder location relative to share name where file exists on smb server
        """
        file_path_local = os.path.join(file_location_local, file_name)
        file_path_smb = os.path.join(file_location_smb, file_name)

        with open(file_path_local, 'wb') as fp:
            self.server.retrieveFile(self.share_name, file_path_smb, fp)
        logging.info(f"File [{file_name}] has been downloaded")


    def delete(self, file_name, file_location_smb):
        """Function to delete a file on smb server

        Parameters
        ----------
        file_name : str
            Name of the file that needs to be deleted
        file_location_smb : str
            Folder location relative to share name where file exists on smb server
        """
        file_path = os.path.join(file_location_smb, file_name)
        self.server.deleteFiles(self.share_name, file_path)
        logging.info(f"File [{file_name}] has been deleted")


    def list_files(self, files_location_smb):
        """Function to get the list of files from a folder on SMB server

        Parameters
        ----------
        files_location_smb : [type]
            Folder location relative to share name where files exists on smb server

        Returns
        -------
        list[str]
            List of files in the given smb folder
        """
        filelist = self.server.listPath(self.share_name, files_location_smb)

        file_name_list = []
        for f in filelist:
            file_name_list.append(f.filename)
        file_name_list = [ file_name for file_name in file_name_list if file_name not in ['.', '..']]
        logging.info("File list has been fetched")
        return file_name_list


if __name__ == '__main__':
    server_ip = "0.0.0.0" # Update with samba server ip
    username = "update_me" # Update with samba server username
    password = "update_me" # Update with samba server password
    share_name = "share_name" # Update with the share name

    smb = SmbClient(server_ip, username, password, share_name)
    smb.connect()

    # Copy file from Local to SMB
    file_name = "university_records.csv"
    file_location_local = "data/"
    file_location_smb = "/file_paht/today/" # Update the path relative to share name i.e. excluding the share name
    smb.upload(file_name, file_location_local, file_location_smb)

    # Download the file from SMB to Local
    file_name = "university_records.csv"
    file_location_local = "data/"
    file_location_smb = "/file_path/today/" # Update the path relative to share name i.e. excluding the share name
    smb.download(file_name, file_location_local, file_location_smb)

    # Get the list of files from SMB
    file_name_list = smb.list_files(file_location_smb)
    logging.info(file_name_list)

    # Delete a file from SMB server
    smb.delete(file_name, file_location_smb)