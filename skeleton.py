from numpy import random


class SharedFolder(object):
    # constructor
    def __init__(self, id):
        self.id = id
        self.my_devices = []

    # fancy printing as string
    def __str__(self):
        return str(self.id)

    # add a device to the list of devices registering this shared folder
    def add_device(self, device):
        self.my_devices.append(device)

    def on_upload(self, file_id, up_device_id, list_devices):
        for device in self.my_devices:
            if device.id != up_device_id:
                list_devices[device.id].file_to_download.append(file_id)


class Device(object):
    # constructor
    def __init__(self, id):
        self.id = id
        self.my_shared_folders = []
        self.file_to_download = []

    # fancy printing as string
    def __str__(self):
        sf_str = ", ".join([str(i) for i in self.my_shared_folders])
        return "Device: " + str(self.id) + ", Shared Folders [" + sf_str + "]"

    # add a shared folder to this device
    def add_shared_folder(self, sf):
        self.my_shared_folders.append(sf)

