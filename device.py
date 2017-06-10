# coding=utf-8
from collections import deque
import simpy
import random
import skeleton
import numpy
import re



class ObjFile:
    def __init__(self, id, th, size):
        self.id = id
        self.throughput = th
        self.size = size
        self.owners = []
        self.transfer_time = None
        self.uploaded = False


# modify: review the creation of objects
def create_files_object(file_objects):

    with open('Throughput.txt') as f:
        pattern = r"\d[0-9.]*"
        content = f.readlines()
        for line in content:
            if re.match(pattern, line):
                result = re.findall(pattern, line)
                temp_obj = ObjFile(numpy.abs(hash(line)), float(result[1]), int(result[0]))
                try:
                    temp_obj.transfer_time = int(result[0]) / float(result[1])
                except ZeroDivisionError:
                    print "error " + str(len(file_objects))
                file_objects[temp_obj.id] = temp_obj
    f.close()


def log_norm_computation(mu, sigma):
    # last number is the size for output shape. I don't think is necessary
    return numpy.random.lognormal(mu, sigma)


# spec: define how to pick a new file to download
def pick_a_file(dic_list_files):
    not_up_files = filter(lambda x: True if not x.uploaded else False, dic_list_files.values())
    selected_file_id = not_up_files[random.randint(0, len(not_up_files) + 1)].id
    dic_list_files[selected_file_id].uploaded = True
    return selected_file_id


def verify_devices_download_list_empty(devices):
    for device in devices:
        if len(device.file_to_download) > 0:
            return True
    return False


def verify_remaining_file_to_upload(dic_files):
    if len(filter(lambda x: True if not x.uploaded else False, dic_files.values())) == 0:
        return False
    else:
        return True


class SimulationDevice(skeleton.Device):
    def __init__(self, id):
        super(SimulationDevice, self).__init__(id)
        self.chosen_sf_id = None
        self.status = True
        self.total_uploaded_traffic = 0.0
        self.total_downloaded_traffic = 0.0
        self.file_pending_up = None
        self.file_pending_dw = None

    def run(self, environment, shared_folders, devices, dic_files):
        while True:
            # modify: use a log-normal mu: 8.492 sigma: 1.545

            # spec: select a random SF on which we'll work during the session
            self.chosen_sf_id = self.my_shared_folders[random.randint(0, len(self.my_shared_folders)-1)].id

            session_duration = log_norm_computation(8.492, 1.545)
            # modify: use a log-normal mu: 7.971 sigma: 1.308
            inter_session_time = log_norm_computation(7.971, 1.308)

            # spec: we consider both handled by the device using global information
            yield environment.process(self.upload(session_duration, environment, shared_folders, devices, dic_files))
            yield environment.process(self.download(session_duration, environment, dic_files))

            # DEBUG
            print "\nDevice: " + str(self.id) + " login duration finished @ " + str(environment.now)

            # spec: wait the entire time before initiate another upload-download session
            self.status = True
            # yield environment.timeout(session_duration)
            self.status = False
            yield environment.timeout(inter_session_time)

            # DEBUG
            print "\nDevice: " + str(self.id) + " logout duration finished @ " + str(environment.now)

    def upload(self, session_duration, environment, shared_folders, devices, dic_list_files):
        while session_duration >= 0:
            # modify: use a log-normal mu: 3.748 sigma: 2.286
            inter_upload_time = log_norm_computation(3.748, 2.286)
            # spec: wait fot a time (simulating the alternating behaviour of uploads)
            yield environment.timeout(inter_upload_time)
            session_duration -= inter_upload_time
            print "here"
            if verify_remaining_file_to_upload(dic_list_files):
                # spec: select a random  file to upload
                if self.file_pending_up is None:
                    upload_file_id = pick_a_file(dic_list_files)
                    time_upload = dic_list_files[upload_file_id].transfer_time

                    # spec: update the owners of the file (in any case the file will be uploaded by this device)
                    dic_list_files[upload_file_id].owners.append(self.id)
                    if time_upload < session_duration:
                        # spec: simulating the upload
                        yield environment.timeout(time_upload)

                        # spec: update all download lists of devices connected with the SF
                        sf = shared_folders[self.chosen_sf_id]
                        sf.on_upload(upload_file_id, self.id, devices)
                        self.total_uploaded_traffic += dic_list_files[upload_file_id].size
                        session_duration -= time_upload
                    else:
                        try:
                            yield environment.timeout(session_duration)
                        except ValueError:
                            print ""
                        self.file_pending_up = (upload_file_id, time_upload - session_duration)
                        session_duration = -1
                else:
                    # spec: finishing the upload
                    yield environment.timeout(self.file_pending_up[1])
                    # spec: trigger the update information for all devices
                    shared_folders[self.chosen_sf_id].on_upload(self.file_pending_up[0], self.id, devices)
                    session_duration -= self.file_pending_up[1]
                    self.total_uploaded_traffic += dic_list_files[self.file_pending_up[0]].size
                    self.file_pending_up = None
            else:
                # DEBUG
                print "no more files to upload"

    def download(self, session_duration, environment, dic_list_files):
        while session_duration >= 0:
            if len(self.file_to_download) > 0:
                # spec: check if there's a file to download
                print "there"
                if self.file_pending_dw is None:
                    # spec: take the first file and assess if there's enough time to process it
                    file_id = self.file_to_download[0].pop(0)
                    time_to_process = dic_list_files[file_id].transfer_time
                    if time_to_process < session_duration:
                        # spec: update the global information removing the file from the list
                        dic_list_files[file_id].owners.append(self.id)
                        yield environment.timeout(time_to_process)
                        # spec: update global information
                        self.total_downloaded_traffic += dic_list_files[file_id].size
                        session_duration -= time_to_process
                    else:
                        yield environment.timeout(session_duration)
                        self.file_pending_up = (self.file_to_download[0], time_to_process - session_duration)
                        session_duration = -1
                else:
                    # spec: finishing the download
                    yield environment.timeout(self.file_pending_dw[1])
                    dic_list_files[self.file_pending_dw[1]].owners.append(self.id)
                    self.total_downloaded_traffic += dic_list_files[self.file_pending_dw[1]].size
                    session_duration -= self.file_pending_dw[1]
                    self.file_pending_dw = None
            else:
                yield environment.timeout(10)
                session_duration -= 10


def generate_network(num_dv, devices, shared_folders):

    # shared folders per device - negative_binomial (s, mu)
    DV_DG = [0.470, 1.119]

    # device per shared folder - negative_binomial (s, mu)
    SF_DG = [0.231, 0.537]

    # derive the expected number of shared folders using the negative_binomials

    # this piece is just converting the parametrization of the
    # negative_binomials from (s, mu) to "p". Then, we use the rate between
    # the means to estimate the expected number of shared folders
    # from the given number of devices

    dv_s = DV_DG[0]
    dv_m = DV_DG[1]
    dv_p = dv_s / (dv_s + dv_m)
    nd = 1 + (dv_s * (1.0 - dv_p) / dv_p)

    sf_s = SF_DG[0]
    sf_m = SF_DG[1]
    sf_p = sf_s / (sf_s + sf_m)
    dn = 1 + (sf_s * (1.0 - sf_p) / sf_p)

    # the number of shared folders is finally derived
    num_sf = int(num_dv * nd / dn)

    # sample the number of devices per shared folder (shared folder degree)
    sf_dgr = [x + 1 for x in numpy.random.negative_binomial(sf_s, sf_p, num_sf)]

    # sample the number of shared folders per device (device degree)
    dv_dgr = [x + 1 for x in numpy.random.negative_binomial(dv_s, dv_p, num_dv)]

    # create the population of edges leaving shared folders
    l = [i for i, j in enumerate(sf_dgr) for k in range(min(j, num_dv))]
    random.shuffle(l)
    sf_pop = deque(l)

    # create empty shared folders
    for sf_id in range(num_sf):
        shared_folders[sf_id] = skeleton.SharedFolder(sf_id)

    # first we pick a random shared folder for each device
    for dv_id in range(num_dv):
        devices[dv_id] = SimulationDevice(dv_id)

        sf_id = sf_pop.pop()
        devices[dv_id].add_shared_folder(shared_folders[sf_id])
        shared_folders[sf_id].add_device(devices[dv_id])

    # then we complement the shared folder degree

    # we skip devices with degree 1 in a first pass, since they just got 1 sf
    r = 1

    # we might have less edges leaving devices than necessary
    while sf_pop:
        # create the population of edges leaving devices
        l = [i for i, j in enumerate(dv_dgr) for k in range(min(j - r, num_sf))]
        random.shuffle(l)
        dv_pop = deque(l)

        # if we need to recreate the population, we use devices w/ degree 1 too
        r = 0

        while sf_pop and dv_pop:
            dv = dv_pop.pop()
            sf = sf_pop.pop()

            # we are lazy and simply skip the unfortunate repetitions
            if not shared_folders[sf] in devices[dv].my_shared_folders:
                devices[dv].add_shared_folder(shared_folders[sf])
                shared_folders[sf].add_device(devices[dv])
            else:
                sf_pop.append(sf)



if __name__ == "__main__":

    # number of devices in the simulation
    NUM_DEV = 4

    # collection of devices, shared_folders, files_objects
    devices = {}
    shared_folders = {}
    files_object_list = {}

    # create the content sharing network
    generate_network(NUM_DEV, devices, shared_folders)

    # generate file objects
    create_files_object(files_object_list)

    env = simpy.Environment()

    for device in devices.values():
        env.process(device.run(env, shared_folders, devices, files_object_list))

    env.run(until=10000000)
