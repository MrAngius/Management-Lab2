# coding=utf-8
from collections import deque
import simpy
import random
import skeleton
import numpy
import re

global RECOVERY_DW
global RECOVERY_UP
global P2P
global SHUFFLE
global UF


class ObjFile:
    def __init__(self, id, th, size):
        self.id = id
        self.throughput = th
        self.size = size
        self.owners = []
        self.transfer_time = None
        self.uploaded = False

    def _clear_(self):
        self.owners = []
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
                    file_objects[temp_obj.id] = temp_obj
                except ZeroDivisionError:
                    pass
    f.close()


def create_files_object_test(file_objects):
    for id in range(1, 100000):
        temp_obj = ObjFile(id, 100, 150)
        file_objects[id] = temp_obj

def evaluate_th():

    throughput = []
    with open('Throughput.txt') as f:
        pattern = r"\d[0-9.]*"
        content = f.readlines()
        for line in content:
            if re.match(pattern, line):
                result = re.findall(pattern, line)
                if float(result[1]) > 0.0:
                    throughput.append(float(result[1]))

    f.close()
    return sum(throughput)/len(throughput)


def long_trace(file_objects, avg_th, size=100000):
    with open('trace_partial.txt') as f:
        content = f.readlines(size)
        for line in content:
            if int(line.split(" ")[4]) > 0:

                temp_obj = ObjFile(numpy.abs(hash(line)), avg_th, int(line.split()[4]))
                try:
                    temp_obj.transfer_time = int(line.split()[4]) / avg_th
                    file_objects[temp_obj.id] = temp_obj
                except ZeroDivisionError:
                    pass
    f.close()


def log_norm_computation(mu, sigma):
    # last number is the size for output shape. I don't think is necessary
    return numpy.random.lognormal(mu, sigma)


# spec: define how to pick a new file to download
def pick_a_file(dic_list_files, stats):
    stats.total_uploaded_files += 1
    try:
        if not SHUFFLE:
            not_up_files = filter(lambda x: True if not x.uploaded else False, dic_list_files.values())
            index = random.randint(0, len(not_up_files)-1)
            selected_file_id = not_up_files[index].id
            dic_list_files[selected_file_id].uploaded = True
            return selected_file_id
        else:
            list_shuffle = dic_list_files.values()
            random.shuffle(list_shuffle)
            index = random.randint(0, len(list_shuffle)-1)
            selected_file_id = list_shuffle[index].id
            return selected_file_id
    except IndexError:
        # DEBUG
        print "INDEX ERROR"


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
        # spec: reset
        self.file_to_download = []
        self.total_uploaded_traffic = 0.0
        self.total_downloaded_traffic = 0.0
        self.file_pending_up = None
        self.file_pending_dw = None
        self.total_uploaded_p2p = 0.0
        self.total_downloaded_p2p = 0.0
        self.fails = 0
        self.no_availability = 0

    def _clear_(self):
        self.file_to_download = []
        self.total_uploaded_traffic = 0.0
        self.total_downloaded_traffic = 0.0
        self.file_pending_up = None
        self.file_pending_dw = None
        self.total_uploaded_p2p = 0.0
        self.total_downloaded_p2p = 0.0
        self.fails = 0
        self.no_availability = 0

    def run(self, environment, shared_folders, devices, dic_files, stats):
        while True:
            # modify: use a log-normal mu: 8.492 sigma: 1.545
            self.status = True
            # spec: select a random SF on which we'll work during the session
            self.chosen_sf_id = self.my_shared_folders[random.randint(0, len(self.my_shared_folders)-1)].id

            session_duration = log_norm_computation(8.492, 1.545)
            # modify: use a log-normal mu: 7.971 sigma: 1.308
            inter_session_time = log_norm_computation(7.971, 1.308)
            # DEBUG
            # print "\nDevice: " + str(self.id) + " login duration  @ " + str(environment.now)

            # spec: we consider both handled by the device using global information
            environment.process(self.upload(session_duration, environment, shared_folders, devices, dic_files, stats))
            environment.process(self.download(session_duration, environment, dic_files, devices, stats))

            yield environment.timeout(session_duration)

            # DEBUG
            # print "\nDevice: " + str(self.id) + " logout duration  @ " + str(environment.now)

            # spec: wait the entire time before initiate another upload-download session

            # yield environment.timeout(session_duration)
            self.status = False
            yield environment.timeout(inter_session_time)

    def upload(self, session_duration, environment, shared_folders, devices, dic_list_files, stats):
        while session_duration >= 0:
            # modify: use a log-normal mu: 3.748 sigma: 2.286
            inter_upload_time = log_norm_computation(3.748, 2.286)
            # spec: wait fot a time (simulating the alternating behaviour of uploads)
            yield environment.timeout(inter_upload_time)
            session_duration -= inter_upload_time

            if verify_remaining_file_to_upload(dic_list_files) and session_duration > 0:
                # spec: select a random  file to upload
                if self.file_pending_up is None:

                    try:
                        upload_file_id = pick_a_file(dic_list_files, stats)
                    except IndexError:
                        print "INDEX OUT OF RANGE"

                    time_upload = dic_list_files[upload_file_id].transfer_time

                    # spec: update the owners of the file (in any case the file will be uploaded by this device)
                    dic_list_files[upload_file_id].owners.append(self.id)
                    if time_upload < session_duration:
                        # spec: simulating the upload

                        try:
                            yield environment.timeout(time_upload * UF)
                        except ValueError:
                            print ""

                        # DEBUG
                        # print "\nDevice: " + str(self.id) + " UPLOADED " + str(upload_file_id) + \
                        #       " @ " + str(environment.now)

                        # spec: update all download lists of devices connected with the SF
                        sf = shared_folders[self.chosen_sf_id]
                        sf.on_upload(upload_file_id, self.id, devices)
                        self.total_uploaded_traffic += dic_list_files[upload_file_id].size
                        session_duration -= time_upload
                    else:
                        # spec: enable or not the possibility to finish the upload at the next session
                        if RECOVERY_UP:
                            try:
                                yield environment.timeout(session_duration)
                            except ValueError:
                                print ""
                            self.file_pending_up = (upload_file_id, time_upload * UF - session_duration,
                                                    self.chosen_sf_id)
                            session_duration = -1
                        else:
                            # spec: the procedure it's the same, instead it will restart waiting the all upload time
                            try:
                                yield environment.timeout(session_duration)
                            except ValueError:
                                print ""
                            self.file_pending_up = (upload_file_id, time_upload * UF, None)
                            session_duration = -1

                if self.file_pending_up is not None:
                    # spec: finishing the upload
                    pending = self.file_pending_up
                    try:
                        yield environment.timeout(pending[1])
                        # DEBUG
                        # print "\nDevice: " + str(self.id) + " UPLOADED " + str(self.file_pending_up[0]) + \
                        #       " @ " + str(environment.now)

                        # spec: trigger the update information for all devices
                        # spec: we need a trick due to we have already selected a file to upload, so in the next session
                        # spec: the file we'll be uploaded in the next randomly chosen folder

                        if pending[2] is not None:
                            shared_folders[pending[2]].on_upload(pending[0], self.id, devices)
                        else:
                            shared_folders[self.chosen_sf_id].on_upload(pending[0], self.id, devices)
                    except TypeError:
                        print "TYPE ERROR"

                    session_duration -= pending[1]
                    self.total_uploaded_traffic += dic_list_files[pending[0]].size
                    self.file_pending_up = None
            else:
                # DEBUG
                if not verify_remaining_file_to_upload(dic_list_files):
                    print ">>>>>>>>>>>>>>>>>>>>NO MORE FILES TO UPLOAD"

    def download(self, session_duration, environment, dic_list_files, devices, stats):
        while session_duration >= 0:
            if len(self.file_to_download) > 0:
                # spec: check if there's a file to download
                if self.file_pending_dw is None:
                    # spec: take the first file and assess if there's enough time to process it
                    file_id = self.file_to_download[0]

                    selected_peer = None
                    fail = False
                    # implement: P2P
                    if P2P:
                        for device in dic_list_files[file_id].owners:
                            # spec: save the id of the first active peer
                            if devices[device].status:
                                selected_peer = device
                                break

                        # spec: check if at least is active and has file
                        if selected_peer is not None:
                            time_to_process = dic_list_files[file_id].transfer_time
                            if time_to_process < session_duration:
                                # spec: update the global information removing the file from the list
                                #  dic_list_files[file_id].owners.append(self.id)
                                yield environment.timeout(time_to_process)
                                session_duration -= time_to_process

                                if devices[selected_peer].status:
                                    dic_list_files[file_id].owners.append(self.id)
                                    self.total_downloaded_traffic += dic_list_files[file_id].size
                                    self.file_to_download.pop(0)
                                    # spec: update global stats
                                    self.total_downloaded_p2p += dic_list_files[file_id].size
                                    devices[selected_peer].total_uploaded_p2p += dic_list_files[file_id].size
                                    stats.total_downloaded_files += 1
                                    # DEBUG
                                    # print "\nPeer: " + str(self.id) + " P2P " + str(file_id) + \
                                    #       " @ " + str(environment.now) + " from peer" + str(selected_peer)
                                else:
                                    fail = True
                                    self.fails += 1
                                    # DEBUG
                                    # print "\n>>>>>>>>>>>>>>>>>>>Peer: " + str(self.id) + " FAILED CHURN"
                                    #  + str(file_id) + \
                                    #       " @ " + str(environment.now) + " from peer" + str(selected_peer)
                            else:
                                fail = True
                                # DEBUG
                                # print "\nPeer: " + str(self.id) + " IMPOSSIBLE TIME" + str(file_id) + \
                                #       " @ " + str(environment.now) + " from peer" + str(selected_peer)
                        else:
                            fail = True
                            self.no_availability += 1
                        # DEBUG
                        # print "\nPeer: " + str(self.id) + " NO PEERS" + str(file_id) + \
                        #       " @ " + str(environment.now)

                    # spec: if not possible using P2P
                    if fail or not P2P:
                        # spec: update global information

                        time_to_process = dic_list_files[file_id].transfer_time
                        if time_to_process < session_duration:
                            # spec: update the global information removing the file from the list
                            dic_list_files[file_id].owners.append(self.id)
                            yield environment.timeout(time_to_process)
                            self.file_to_download.pop(0)
                            # spec: update global information
                            self.total_downloaded_traffic += dic_list_files[file_id].size
                            session_duration -= time_to_process
                            stats.total_downloaded_files += 1

                            # DEBUG
                            # print "\n@@@@@@@@@@@@@@@@@Device: " + str(self.id) + " DOWNLOADED " + str(file_id) + \
                            #       " @ " + str(environment.now)

                        else:
                            # spec: as for the upload we enable or not the recovery of the download
                            if RECOVERY_DW:
                                yield environment.timeout(session_duration)
                                self.file_pending_dw = (self.file_to_download[0], time_to_process - session_duration)
                                self.file_to_download.pop(0)
                                session_duration = -1
                            else:
                                yield environment.timeout(session_duration)
                                self.file_pending_dw = (self.file_to_download[0], time_to_process)
                                self.file_to_download.pop(0)
                                session_duration = -1
                else:
                    # spec: finishing the download
                    pending = self.file_pending_dw
                    yield environment.timeout(pending[1])
                    # DEBUG
                    # print "\nDevice: " + str(self.id) + " DOWNLOADED " + str(self.file_pending_dw[0]) + \
                    #       " @ " + str(environment.now)
                    try:
                        dic_list_files[pending[0]].owners.append(self.id)

                        self.total_downloaded_traffic += dic_list_files[pending[0]].size
                        session_duration -= pending[1]
                        self.file_pending_dw = None
                        stats.total_downloaded_files += 1

                    except TypeError:
                        print "TYPE ERROR"
            else:
                yield environment.timeout(1)
                session_duration -= 1


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


class Statistics:

    def __init__(self):
        self.active_devices = []
        self.total_uploaded_files = 0
        self.total_downloaded_files = 0

    def deviceAverage(self):
        total_area = 0.0
        old_sample = self.active_devices[0][1]
        last_time = self.active_devices[0][0]

        for sample in self.active_devices:
            delta_time = sample[0] - last_time
            total_area += old_sample * delta_time
            old_sample = sample[1]
            # update the last time
            last_time = sample[0]

        return total_area / (self.active_devices[-1][0] - self.active_devices[0][0])

    @staticmethod
    def compute_server_throughput(devices, sim_time):
        upload = 0
        download = 0

        for device in devices.values():
            upload += device.total_uploaded_traffic
            download += device.total_downloaded_traffic
        return upload/sim_time, download/sim_time, upload/(upload+download), download/(download+upload)

    @staticmethod
    def compute_p2p_throughput(devices, sim_time):
        upload = 0
        download = 0

        for device in devices.values():
            upload += device.total_uploaded_p2p
            download += device.total_downloaded_p2p
        return download/sim_time, upload/sim_time

    @staticmethod
    def number_fails(devices):
        fails = 0
        for device in devices.values():
            fails += device.fails
        return fails

    @staticmethod
    def number_no_availability(devices):
        no_av = 0
        for device in devices.values():
            no_av += device.no_availability
        return no_av

    @staticmethod
    def remaining_downloads(devices):
        dw_rm = 0
        for device in devices.values():
            dw_rm += len(device.file_to_download)

        return dw_rm


def number_active_device(environment, devices, stats):
    while True:
        active_dev = 0
        for device in devices.values():
            if device.status:
                active_dev += 1

        stats.active_devices.append((environment.now, active_dev))

        yield environment.timeout(10)



def clear_trace_file(trace):
    for obj_file in trace.values():
        obj_file._clear_()


if __name__ == "__main__":

    # number of devices in the simulation
    NUM_DEV = [800, 1600]
    SHUFFLE = True
    SIM_TIME = 10000
    NUM_SIM = 4
    SIZE_TRACE = 100000
    STORE = True
    TRACE = True
    TEST = False
    UF = 1

    print "\nLOADING THE TRACE..."

    files_object_list = {}
    if TRACE:
        long_trace(files_object_list, evaluate_th(), SIZE_TRACE)
    elif TEST:
        create_files_object_test(files_object_list)
    else:
        create_files_object(files_object_list)

    for mode in [False]:
        P2P = mode

        for n_dev in NUM_DEV:

            for recovery in [False, True]:

                RECOVERY_DW = recovery
                RECOVERY_UP = recovery

                if STORE:
                    try:
                        text = "SIM_M" + str(P2P).lower() + "D" + str(n_dev) + "R" + str(recovery).lower() + \
                               "T" + str(TRACE).lower() + ".csv"
                        f = open(text, "w")
                    except IOError:
                        print "I/O ERROR"
                        f.close()

                for i in range(NUM_SIM):

                    # collection of devices, shared_folders, files_objects
                    devices = {}
                    shared_folders = {}

                    # create the content sharing network
                    generate_network(n_dev, devices, shared_folders)

                    # regenerate the trace sim
                    print "\nRESETTING THE TRACE..."
                    clear_trace_file(files_object_list)

                    print "\n\n>>>>> SIM " + str(i).zfill(2) + "  ####  NUM DEV: " + str(n_dev) + \
                          " - P2P: " + str(P2P) + " - FILE SHUFFLE: " + str(SHUFFLE) + " - RECOVERY DW: " + \
                          str(RECOVERY_DW) + " - RECOVERY UP: " + str(RECOVERY_UP) + " - TRACE: " + str(TRACE)

                    if STORE:
                        f.write("SIM " + str(i).zfill(2) + ",NUM DEV: " + str(NUM_DEV) + \
                                ",P2P: " + str(P2P) + ",FILE SHUFFLE: " + str(SHUFFLE) + ",RECOVERY DW: " + \
                                str(RECOVERY_DW) + ",RECOVERY UP: " + str(RECOVERY_UP) +
                                ", TRACE: " + str(TRACE) + "\n")

                    stats = Statistics()
                    env = simpy.Environment()

                    for device in devices.values():
                        env.process(device.run(env, shared_folders, devices, files_object_list, stats))

                    env.process(number_active_device(env, devices, stats))
                    env.run(until=SIM_TIME)

                    print "\n"
                    print "average devices: " + str(stats.deviceAverage())
                    temp = stats.compute_server_throughput(devices, SIM_TIME)
                    print "upload_th: " + str(temp[0]) + " -- downloaded_th: " + str(temp[1])
                    print "upload_p: " + str(temp[2]) + " -- downloaded_p: " + str(temp[3])
                    temp = stats.compute_p2p_throughput(devices, SIM_TIME)
                    print "P2P_dw_th: " + str(temp[0]) + "  -- P2P_up_th: " + str(temp[1])
                    print "number fails_churn: " + str(stats.number_fails(devices))
                    print "number fails_availability: " + str(stats.number_no_availability(devices))
                    print "downloaded_files: " + str(stats.total_downloaded_files)
                    print "uploaded files: " + str(stats.total_uploaded_files)
                    print "remaining total dw: " + str(stats.remaining_downloads(devices))

                    if STORE:
                        f.write("average_users," + str(stats.deviceAverage()) + "\n")
                        temp = stats.compute_server_throughput(devices, SIM_TIME)
                        f.write("upload_th," + str(temp[0]) + "\n")
                        f.write("download_th," + str(temp[1]) + "\n")
                        f.write("upload_percentage," + str(temp[2]) + "\n")
                        f.write("download_percentage," + str(temp[3]) + "\n")

                        temp = stats.compute_p2p_throughput(devices, SIM_TIME)
                        f.write("download_p2p," + str(temp[0]) + "\n")
                        f.write("upload_p2p," + str(temp[1]) + "\n")

                        f.write("fail_churn," + str(stats.number_fails(devices)) + "\n")
                        f.write("no_availability," + str(stats.number_no_availability(devices)) + "\n")

                        f.write("total_dw," + str(stats.total_downloaded_files) + ",total_up,"
                                + str(stats.total_uploaded_files) + "\n")

                        f.write("END SIM\n")
                        f.write("\n")
