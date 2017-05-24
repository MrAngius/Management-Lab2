import simpy
import random
import numpy


# todo: create a script to load a list of objects of class file, reading the .txt file
def createFileObject():
    with open('C:\Users\Matteo\Desktop\PoliTO\Managment and content delivery\Lab\Lab2\Throughput.txt') as f:
        content = f.readlines()
        # removing whitespace characters like `\n` at the end of each line
    return [x.strip('\n') for x in content]

# todo: include the log-normal function
def logNormComputation(mu, sigma):
    return numpy.random.lognormal(mu, sigma, 1000) #last number is the size for output shape. I don't think is necessary

def device(environment, name):

    while True:
        # modify: use a log-normal mu: 8.492 sigma: 1.545
        session_duration = logNormComputation(8.492,1.545)

        # implement: include the log_in device call (including the selected SF and ID)
        while session_duration > 0:
            # modify: use a log-normal mu: 3.748 sigma: 2.286
            inter_upload_time = logNormComputation(3.748, 2.286)
            # control for the possibility of the last upload

            yield environment.timeout(inter_upload_time)
            print "debug - " + name + " completed the upload @ " + str(environment.now)
            session_duration -= inter_upload_time

        # spec: once out the while, the session is over, the log out is performed by the device
        # implement: include the log_out device call (possible the info of the upload)

        # DEBUG
        print "\nDevice: " + name + " login duration finished @ " + str(environment.now)

        # modify: use a log-normal mu: 7.971 sigma: 1.308
        inter_session_time = random.randint(5, 10)
        yield environment.timeout(inter_session_time)

        # DEBUG
        print "\nDevice: " + name + " logout duration finished @ " + str(environment.now)


class Server:

    def __init__(self, environment, device_dic):
        # spec: the device dictionary is due to the mapping between device and shared folder is required
        self.environment = environment
        self.users_online = device_dic

    # spec: a method to create as many lists as devices
    # todo: think about a possible implementation of this method
    def createDownloadLists(self):
        pass

    # spec: the server should periodically check the online devices and then provide the relative files
    def run(self):
        pass


# todo: define the class File to model the files
class Files:
    def __init__(self):
        pass


if __name__ == "__main__":
    env = simpy.Environment()

    env.process(device(env, "marco"))
    env.process(device(env, "matteo"))
    env.process(device(env, "carla"))

    env.run(until=80)