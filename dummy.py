import simpy
import random


def device(env, name):

    while True:
        session_duration = random.randint(15, 20)

        while session_duration > 0:
            inter_upload_time = random.randint(1, 4)
            # control for the possibility of the last upload

            yield env.timeout(inter_upload_time)
            print "debug - " + name + " completed the upload @ " + str(env.now)
            session_duration -= inter_upload_time

        print "\nDevice: " + name + " login duration finished @ " + str(env.now)
        inter_session_duration = random.randint(5, 10)
        yield env.timeout(inter_session_duration)
        print "\nDevice: " + name + " logout duration finished @ " + str(env.now)


if __name__ == "__main__":
    env = simpy.Environment()

    env.process(device(env, "marco"))
    env.process(device(env, "matteo"))
    env.process(device(env, "carla"))

    env.run(until=80)