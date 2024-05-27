import time

import asyncio

import docker
import scheduler_logger


# run a bench as container with default arguments
def run_container(
    client, name: str, num_threads: int = 0, cpus: str = str(), cpus_index: int = 0
):
    # there are two different benchmark sets, one is just radix, rest is parsec
    suite = "splash2x" if name == "radix" else "parsec"
    cpus = schedule[name][cpus_index] if cpus == "" else cpus
    num_threads = (
        len(schedule[name][cpus_index].split(",")) if num_threads == 0 else num_threads
    )

    return client.containers.run(
        image=images[name],
        command=f"./run -a run -S {suite} -p {name} -i native -n {num_threads}",
        auto_remove=False,  # if True, removes the container as soon as it's done
        # set the cpu cores the container is allowed to run on, as a comma separated
        cpuset_cpus=cpus,
        # if True, run container in the background. otherwise wait for containier
        # to finish
        detach=True,
        name=name,
        # sets the relative cpu shares. default is 1024, gets added with other
        # containers to get a cpu share for each.
        # this is a soft constraint, if cpu time is not limited then
        # it will freely be used
        cpu_shares=2,
    )


# pull all necessary docker images
def pull_images(client):
    for image in images.values():
        client.images.pull(image)


# check if any of the cpu configurations for a bench are currently
# runnable
def can_run(bench: str):
    for i, cpus in enumerate(schedule[bench]):

        b_cores = [int(c) for c in cpus.split(",")]
        for j in b_cores:
            if cores[j]:
                return True, i
    return False, -1


# finds and runs the job that should run next if there is one, otherwise returns None
def run_next_job(
    client, benchmarks: list[str], logger: scheduler_logger.SchedulerLogger
):
    for bench in benchmarks:
        # for each core configuration, check if it's runnable
        c_r, r_ind = can_run(bench)
        if c_r:
            # get arguments for container and run it
            cpus = schedule[bench][r_ind].split(",")
            num_threads = len(cpus)
            logger.job_start(scheduler_logger.Job(bench), cpus, num_threads)
            benchmarks.remove(bench)
            for core in cpus:
                cores[int(core)] = False
            return run_container(
                client,
                bench,
                num_threads=num_threads,
                cpus=schedule[bench][r_ind],
            )
    return None


# all remote images for the benchmarks
images = {
    "blackscholes": "anakli/cca:parsec_blackscholes",
    "canneal": "anakli/cca:parsec_canneal",
    "dedup": "anakli/cca:parsec_dedup",
    "ferret": "anakli/cca:parsec_ferret",
    "freqmine": "anakli/cca:parsec_freqmine",
    "radix": "anakli/cca:splash2x_radix",
    "vips": "anakli/cca:parsec_vips",
}

# non-total (is that the word? idk not all are comparable) ordering of jobs along with
# what cores they can run on. can be multiple possible configurations
schedule = {
    "freqmine": ["0,1,2,3"],
    "canneal": ["2,3"],
    "ferret": ["0,1"],
    "blackscholes": ["2,3"],
    "dedup": ["2,3"],
    "radix": ["2,3"],
    "vips": ["0,1", "2,3"],
}

# symbolizes availability of cpu cores
cores = [True, True, True, True]


async def main():
    # get docker client
    client = docker.from_env()

    # stop and remove any leftover running containers
    for container in client.containers.list(all=True):
        container.stop()
        container.remove()

    # pull remote docker images before starting measurements
    pull_images(client)
    # start logger and log memcached
    logger = scheduler_logger.SchedulerLogger()
    logger.job_start(scheduler_logger.Job("memcached"), ["0", "1"], 2)

    # get the list of jobs in correct execution order
    benchmarks = list(schedule.keys())  # list of jobs that still need to be run

    running = []  # holds the running containers

    # if any jobs are still running or if we have any jobs that we still need to start
    while running or benchmarks:
        for job in running:
            # get recent information about the job
            job.reload()

            # check if the container is finished running
            if job.status == "exited":
                # get the cores that the container was using, set them to True to
                # symbolize that they're available now
                freed_cores = job.attrs["HostConfig"]["CpusetCpus"]
                for core in freed_cores.split(","):
                    cores[int(core)] = True

                # log the job completion and remove it from running
                # processes since it's done
                logger.job_end(scheduler_logger.Job(job.name))
                running.remove(job)

        # if any cores on the machine are currently available
        if any(cores):
            # start the next job in line that can run on currently free cores,
            # if such a job exists then it will be started and we add it to running
            next_job = run_next_job(client, benchmarks, logger)
            if next_job is not None:
                running.append(next_job)

            # make sure we're not fully done and there's still something running
            elif running:
                # arbitrarily get the least recently started job and give it all
                # currently available cores
                last_job = running[-1]
                last_job_cpus = last_job.attrs["HostConfig"]["CpusetCpus"]
                free_cores = [i for i, b in enumerate(cores) if b]
                for i in free_cores:
                    cores[i] = False
                new_cpus = last_job_cpus.split(",") + [str(i) for i in free_cores]
                # add free cores to running container, update it
                last_job.update(cpuset_cpus=",".join(new_cpus))
                logger.update_cores(scheduler_logger.Job(last_job.name), new_cpus)
        time.sleep(1)
    logger.end()


if __name__ == "__main__":
    asyncio.run(main())
