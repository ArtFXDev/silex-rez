from subprocess import PIPE, STDOUT, Popen

import tractor.api.author as author

def job():
    cmd = "rez env redshift_license_client -- python -m redshift_license_client.main list"
    p = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
    output = p.stdout.read()
    lines = str(output).split("\\n")

    members = [l.split(" ") for l in lines]
    blades = []

    skyrace = [
        "i7-mk11-2020-03",
        "i7-mk11-2020-88",
        "i7-mk11-2020-87",
        "i7-mk11-2020-95",
        "i7-mk11-2020-93",
        "md13-2021-004",
        "md13-2021-007",
    ]

    for m in members:
        if len(m) > 2 and m[2] == "True":
            blades.append(m[3].rstrip("\\r"))

    blades = [b for b in blades if b not in skyrace]

    print("RELEASING LICENSES")
    print(blades)

    job = author.Job(title="Redshift release license", priority=100000)

    for blade in blades:
        job.newTask(
            title=blade,
            argv="rez env redshift_license_client -- python -m redshift_license_client.main stop".split(
                " "
            ),
            service=blade,
        )

    jid = job.spool(owner="jhenry")
    print(f"JOB SPOOLED {jid}")

if __name__ == "__main__":
    job()