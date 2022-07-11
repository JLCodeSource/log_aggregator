import os
import zipfile
import fileinput
import sys
from pathlib import Path
from shutil import move

# Vars
sourcedir = "./source"
outdir = "./out"

for file in os.listdir(sourcedir):
    # Extract node name from example:
    #GBLogs_node.domain.tld_fanapiservice_1657563223771.zip
    node = file.split("_")[1].split(".")[0]
    # Extract logtype
    logtype = file.split("_")[2]

    logsout = os.path.join(outdir, node, logtype)
    Path(logsout).mkdir(parents=True, exist_ok=True)

    if file.endswith(".zip"):
        with zipfile.ZipFile(os.path.join(sourcedir, file), 'r') as zip_file:
            filesInZip = zip_file.namelist()
            for filename in filesInZip:
                if filename.endswith('.log'):
                    zip_file.extract(filename, logsout)

    # Move log files out of System folder where they are by default
    tmplogsout = os.path.join(logsout, "System")
    for filename in os.listdir(tmplogsout):
        move(os.path.join(tmplogsout, filename),
             os.path.join(logsout, filename))
    os.rmdir(tmplogsout)

    for logfile in os.listdir(logsout):
        prepend = node + '\t| '

        for line in fileinput.input([os.path.join(logsout, logfile)],
                                    inplace=True):
            sys.stdout.write("{prepend}{line}".format(
                prepend=prepend, line=line))
