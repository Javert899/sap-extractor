from plantuml import *
import tempfile
import os
import sys
import shutil


def apply(filename):
    with tempfile.TemporaryDirectory() as dir:
        dir_name = dir
    os.mkdir(dir_name)
    basename = os.path.basename(filename).split(".")[0] + ".png"
    pl = PlantUML(url="http://www.plantuml.com/plantuml/img/")
    pl.processes_file(filename, directory=dir_name, outfile=basename)
    fullpath = os.path.join(dir_name, basename)
    return fullpath


if __name__ == "__main__":
	result = apply(sys.argv[1])
	basename = os.path.basename(result)
	print(basename)
	shutil.copy(result, basename)
