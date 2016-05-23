import sys
from subprocess import call, Popen

call("python get_cyclone_members.py", shell = True)
call("python geoprocess_members.py", shell = True)
call("python drop_members.py", shell = True)
call("python exposure_summary.py", shell = True)
