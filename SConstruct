# Preliminaries
import os
import sys
import atexit
import source.lib.JMSLab as jms

sys.path.append('config')
sys.dont_write_bytecode = True # Don't write .pyc files

os.environ['PYTHONPATH'] = '.'
env = Environment(ENV = {'PATH' : os.environ['PATH']},
                  IMPLICIT_COMMAND_DEPENDENCIES = 0,
                  BUILDERS = {'R'         : Builder(action = jms.build_r),
                              'Tablefill' : Builder(action = jms.build_tables),
                              'Stata'     : Builder(action = jms.build_stata),
                              'Matlab'    : Builder(action = jms.build_matlab),
                              'Python'    : Builder(action = jms.build_python),
                              'Lyx'       : Builder(action = jms.build_lyx),
                              'Latex'     : Builder(action = jms.build_latex)})

env.Decider('MD5-timestamp') # Only computes hash if time-stamp changed
Export('env')

jms.start_log('develop', '')

SConscript('source/derived/SConscript')
#SConscript('source/analysis/SConscript')
#SConscript('source/tables/SConscript')

### FROM https://stackoverflow.com/a/43850388
screen = open('/dev/tty', 'w')
node_count = 0
node_count_max = 0
node_count_interval = 1
node_count_fname = str(env.Dir('#')) + '/.scons_node_count'

def progress_function(node):
    global node_count, node_count_max, node_count_interval, node_count_fname
    node_count += node_count_interval
    if node_count > node_count_max: node_count_max = 0
    if node_count_max>0 :
        screen.write('\r[%3d%%] ' % (node_count*100/node_count_max))
        screen.flush()

def progress_finish(target, source, env):
    global node_count
    with open(node_count_fname, 'w') as f: f.write('%d\n' % node_count)

try:
    with open(node_count_fname) as f: node_count_max = int(f.readline())
except: pass
Progress(progress_function, interval=node_count_interval)

progress_finish_command = Command('progress_finish', [], progress_finish)
Depends(progress_finish_command, BUILD_TARGETS)
if 'progress_finish' not in BUILD_TARGETS:     
    BUILD_TARGETS.append('progress_finish')