#!/usr/bin/env python
import subprocess

import slur
db = slur.SlurDB()

targetkey = ('main','morten','PD')
targetMax = 100

p=subprocess.Popen(['squeue', '-h'], stdout=subprocess.PIPE)
(squeue,_) = p.communicate()
counter = {targetkey:0}
for ln in squeue.split('\n')[:-1]:
	ln=ln.split()
	key = (ln[1],ln[3],ln[4])
	if key in counter:
		counter[key] += 1
	else:
		counter[key] = 1

remaining = targetMax-counter[targetkey]
print 'Slots available:', remaining

if remaining<=0:
	print 'No slots available!'
	exit(0)

for i in range(0,remaining):
	nj = db.next_job()
	if nj is None:
		print 'No more jobs!'
		exit(0)
	print 'Setting CWD to:', nj['cwd']
	
	cmd  = ['sbatch']
	cmd += nj['slurmargs']
	cmd += [nj['script']]
	cmd += nj['args']
	print 'CMD list:', cmd
	print 'Command:', ' '.join(cmd)
	p=subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=nj['cwd'], env=nj['env'])
	(stdout,stderr) = p.communicate()
	if len(stderr) > 0:
		print '-------------- STDERR(sbatch) --------------'
		print stderr
		print '--------------------------------------------'
	print 'RETURN CODE:', p.returncode
	if p.returncode == 0:
		slurmjobid=stdout.split()[3]
		print 'SLURM JOBID:', slurmjobid
		db.submit(nj['id'], slurmjobid)
	else:
		print 'ERROR: bad return code!'
