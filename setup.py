from distutils.core import setup

setup(
	name='slurpy',
	version='0.1.0a1',
	author='Morten Piibeleht',
	author_email='morten.piibeleht@cern.ch',
	#packages=[],
	scripts=['bin/slur.py','bin/slurmit.py'],
	url='https://github.com/mortenpi/slurpy',
	license='MIT',
	description='A SLURM prequeue.',
	long_description=open('README.md').read()
)
