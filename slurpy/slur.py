import sqlite3
import os
import os.path
import pickle
from datetime import datetime

class SlurDB:
	def __init__(self):
		self._db = sqlite3.connect(os.path.expanduser('~/slurpy.db'), detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
		self._db.row_factory=sqlite3.Row
		self._create_database()
	def close(self):
		self._db.close()
	def _create_database(self):
		self._db.execute("CREATE TABLE IF NOT EXISTS 'queue' (id INTEGER PRIMARY KEY, env INTEGER, script TEXT, args BLOB, slurmargs BLOB, instances INTEGER DEFAULT 1, submitted INTEGER DEFAULT 0)")
		self._db.execute("CREATE TABLE IF NOT EXISTS 'env' (id INTEGER PRIMARY KEY, cwd TEXT)")
		self._db.execute("CREATE TABLE IF NOT EXISTS 'env_var' (env INTEGER, name TEXT, value TEXT)")
		self._db.execute("CREATE TABLE IF NOT EXISTS 'job' (slurmid INTEGER, queue INTEGER, submit TIMESTAMP)")
		self._db.commit()
	def write_env(self, cwd, env):
		envid=self._db.execute("INSERT INTO env(cwd) VALUES (?)", (cwd,)).lastrowid
		q = "INSERT INTO env_var(env,name,value) VALUES ({0:d}, ?, ?)".format(envid)
		self._db.executemany(q, env.iteritems())
		self._db.commit()
		return envid
	def read_env(self, id):
		cwd=self._db.execute("SELECT cwd FROM env WHERE id=?", (id,)).fetchone()[0]
		env=dict(self._db.execute("SELECT name,value FROM env_var WHERE env=?", (id,)).fetchall())
		return cwd,env
	def queue_job(self, script, args, sbatch_args=[], N=1, env=os.environ, cwd=None):
		cwd = os.getcwd() if cwd is None else cwd
		envid=self.write_env(cwd, env)
		self._db.execute("INSERT INTO queue(script,args,slurmargs,env,instances) VALUES(?,?,?,?,?)",
			(script, pickle.dumps(args), pickle.dumps(sbatch_args), envid, N))
		self._db.commit()
	def next_job(self):
		jr=self._db.execute("SELECT * FROM queue WHERE submitted<instances ORDER BY id ASC LIMIT 1").fetchone()
		if jr is None:
			return None
		jr=dict(jr)
		jr['script'] = str(jr['script'])
		jr['args'] = pickle.loads(str(jr['args']))
		jr['slurmargs'] = pickle.loads(str(jr['slurmargs']))
		jr['envid']=jr['env']
		jr['cwd'],jr['env'] = self.read_env(jr['envid'])
		return jr
	def submit(self, id, slurmid):
		self._db.execute("UPDATE queue SET submitted=submitted+1 WHERE id=?", (id,))
		self._db.execute("INSERT INTO job(queue,slurmid,submit) VALUES (?, ?, ?)", (id,slurmid,datetime.today()))
		self._db.commit()
