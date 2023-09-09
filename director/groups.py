import subprocess, time, textwrap, traceback, os, sys, signal
from subprocess import Popen
from textwrap import indent
from pythreader import Task, Primitive, synchronized, TaskQueue


class Step(Primitive):
    
    LevelIndent = "  "
    LogLock = Primitive()

    def __init__(self, config, env, level):
        self.Title = config.get("title")
        Primitive.__init__(self, name=self.Title)
        self.Killed = False
        self.Env = env
        self.Status = None
        self.StartT = self.EndT = self.Elapsed = None
        self.Level = level
        self.Indent = self.LevelIndent * level
        self.RunEnv = None
        self.ExitCode = 0

    def run(self, quiet = False):
        self.StartT = time.time()
        self.Status = self._run(quiet)
        self.EndT = time.time()
        self.Elapsed = self.EndT - self.StartT
        return self.Status             # "ok" or "failed" or "cancelled"
        
    def kill(self):
        raise NotImplementedError()
        
    def killed(self):
        self.Status = "killed"
        self.ExitCode = None
        self.Killed = True
        
    def exception(self, exc_type, exc_value, tb):
        self.Exception = (exc_type, exc_value, tb)
        
    def format_status(self, indent=""):
        raise NotImplementedError()

    @property
    def is_killed(self):
        return self.Killed
        
    def parse_env(self, config):
        if "env" not in config:
            return None
        env = {}
        for name, value in config.get("env", {}).items():
            v = os.environ.get(name, "")
            while "$" + name in value:
                value = value.replace("$" + name, v) 
            env[name] = value
        return env
        
    def combine_env(self, env):
        env = (env or {}).copy()
        for name, value in (self.Env or {}).items():
            if "$" + name in value:
                v = env.get(name, "")
                value = value.replace("$" + name, v)
            env[name] = value
        return env
        
    def indent(self, text, extra_indent = ""):
        return indent(text, self.Indent + extra_indent)

    LogOffset = "                          "

    def log(self, *parts, timestamp = False, **kv):
        if not parts:
            parts = [""]
        #if indent is None:
        #    indent = self.Indent
        #indent = self.Indent + ("" if timestamp else self.LogOffset)
        #offset = "" if timestamp else self.LogOffset
        indent = self.Indent + ("" if timestamp else self.LogOffset)
        t = time.time()
        parts = [str(p) for p in parts]
        if indent and parts:
            parts[0] = textwrap.indent(parts[0], indent)
        with self.LogLock:
            if timestamp:
                print("%s:" % (time.ctime(t),), *parts, **kv)
            else:
                print(*parts, **kv)
                

    @staticmethod
    def pretty_time(t):
        fs = t - int(t/60)*60
        t = int(t)
        h = t//3600
        m = (t % 3600) // 60
        s = int(t % 60)
        if t > 3600:
            return(f"{h}h {m}m")
        elif t > 60:
            return(f"{m}m {s}s")
        else:
            return("%.2fs" % (fs,))

class StepTask(Task):
    
    def __init__(self, step, quiet):
        Task.__init__(self)
        self.Step = step
        self.Quiet = quiet
        
    def run(self):
        return self.Step.run(self.Quiet)

class Command(Step):
    
    def __init__(self, config, env, level, command):
        Step.__init__(self, config, env, level)
        self.Command = command
        self.Title = self.Title or self.Command
        self.Process = None
        self.Out = None
        self.Err = None
        
    @synchronized
    def dump_state(self):
        status = self.Status if self.Status else (
            "running" if self.Process is not None
            else "pending"
        )
        return {"type":"command", "status":status, "title":self.Title}

    def update_run_env(self, outer):
        self.RunEnv = self.combine_env(outer)

    def __str__(self):
        process = self.Process
        pid = process.pid if process is not None else ""
        return f"Command {self.Title}"
        
    def _run(self, quiet):
        with self:
            if self.is_killed:
                return self.Status
            else:
                t0 = time.time()
                self.Process = Popen(self.Command, shell=True,
                            stdin=subprocess.DEVNULL,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            env=self.RunEnv, process_group=0)
                if not quiet:
                    self.log("started:", self.Title, "pid:", self.Process.pid, timestamp=True)
        try:
            out, err = self.Process.communicate()
        except:
            out = err = b""
        t1 = time.time()
        self.Out = out.decode("utf-8")
        self.Err = err.decode("utf-8")
        self.ExitCode = self.Process.returncode
        status = "ok"
        if self.is_killed:
            status = "killed"
            self.ExitCode = None
        elif self.ExitCode:
            status = "failed"
        self.Status = status

        if not quiet:
            self.log("%s command:" % ("done" if self.Status=="ok" else "failed",), self.Title, timestamp=True)
            self.log("status:", self.Status, "exit code:", self.ExitCode)
            self.log("elapsed time:", self.pretty_time(t1 - t0))
            out = out.strip()
            err = err.strip()
    
            if out:
                self.log()
                self.log("-- stdout: ------")
                self.log(self.Out)
                self.log("-----------------")
            if err:
                self.log()
                self.log("-- stderr: ------")
                self.log(self.Err)
                self.log("-----------------")

        return self.Status
        
    @synchronized
    def kill(self):
        #print("Command.kill(): self.Killed:", self.Killed, "  self.Process:", self.Process)
        if not self.Killed and self.Process is not None:
            #print("Command.kill()...")
            try:    
                os.killpg(self.Process.pid, signal.SIGINT)
                self.Process.kill()
                self.Process.communicate()
            except:
                #print("exception killing command:", self)
                traceback.print_exc()
                pass
        self.killed()

class ParallelGroup(Step):
    
    def __init__(self, config, env, level, steps=[]):
        Step.__init__(self, config, env, level)
        self.Title = self.Title or "parallel group #%04x" % (id(self) % 256,)
        self.Queue = TaskQueue(config.get("multiplicity", 5), delegate=self)
        self.Steps = steps
        self.ShotDown = False

    @synchronized
    def dump_state(self):
        running = self.Queue.activeTasks()
        running_steps = [task.Step for task in running]
        steps = []
        for step in self.Steps:
            step_dump = step.dump_state()
            if step in running_steps:
                step_dump["status"] = "running"
            elif step.Status is None:
                step_dump["status"] = "pending"
            steps.append(step_dump)
        return {"type":"sequential", "status":self.Status, "title":self.Title, "steps":steps}
        
    def update_run_env(self, outer):
        self.RunEnv = self.combine_env(outer)
        for step in self.Steps:
            step.update_run_env(self.RunEnv)

    @synchronized
    def taskFailed(self, queue, task, exc_type, exc_value, tb):
        step = task.Step
        self.log(self.Indent + f"EXCEPTION in {step.Title}:", timestamp=True)
        traceback.print_exc(exc_type, exc_value, tb)
        self.log("")
        self.Failed = True
        if step.ExitCode is not None:
            self.ExitCode = step.ExitCode
        self.shutdown()

    @synchronized
    def taskEnded(self, queue, task, status):
        step = task.Step
        #print("step ended:", status, "code:", step.ExitCode)
        if status != "ok":
            self.Status = "failed"
            if not self.ShotDown:
                self.shutdown()
            if status != "killed" and step.ExitCode is not None:
                self.ExitCode = step.ExitCode
        #print("step ended: self.ExitCode ->", self.ExitCode)
    
    def kill(self):
        self.shutdown()
        self.Status = "killed"
    
    @synchronized
    def shutdown(self):
        #print("stopping:", self.Title)
        self.Queue.hold()
        for task in self.Queue.waitingTasks():
            self.Queue.cancel(task)
        for task in self.Queue.activeTasks():
            step = task.Step
            if not step.Killed and step.Status is None:
                #print("killing:", task)
                step.kill()
        self.Queue.release()
        self.ShotDown = True
        
    def _run(self, quiet):
        self.Status = "ok"
        if not quiet:
            self.log("started:", self.Title, timestamp=True)
        t0 = time.time()
        for step in self.Steps:
            self.Queue.append(StepTask(step, quiet))
        self.Queue.join()
        t1 = time.time()
        if not quiet:
            self.log("%s group:" % ("done" if self.Status=="ok" else "failed",), self.Title, timestamp=True)
            self.log("status:", self.Status, "exit code:", self.ExitCode)
            self.log("elapsed time:", self.pretty_time(t1 - t0))
            self.log("")
        if self.Status == "killed":
            self.StatusCoce = None
        return self.Status

class SequentialGroup(Step):
    
    def __init__(self, config, external_env, level, steps = []):
        Step.__init__(self, config, external_env, level)
        self.Title = self.Title or "sequential group #%04x" % (id(self) % 256,)
        self.Steps = steps
        self.RunningStep = None
    
    @synchronized
    def dump_state(self):
        steps = []
        for step in self.Steps:
            step_dump = step.dump_state()
            if step is self.RunningStep:
                step_dump["status"] = "running"
            elif step.Status is None:
                step_dump["status"] = "pending"
            steps.append(step_dump)
        return {"type":"sequential", "status":self.Status, "title":self.Title, "steps":steps}

    def update_run_env(self, outer):
        self.RunEnv = self.combine_env(outer)
        for step in self.Steps:
            step.update_run_env(self.RunEnv)

    def _run(self, quiet):
        if not quiet:
            self.log("started:", self.Title, timestamp=True)
        t0 = time.time()
        for step in self.Steps:
            with self:
                if self.Status is None:
                    self.RunningStep = step
                    with self.unlock:
                        status = step.run()
                        if step.ExitCode is not None:
                            self.ExitCode = step.ExitCode
                    if status != "ok":
                        self.Status = "killed"
        t1 = time.time()
        if self.Status is None:
            self.Status = "ok"
        if not quiet:
            self.log("%s group:" % ("done" if self.Status=="ok" else "failed",), self.Title, timestamp=True)
            self.log("status:", self.Status, "exit code:", self.ExitCode)
            self.log("elapsed time:", self.pretty_time(t1 - t0))
            self.log("")
        return self.Status

    @synchronized
    def kill(self):
        if self.RunningStep is not None:
            self.RunningStep.kill()
            self.Status = "killed"
            self.RunningStep = None
