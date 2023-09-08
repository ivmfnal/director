import pprint
from lark import Tree, Lark, Transformer
import textwrap

grammar = """
?script: step

?step:    command
    | parallel
    | sequential
    | "(" step ")"
    
parallel: "{" options? steps "}"

sequential: "[" options?  steps "]"

command:  options? CMD

?steps : step+

options : option+

?option: env 
    | opt

opt: "-" CNAME REST_OF_LINE NEWLINE

env: "-" "env" CNAME "=" REST_OF_LINE NEWLINE
    
CMD: /[a-zA-Z0-9.\/][^\r\n].*/x
REST_OF_LINE: /[^\r\n].*/x
COMMENT: ("#"|"//") REST_OF_LINE NEWLINE

%import common.CNAME
%import common.INT
%import common.NEWLINE
%import common.WS_INLINE
%import common.WS
%ignore WS_INLINE
%ignore NEWLINE
%ignore COMMENT

STRING : /("(?!"").*?(?<!\\\\)(\\\\\\\\)*?"|'(?!'').*?(?<!\\\\)(\\\\\\\\)*?')/i
UNQUOTED_STRING : /[a-z0-9:%$@_^.%*?-]+/i


"""

parser = Lark(grammar, start="script")


class Node(object):
    
    def __init__(self, type, children=[], **data):
        self.Type = type
        self.Children = children
        self.Data = data
        
    def __getitem__(self, name):
        return self.Data[name]
        
    def __str__(self):
        return f"Node(type={self.Type}, data: {self.Data})"
    
    __repr__ = __str__
        
    def format(self, indent=""):
        lines = [
            indent + self.Type,
        ] + \
        [ indent + f": {k} = {v}" for k, v in self.Data.items() ]
        for c in self.Children:
            lines += c.format(indent + "  ")
        return lines
    
    def pretty(self):
        return "\n".join(self.format())

class Parser(Transformer):
    
    def parse(self, text):
        parsed = Lark(grammar, start="script").parse(text)
        return self.transform(parsed)

    def sequential(self, args):
        opts = None
        steps = []
        env = None
        for arg in args:
            if arg.Type == "options":
                opts = arg["opts"]
                env = arg["env"]
            elif arg.Type == "steps":
                steps = arg.Children
        return Node("sequential", steps, env=env, opts=opts)
    
    def parallel(self, args):
        opts = None
        steps = []
        env = None
        for arg in args:
            if arg.Type == "options":
                opts = arg["opts"]
                env = arg["env"]
            elif arg.Type == "steps":
                steps = arg.Children
        return Node("parallel", steps, env=env, opts=opts)
    
    def command(self, args):
        opts = None
        env = None
        if isinstance(args[0], Node) and args[0].Type == "options":
            opts = args[0]["opts"]
            env = args[0]["env"]
            cmd = args[1].value.strip()
        else:
            cmd = args[0].value.strip()
        return Node("command", command=cmd, env=env, opts=opts)
    
    def env(self, args):
        name, value = args[0].value.strip(), args[1].value.strip()
        return Node("env", env={name:value})
    
    def opt(self, args):
        name, value = args[0].value.strip(), args[1].value.strip()
        return Node("opt", opt={name:value})
    
    def concurrency(self, args):
        n = int(args[0].strip())
        return Node("opt", data={"concurrency": int(args[0].value)})
    
    def options(self, nodes):
        env = {}
        opts = {}
        for node in nodes:
            if node.Type == "opt":
                opts.update(node["opt"])
            elif node.Type == "env":
                env.update(node["env"])
        return Node("options", opts=opts, env=env)
    
    def __default__(self, type, args, meta):
        return Node(type.value, args)

def convert(node, level=0):
    #
    # Recursively converts the Node tree into Director tasks tree
    #
    
    from script import Command, ParallelGroup, SequentialGroup

    if node.Type == "command":
        return Command(node["opts"] or {}, node["env"] or {}, level, node["command"])
    elif node.Type == "parallel":
        tasks = [convert(t, level+1) for t in node.Children]
        return ParallelGroup(node["opts"] or {}, node["env"] or {}, level, tasks)
    elif node.Type == "sequential":
        tasks = [convert(t, level+1) for t in node.Children]
        return SequentialGroup(node["opts"] or {}, node["env"] or {}, level, tasks)

        
