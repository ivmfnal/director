import pprint
from lark import Tree, Lark, Transformer
import textwrap
from .groups import Command, ParallelGroup, SequentialGroup

grammar = """
?script: step

?step:    command
    | parallel
    | sequential
    | "(" options? step ")"
    
parallel: "{" options? steps "}"

sequential: "[" options?  steps "]"

command:  CMD

?steps : step+

options : option+

?option: env 
    | opt

opt: "-" CNAME "=" value 

env: "env" CNAME "=" value 

?value : (WORD|STRING)

CMD: /[a-zA-Z0-9.\/][^\r\n\#]*/x

%import common.CNAME
%import common.INT
%import common.NEWLINE
%import common.WS_INLINE
%import common.WS
%ignore WS_INLINE
%ignore NEWLINE
%ignore WS

COMMENT.4: "#" /[^\r\n]+/x? NEWLINE
%ignore COMMENT

WORD.1 : /[^ ]+/
STRING.2 : /("(?!"").*?(?<!\\\\)(\\\\\\\\)*?"|'(?!'').*?(?<!\\\\)(\\\\\\\\)*?')/i
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
        
    def __setitem__(self, name, value):
        self.Data[name] = value
        
    def get(self, name, default=None):
        return self.Data.get(name, default)
        
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
        #print(parsed.pretty())
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
    
    def step(self, args):
        #print("step: args:", args)
        assert len(args) == 2 and args[0].Type == "options"
        opts = args[0]["opts"].copy()
        env = args[0]["env"].copy()
        
        opts.update(args[1].get("opts") or {})
        env.update(args[1].get("env") or {})
        args[1]["opts"] = opts
        args[1]["env"] = env
        return args[1]
    
    def env(self, args):
        #print("env: args:", args)
        name = args[0].value.strip()
        if args[1].type == "STRING":
            value = args[1].value[1:-1]         # remove quotes
        else:
            value = args[1].value.strip()
        return Node("env", env={name:value})
    
    def opt(self, args):
        name = args[0].value.strip()
        if args[1].type == "STRING":
            value = args[1].value[1:-1]         # remove quotes
        else:
            value = args[1].value.strip()
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
    

    if node.Type == "command":
        return Command(node["opts"] or {}, node["env"] or {}, level, node["command"])
    elif node.Type == "parallel":
        tasks = [convert(t, level+1) for t in node.Children]
        return ParallelGroup(node["opts"] or {}, node["env"] or {}, level, tasks)
    elif node.Type == "sequential":
        tasks = [convert(t, level+1) for t in node.Children]
        return SequentialGroup(node["opts"] or {}, node["env"] or {}, level, tasks)
    else:
        raise ValueError("convert: unknown node type: " + node.Type)

        
