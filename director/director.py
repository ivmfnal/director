import sys, traceback, os, signal, time, textwrap, json
from pythreader import Task, Primitive, synchronized, TaskQueue
from .parser import Parser, convert

#
# Dependencies
#

if sys.version_info[:2] < (3,11):
    print("Pytbon version 3.11 or later is required", file=sys.stderr)
    sys.exit(1)

try:
    import lark
except ModuleNotFoundError:
    import sys
    print("lark library needs to be installed.\nPlease use 'pip install lark'.", file=sys.stderr)
    sys.exit(1)

try:
    import webpie
except ModuleNotFoundError:
    import sys
    print("webpie library needs to be installed.\nPlease use 'pip install webpie'.", file=sys.stderr)
    sys.exit(1)

from webpie import HTTPServer, WPApp, WPHandler

Usage = """
director [-q] <script>
"""

class Script(WPApp):

    def __init__(self, text, port=8888):
        parsed = Parser().parse(text)
        #print("parsed:", parsed.pretty())
        self.Tree = convert(parsed)
        
        try:
            from webpie import HTTPServer, WPApp, WPHandler
            self.HTTPServer = HTTPServer(port, self, daemon=True)
            WPApp.__init__(self, self.status_request)            
        except ModuleNotFoundError:
            print("Can not import webpie module. HTTP status server will not be running. Use 'pip install webpie' to enable the HTTP server.", file=sys.stderr)
            self.HTTPServer = None

    def run(self, quiet):
        self.Tree.update_run_env(os.environ)
        if self.HTTPServer is not None:
            self.HTTPServer.start()
        result = self.Tree.run(quiet)
        if self.HTTPServer is not None:
            self.HTTPServer.close()
        return result

    def status_request(self, request, relpath, **args):
        info = self.Tree.dump_state()
        return json.dumps(info), "text/json"


def main():
    import getopt

    opts, args = getopt.getopt(sys.argv[1:], "h?qp:", ["--help"])
    opts = dict(opts)
    if len(args) != 1 or "-?" in opts or "-h" in opts or "--help" in opts:
        print(Usage)
        sys.exit(2)

    quiet = "-q" in opts
    port = int(opts.get("-p", 8888))
    script = Script(open(args[0], "r").read(), port)
    status = script.run(quiet)
    if status != "ok":
        sys.exit(1)

if __name__ == "__main__":
    main()
