import json
import cherrypy

host = '127.0.0.1'


class Catalog(object):
    exposed = True

    def __init__(self):
        with open('conf.json') as file:
            self.catalog = json.load(file)

    def GET(self, *uri):
        try:
            return json.dumps(self.catalog[uri[0]])
        except Exception as e:
            return "0"


if __name__ == "__main__":
    conf = {
        "/": {
            "request.dispatch": cherrypy.dispatch.MethodDispatcher(),
            "tools.sessions.on": True,
        }
    }
    cherrypy.tree.mount(Catalog(), "/", conf)
    cherrypy.config.update({
        "server.socket_host": host,
        "server.socket_port": 8089})
    cherrypy.engine.start()
    cherrypy.engine.block()