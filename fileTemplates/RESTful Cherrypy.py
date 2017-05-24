import cherrypy


class MyClass(object):
    exposed = True

    def GET(self, * uri, ** params):
        pass

    def POST(self, * uri, ** params):
        pass

    def PUT(self, * uri, ** params):
        pass

    def DELETE(self, * uri, ** params):
        pass
    
    
if __name__ == '__main__':
    conf = {
        '/': {
            'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
            'tools.sessions.on': True
        }
    }

    cherrypy.tree.mount(MyClass(), '/', conf)
    cherrypy.engine.start()
    cherrypy.engine.block()
