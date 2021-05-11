package org.dcache;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;

public class FileServer {
    Server server;
    public FileServer (int port) {
        server = new Server(port);
        ServletContextHandler handler = new ServletContextHandler(server, "/sapphire");
        handler.addServlet(FileServlet.class, "/v1");
    }

    public void startServer() throws Exception {
        server.start();
    }
}
