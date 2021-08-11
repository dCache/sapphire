package org.dcache;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.InetAccessHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.MultipartConfigElement;
import java.nio.file.Paths;

public class FileServer {
    Server server;
    private static final Logger _log = LoggerFactory.getLogger(SapphireDriver.class);

    public FileServer (int port, String[] whitelist) {
        server = new Server(port);
        ServletContextHandler handler = new ServletContextHandler(server, "/sapphire");
        handler.addServlet(FileServlet.class, "/v1");

        ServletHolder stageServletHolder = new ServletHolder(new StageServlet());
        String location = Paths.get("").toString();

        long maxFilesize = -1L;
        long maxRequestsize = -1L;
        int filesizeThreshold = 64*1024;

        MultipartConfigElement multipartConfigElement = new MultipartConfigElement(location, maxFilesize, maxRequestsize, filesizeThreshold);
        stageServletHolder.getRegistration().setMultipartConfig(multipartConfigElement);
        handler.addServlet(stageServletHolder, "/stage");

        InetAccessHandler accessHandler = new InetAccessHandler();
        for(String ip : whitelist) {
            accessHandler.include(ip);
        }
        accessHandler.setHandler(server.getHandler());
        server.setHandler(accessHandler);
    }

    public void startServer() throws Exception {
        server.start();
        _log.info("Sapphire-server started");
    }

    public void stopServer() throws Exception {
        server.stop();
        _log.info("Sapphire-server stopped");
    }
}
