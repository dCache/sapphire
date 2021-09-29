package org.dcache;

import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;

/*
 * Servlet to accept GET-Requests and return an archive to the client without
 * producing the overhead that any dCache-door does
 */

public class FileServlet extends HttpServlet {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileServlet.class);

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String filepath = "";
        AsyncContext asyncContext = request.startAsync();

        filepath = request.getHeader("file");

        if(filepath == null || filepath.equals("")) {
            response.setStatus(HttpStatus.BAD_REQUEST_400);
            asyncContext.complete();
            return;
        }
        File file = new File(filepath);

        if (!file.exists() || file.isDirectory()) {
            response.setStatus(HttpStatus.NOT_FOUND_404);
            asyncContext.complete();
            return;
        }

        try(FileInputStream fileIn = new FileInputStream(file); OutputStream outputStream = response.getOutputStream()){
            response.setContentType("application/octet-stream");
            fileIn.transferTo(outputStream);
            response.setStatus(HttpStatus.OK_200);
        } catch (IOException e) {
            LOGGER.error("Error while transferring file to client: ", e);
            response.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
        } finally {
            asyncContext.complete();
        }
    }
}
